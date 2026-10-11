'use strict'

/*
  msnodesqlv8 API tour
  ====================

  One file, top to bottom, showing the main features of the driver. Each section
  is a self-contained async function, so copy whichever one you need.

    connect      open a connection, parameters, typed params, callbacks vs promises
    events       stream rows with events, pause / resume, two selects on one stream
    multiple     several statements in one batch: result sets, row counts
    prepared     prepare once, execute many
    procedures   stored procs: object params, defaults, output params, return code
    transactions begin / commit / rollback on a connection
    table        table manager: build a table, bulk insert / select / update / delete
    bcp          the same bulk insert switched to BCP for speed
    tvp          table valued parameters into a stored procedure
    geography    spatial types: send WKT, compute server side, read back
    errors       print / info messages, errors, multiple errors
    cancel       cancel a running query, query timeouts
    pool         connection pool: concurrency, events, pooled transactions

  Run
    npm install
    set CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=tempdb;UID=sa;PWD=...;TrustServerCertificate=yes;
    node api-tour.js                 all sections
    node api-tour.js tvp bcp         just these sections
    node api-tour.js --keep table    leave the tour_ objects behind to look at

  Re-running
    Every object the tour creates is named tour_*. cleanup() drops all of them,
    both at start and at exit, so a run interrupted with ctrl-c (or a crash) is
    tidied up the next time it starts. You should never see "already exists".

  Not covered here (they need extra setup). The driver repo has full tests for each:
    Always Encrypted  test/encrypt.test.js, setup-always-encrypted.ps1
    Sequelize         app/msnodesqlv8-sequelize
    Logging           sql.logger.setLogLevel(sql.LogLevel.DEBUG), see examples/logging-example.js
*/

const sql = require('msnodesqlv8')

const connectionString = process.env.CONNECTION_STRING ||
  'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=tempdb;Trusted_Connection=yes;TrustServerCertificate=yes;'

const PREFIX = 'tour_'

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

function heading (title) {
  console.log(`\n=== ${title} ${'='.repeat(Math.max(0, 70 - title.length))}`)
}

function show (label, value) {
  console.log(`-- ${label}`)
  if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
    console.table(value)
  } else {
    console.log(value)
  }
}

// drop every table, procedure and type whose name starts with PREFIX.
// order matters: procedures can reference types, so they go first.
const cleanupSql = `
declare @prefix nvarchar(128) = ? + '%'
declare @sql nvarchar(max) = N''
select @sql += N'drop procedure ' + quotename(schema_name(schema_id)) + N'.' + quotename(name) + N';'
  from sys.procedures where name like @prefix
select @sql += N'drop table ' + quotename(schema_name(schema_id)) + N'.' + quotename(name) + N';'
  from sys.tables where name like @prefix
select @sql += N'drop type ' + quotename(schema_name(schema_id)) + N'.' + quotename(name) + N';'
  from sys.types where is_user_defined = 1 and name like @prefix
exec sp_executesql @sql`

async function cleanup (conn) {
  // escape '_' which is a LIKE wildcard
  await conn.promises.query(cleanupSql, [PREFIX.replace(/_/g, '[_]')])
}

// run a query as an event stream, collecting everything the server sends.
// handy when you want print messages and errors as well as rows.
function stream (conn, sqlText, params = []) {
  return new Promise(resolve => {
    const out = { meta: [], rows: [], info: [], errors: [] }
    let row = null
    const q = conn.query(sqlText, params)
    q.on('meta', meta => { out.meta.push(meta) })
    q.on('row', () => { row = {}; out.rows.push(row) })
    q.on('column', (index, value) => { row[out.meta.at(-1)[index].name] = value })
    q.on('info', m => out.info.push(m.message))
    q.on('error', (err, more) => out.errors.push({ message: err.message, code: err.code, more }))
    q.on('free', () => resolve(out)) // 'free' is the very last event for a query
  })
}

// the try / commit / catch / rollback pattern, written once.
async function withTransaction (conn, work) {
  await conn.promises.beginTransaction()
  try {
    const result = await work()
    await conn.promises.commit()
    return result
  } catch (err) {
    await conn.promises.rollback()
    throw err
  }
}

// ---------------------------------------------------------------------------
// connect: open, query with parameters, typed parameters, callbacks
// ---------------------------------------------------------------------------

async function connect (conn) {
  heading('connect')

  // ad hoc: open, query, close in one call. fine for one-offs, use a
  // connection or a pool for anything repeated.
  const version = await sql.promises.query(connectionString,
    'select @@version as version, db_name() as db, @@spid as spid')
  show('server', version.first[0].version.split('\n')[0])

  // parameters are '?' placeholders bound by the native driver: never build
  // SQL by concatenating user input.
  const res = await conn.promises.query(
    'select ? as num, ? as str, ? as dt, ? as flag, ? as nothing',
    [42, 'hello', new Date('2026-01-02T03:04:05Z'), true, null])
  show('promises.query -> res.first', res.first)

  // the driver infers types from JS values. use the type helpers when you
  // need to control precisely how a value is sent.
  const typed = await conn.promises.query(
    'select ? as small, ? as dec, ? as ascii, ? as uid',
    [sql.SmallInt(7), sql.Decimal(123.45), sql.VarChar('not unicode'),
      sql.UniqueIdentifier('6F9619FF-8B86-D011-B42D-00C04FC964FF')])
  show('typed parameters', typed.first)

  // { raw: true } gives arrays rather than objects; column names are in meta
  const raw = await conn.promises.query('select 1 as a, 2 as b', [], { raw: true })
  show('raw rows', raw.first)
  show('meta', raw.firstMeta.map(m => `${m.name}: ${m.sqlType}`))

  // BIGINT beyond 2^53 loses precision as a Number, so ask for a BigInt.
  // the same option is on the connection (setUseBigIntAsNative) and the pool.
  const big = await conn.promises.query({
    query_str: 'select cast(9007199254740993 as bigint) as big',
    bigint_as_native: true
  })
  show('bigint_as_native', big.first[0].big)

  // every promise API has a callback twin, here conn.query(sql, params, cb)
  const rows = await new Promise((resolve, reject) => {
    conn.query('select ? as viaCallback', ['cb'], (err, rows) => err ? reject(err) : resolve(rows))
  })
  show('callback style', rows)
}

// ---------------------------------------------------------------------------
// events: stream rows, pause / resume for flow control, compound query handover
// ---------------------------------------------------------------------------

async function events (conn) {
  heading('events')

  // without a callback, query() returns an event emitter. rows are not held
  // in memory by the driver, which suits large result sets.
  await new Promise((resolve, reject) => {
    const q = conn.query('select top 1000 name, object_id from sys.all_objects order by object_id')
    let meta = null
    let rows = 0
    q.on('submitted', d => console.log(`submitted: ${d.query_str}`))
    q.on('meta', m => { meta = m; console.log(`meta: ${m.map(c => c.name).join(', ')}`) })
    q.on('column', (index, value) => {
      if (rows <= 2) console.log(`  row ${rows - 1} ${meta[index].name} = ${value}`)
    })
    q.on('row', () => {
      // pause the flow every 300 rows, e.g. while writing a batch somewhere slow.
      // no rows are emitted while paused; resume carries on where it stopped.
      if (++rows % 300 === 0) {
        q.pauseQuery()
        console.log(`  ... paused at ${rows} rows`)
        setTimeout(() => { console.log('  ... resumed'); q.resumeQuery() }, 100)
      }
    })
    q.on('error', reject)
    q.on('done', () => console.log(`done: ${rows} rows read`))
    q.on('free', () => { console.log('free: native statement released'); resolve() })
  })

  // a compound query streams each result set in turn on the same emitter.
  // the handover is a new 'meta' event: everything before it belongs to the
  // previous select, everything after to the next. 'done' and 'free' fire
  // once, after the last result set.
  console.log('\ntwo selects, one stream:')
  const sets = await new Promise((resolve, reject) => {
    const q = conn.query(`
      select top 3 object_id as id, name from sys.all_objects order by object_id
      select top 2 name as db, database_id from sys.databases order by database_id`)
    const sets = [] // one entry per result set: { meta, rows: [] }
    let row = null
    q.on('meta', meta => {
      const prev = sets.at(-1)
      if (prev) console.log(`  -- end of result set ${sets.length - 1}: ${prev.rows.length} rows`)
      sets.push({ meta, rows: [] })
      console.log(`  -- result set ${sets.length - 1} starts: ${meta.map(c => c.name).join(', ')}`)
    })
    // the index counts rows across the whole query; it does not restart at 0 per result set
    q.on('row', index => {
      row = {}
      sets.at(-1).rows.push(row)
      console.log(`  row ${index}`)
    })
    q.on('column', (index, value) => {
      const name = sets.at(-1).meta[index].name
      row[name] = value
      console.log(`    ${name} = ${value}`)
    })
    q.on('error', reject)
    q.on('done', () => console.log(`  -- end of result set ${sets.length - 1}: ${sets.at(-1).rows.length} rows\ndone`))
    q.on('free', () => resolve(sets))
  })
  show('rows per result set', sets.map(s => s.rows.length))
  show('result set 1', sets[1].rows)
}

// ---------------------------------------------------------------------------
// multiple: several statements in one round trip
// ---------------------------------------------------------------------------

async function multiple (conn) {
  heading('multiple')

  const res = await conn.promises.query(`
    declare @t table (id int, colour varchar(10))
    insert into @t values (1, 'red'), (2, 'green'), (3, 'blue')
    update @t set colour = upper(colour) where id > 1
    select * from @t where id < 3
    select count(*) as total from @t`)

  // each select becomes an entry in results; insert / update / delete counts in counts
  show('results.length', res.results.length)
  show('results[0]', res.results[0])
  show('results[1]', res.results[1])
  show('row counts', res.counts)
  show('elapsed ms', res.elapsed)
}

// ---------------------------------------------------------------------------
// prepared: prepare once, execute many with different params
// ---------------------------------------------------------------------------

async function prepared (conn) {
  heading('prepared')

  await conn.promises.query(`create table ${PREFIX}prepared (id int primary key, name nvarchar(50))`)

  const insert = await conn.promises.prepare(`insert into ${PREFIX}prepared (id, name) values (?, ?)`)
  const select = await conn.promises.prepare(`select id, name from ${PREFIX}prepared where id = ?`)
  try {
    for (let i = 1; i <= 5; ++i) {
      await insert.promises.query([i, `name ${i}`])
    }
    const res = await select.promises.query([3])
    show(`prepared select: ${select.getSignature()}`, res.first)
  } finally {
    // prepared statements hold server resources until freed
    await insert.promises.free()
    await select.promises.free()
  }
}

// ---------------------------------------------------------------------------
// procedures: params by name, defaults, outputs, return code, prints
// ---------------------------------------------------------------------------

async function procedures (conn) {
  heading('procedures')

  await conn.promises.query(`
    create procedure ${PREFIX}add
      @a int,
      @b int = 10,
      @sum int output,
      @message nvarchar(100) output
    as
    begin
      set nocount on
      print 'adding numbers'
      set @sum = @a + @b
      set @message = concat(@a, ' + ', @b, ' = ', @sum)
      select @a as a, @b as b
      return 99
    end`)

  // the driver reads the proc definition so every param is bound with its
  // declared type. pass an object to name params, leaving out ones with defaults.
  const res = await conn.promises.callProc(`${PREFIX}add`, { a: 5 })
  show('rows', res.first)
  show('output [return, @sum, @message]', res.output)
  show('return code', res.returns)
  show('print messages', res.info)

  // or as an ordered array
  const res2 = await conn.promises.callProc(`${PREFIX}add`, [1, 2])
  show('array params output', res2.output)

  // fetch a definition once and call it repeatedly (also on pool.promises)
  const proc = await conn.promises.getProc(`${PREFIX}add`)
  show('signature', proc.getMeta().signature)
  const res3 = await proc.promises.call({ a: 100, b: 200 })
  show('proc.promises.call output', res3.output)
}

// ---------------------------------------------------------------------------
// transactions
// ---------------------------------------------------------------------------

async function transactions (conn) {
  heading('transactions')

  const table = `${PREFIX}account`
  await conn.promises.query(`create table ${table} (id int primary key, balance money)`)
  await conn.promises.query(`insert into ${table} values (1, 100), (2, 100)`)

  // committed: move 30 from 1 to 2
  await withTransaction(conn, async () => {
    await conn.promises.query(`update ${table} set balance -= ? where id = 1`, [30])
    await conn.promises.query(`update ${table} set balance += ? where id = 2`, [30])
  })

  // rolled back: the second statement fails, so the first is undone too
  try {
    await withTransaction(conn, async () => {
      await conn.promises.query(`update ${table} set balance -= ? where id = 1`, [1000])
      await conn.promises.query(`update ${table} set balance += ? where id = 'oops'`, [1000])
    })
  } catch (err) {
    show('rolled back because', err.message)
  }

  const res = await conn.promises.query(`select * from ${table}`)
  show('balances (70 / 130)', res.first)
}

// ---------------------------------------------------------------------------
// table: the table manager for bulk insert / select / update / delete
// ---------------------------------------------------------------------------

function makeEmployees (count) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `employee ${i + 1}`,
    salary: 30000 + i * 10,
    hired: new Date(Date.UTC(2020, 0, 1 + (i % 365))),
    active: i % 2 === 0
  }))
}

// in real life the table usually exists already; getTable only needs a
// primary key on it to do select / update / delete.
// (when metadata can't be read from the server, e.g. Sybase, describe the
// table in JS instead: conn.tableMgr().makeBuilder(name), addColumn(...),
// then builder.toTable() gives the same bulk manager.)
async function createEmployeeTable (conn, name) {
  await conn.promises.query(`
    create table ${name} (
      id int primary key,
      name nvarchar(50) not null,
      salary decimal(18, 2),
      hired datetime2,
      active bit)`)
}

async function table (conn) {
  heading('table')

  const name = `${PREFIX}employee`
  await createEmployeeTable(conn, name)

  // getTable reads the table's metadata from the server and binds each
  // column precisely (which is also what makes Always Encrypted work).
  const t = await conn.promises.getTable(name)
  show('insert signature', t.getSummary().insertSignature)

  const employees = makeEmployees(10)
  await t.promises.insert(employees)

  // select by primary key: pass objects holding just the key columns
  const some = await t.promises.select([{ id: 2 }, { id: 3 }])
  show('select by key', some)

  // update just the columns you nominate, matched on the primary key
  t.setUpdateCols([{ name: 'salary' }])
  await t.promises.update([{ id: 2, salary: 99999 }, { id: 3, salary: 88888 }])
  show('after update', await t.promises.select([{ id: 2 }, { id: 3 }]))

  await t.promises.delete(t.keys(employees.slice(5)))
  const left = await conn.promises.query(`select count(*) as remaining from ${name}`)
  show('rows left after delete', left.first[0].remaining)
}

// ---------------------------------------------------------------------------
// bcp: the same bulk insert, much faster
// ---------------------------------------------------------------------------

async function bcp (conn) {
  heading('bcp')

  // the normal path binds parameter arrays, one transaction per batch;
  // bcp streams rows like the bcp tool and is faster again.
  const count = 50000
  const rows = makeEmployees(count)
  for (const useBcp of [false, true]) {
    const name = `${PREFIX}bcp_${useBcp ? 'on' : 'off'}`
    await createEmployeeTable(conn, name)
    const t = await conn.promises.getTable(name)
    // bcp needs "ODBC Driver 17/18 for SQL Server". The version is read from
    // the connection string; with a DSN call t.setBcpVersion(18) yourself.
    t.setUseBcp(useBcp)
    const start = Date.now()
    await t.promises.insert(rows)
    const ms = Date.now() - start
    const res = await conn.promises.query(`select count(*) as n from ${name}`)
    console.log(`bcp ${useBcp ? 'on ' : 'off'}: ${res.first[0].n} rows in ${ms} ms (${Math.round(count / ms * 1000)} rows/sec)`)
  }
}

// ---------------------------------------------------------------------------
// tvp: send a whole table as one parameter
// ---------------------------------------------------------------------------

async function tvp (conn) {
  heading('tvp')

  const type = `${PREFIX}person_type`
  const table = `${PREFIX}person`
  const proc = `${PREFIX}insert_people`
  await conn.promises.query(`create type ${type} as table (name nvarchar(50), age int, joined date)`)
  await conn.promises.query(`create table ${table} (id int identity primary key, name nvarchar(50), age int, joined date)`)
  await conn.promises.query(`
    create procedure ${proc} @people ${type} readonly
    as
    begin
      set nocount on
      insert into ${table} (name, age, joined) select name, age, joined from @people
      select count(*) as inserted from @people
    end`)

  // fetch the type's column definitions, add rows, and wrap it as a parameter
  const people = await conn.promises.getUserTypeTable(type)
  people.addRowsFromObjects([
    { name: 'ada', age: 36, joined: new Date('2020-01-01') },
    { name: 'grace', age: 85, joined: new Date('2021-06-15') },
    { name: 'linus', age: 56, joined: new Date('2022-11-30') }
  ])
  const param = sql.TvpFromTable(people)
  people.rows = [] // the parameter holds its own copy

  const res = await conn.promises.callProc(proc, [param])
  show('proc says', res.first)

  // a tvp works in a plain query too
  const again = await conn.promises.query('select name, age from ? where age > 50', [param])
  show('select from tvp', again.first)
}

// ---------------------------------------------------------------------------
// geography: spatial values travel as text (WKT) and are handled server side
// ---------------------------------------------------------------------------

async function geography (conn) {
  heading('geography')

  const table = `${PREFIX}place`
  await conn.promises.query(`create table ${table} (id int identity primary key, name nvarchar(50), location geography)`)

  const places = [
    ['london', 'POINT(-0.1278 51.5074)'],
    ['paris', 'POINT(2.3522 48.8566)'],
    ['thames', 'LINESTRING(-0.1956 51.4867, -0.1206 51.5033, -0.0754 51.5055)'],
    ['hyde park', 'POLYGON((-0.1877 51.5087, -0.1532 51.5087, -0.1532 51.5025, -0.1877 51.5025, -0.1877 51.5087))']
  ]
  for (const [name, wkt] of places) {
    await conn.promises.query(
      `insert into ${table} (name, location) values (?, geography::STGeomFromText(?, 4326))`, [name, wkt])
  }

  // selecting the raw column gives a Buffer in SQL Server's binary format,
  // so ask the server for text or for the values you need.
  const res = await conn.promises.query(`
    declare @london geography = (select location from ${table} where name = 'london')
    select name,
           location.STGeometryType() as kind,
           location.STAsText() as wkt,
           cast(round(location.STDistance(@london) / 1000, 1) as float) as km_from_london
    from ${table}`)
  show('places', res.first)

  // many rows at once: pass WKT through a tvp (see tvp above) and convert
  // with geography::STGeomFromText inside the proc.
}

// ---------------------------------------------------------------------------
// errors: print / info messages, failures, multiple errors
// ---------------------------------------------------------------------------

async function errors (conn) {
  heading('errors')

  // print and low severity raiserror (<= 10) are info messages, not errors
  const res = await conn.promises.query(`
    print 'starting'
    raiserror('just a warning', 10, 1)
    select 1 as ok`)
  show('info messages', res.info)
  show('rows', res.first)

  // severity 11+ rejects the promise. the error carries the server details.
  await conn.promises.query(`
    create procedure ${PREFIX}fail @id int
    as
    begin
      if @id < 0 throw 50001, 'id must not be negative', 1
      select @id as id
    end`)
  try {
    await conn.promises.callProc(`${PREFIX}fail`, { id: -1 })
  } catch (err) {
    const { message, code, severity, sqlstate, lineNumber, procName, serverName } = err
    show('caught', { message, code, severity, sqlstate, lineNumber, procName, serverName })
  }

  // the connection is fine after an error
  show('still usable', (await conn.promises.query('select 1 as alive')).first)

  // a statement failing mid batch does not stop the batch: SQL Server runs the
  // rest, and you get every print, error and result. 'more' says whether the
  // batch goes on after an error.
  // (on linux / macOS, if the very first thing a batch does is fail, unixODBC
  // ends the batch there.)
  const batch = `
    print 'step 1'
    raiserror('step 2 failed', 16, 1)
    print 'step 3 still runs'
    select 'done' as status`
  const streamed = await stream(conn, batch)
  show('stream: info', streamed.info)
  show('stream: errors', streamed.errors)
  show('stream: rows', streamed.rows)

  // the promise API rejects with the last error; everything else the batch
  // produced is on err._results
  try {
    await conn.promises.query(batch)
  } catch (err) {
    show('promise: rejected with', err.message)
    show('promise: err._results.first', err._results.first)
  }

  // compile errors stop the whole batch before it runs, but all are reported
  const several = await stream(conn, 'select no_such_column_a; select no_such_column_b')
  show('stream: several compile errors', several.errors)
}

// ---------------------------------------------------------------------------
// cancel: stop a running query, or put a time limit on it
// ---------------------------------------------------------------------------

async function cancel (conn) {
  heading('cancel')

  // conn.query returns a handle you can cancel at any point: queued, executing
  // or streaming rows. promises.cancel resolves with the error the query ended
  // with ('Operation canceled').
  const start = Date.now()
  const q = conn.query("waitfor delay '00:00:30'")
  await new Promise(resolve => setTimeout(resolve, 500))
  const err = await q.promises.cancel()
  console.log(`cancelled after ${Date.now() - start} ms: ${err.message}`)

  // callback style: cancelQuery(cb), and the query's own callback gets the error
  const streaming = conn.query('select * from sys.all_objects a cross join sys.all_objects b', e => {
    console.log(`streaming query ended: ${e.message}`)
  })
  let seen = 0
  streaming.on('row', () => {
    if (++seen === 1000) streaming.cancelQuery(e => { if (e) console.error(e.message) })
  })
  await new Promise(resolve => streaming.on('free', resolve))
  console.log(`cancelled after ${seen} rows`)

  // or give the query a server side timeout in seconds
  try {
    await conn.promises.query({ query_str: "waitfor delay '00:00:30'", query_timeout: 1 })
  } catch (err) {
    console.log(`timed out: ${err.message}`)
  }

  show('still usable', (await conn.promises.query('select 1 as alive')).first)
}

// ---------------------------------------------------------------------------
// pool: share a set of connections between concurrent work
// ---------------------------------------------------------------------------

async function pool () {
  heading('pool')

  const p = new sql.Pool({
    connectionString,
    floor: 0, // connections kept open when idle
    ceiling: 4, // never more than this, extra work waits in a queue
    heartbeatSecs: 20,
    inactivityTimeoutSecs: 60
  })
  // always listen for 'error' so a dropped connection is reported and replaced
  p.on('error', err => console.error(`pool error: ${err.message}`))
  await p.promises.open()

  try {
    // 8 one-second queries on 4 connections: about 2 seconds, not 8
    const start = Date.now()
    const jobs = Array.from({ length: 8 }, (_, i) =>
      p.promises.query("waitfor delay '00:00:01'; select ? as job, @@spid as spid", [i]))
    const results = await Promise.all(jobs)
    show(`8 queries in ${Date.now() - start} ms`, results.map(r => r.first[0]))

    // a pooled transaction runs on a single connection, handed to you as
    // description.connection. throw to roll back (the pool also reports the
    // error through its 'error' event).
    const table = `${PREFIX}pool_txn`
    await p.promises.query(`create table ${table} (id int)`)
    await p.promises.transaction(async description => {
      const cp = description.connection.promises
      await cp.query(`insert into ${table} values (1), (2)`)
    })
    try {
      await p.promises.transaction(async description => {
        await description.connection.promises.query(`insert into ${table} values (3)`)
        throw new Error('changed my mind')
      })
    } catch (err) {
      show('pooled transaction rolled back', err.message)
    }
    show('rows (1, 2)', (await p.promises.query(`select id from ${table}`)).first)

    // the pool has the same helpers as a connection
    // p.promises.callProc, getProc, getTable, getUserTypeTable
  } finally {
    await p.promises.close()
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

const sections = {
  connect, events, multiple, prepared, procedures, transactions, table, bcp, tvp, geography, errors, cancel, pool
}

async function main () {
  const args = process.argv.slice(2)
  const keep = args.includes('--keep')
  const chosen = args.filter(a => !a.startsWith('--'))
  const unknown = chosen.filter(name => !sections[name])
  if (unknown.length > 0) {
    console.log(`unknown section(s): ${unknown.join(', ')}\navailable: ${Object.keys(sections).join(' ')}`)
    process.exitCode = 1
    return
  }
  const toRun = chosen.length > 0 ? chosen : Object.keys(sections)

  let conn
  try {
    conn = await sql.promises.open(connectionString)
  } catch (err) {
    console.error(`could not connect: ${err.message}\nset CONNECTION_STRING, see the top of this file.`)
    process.exitCode = 1
    return
  }

  // ctrl-c: try to tidy up now. if that fails the next run's cleanup catches it.
  process.once('SIGINT', () => {
    console.log('\ninterrupted, cleaning up ...')
    // don't hang if the cleanup is blocked, e.g. by locks from an open transaction
    setTimeout(() => process.exit(130), 5000).unref()
    sql.promises.open(connectionString)
      .then(c => cleanup(c).finally(() => c.promises.close()))
      .catch(err => console.error(`cleanup failed (the next run will retry): ${err.message}`))
      .finally(() => process.exit(130))
  })

  try {
    await cleanup(conn) // remove anything left by an earlier, interrupted run
    for (const name of toRun) {
      await sections[name](conn)
    }
    console.log('\nall done.')
  } catch (err) {
    console.error(`\nfailed: ${err.message}`)
    process.exitCode = 1
  } finally {
    if (keep) {
      console.log(`--keep: left ${PREFIX}* objects in place`)
    } else {
      await cleanup(conn).catch(err => console.error(`cleanup failed: ${err.message}`))
    }
    await conn.promises.close()
  }
}

main()
