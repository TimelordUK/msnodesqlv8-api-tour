# msnodesqlv8 API tour

A single file, `api-tour.js`, that walks through the main features of
[msnodesqlv8](https://github.com/TimelordUK/node-sqlserver-v8). Each section is a
self-contained async function you can copy into your own code.

| section | shows |
| --- | --- |
| connect | open, `?` parameters, typed parameters, raw rows, BigInt, callbacks vs promises |
| events | streaming rows with events, pause / resume, a compound query handing over from one select to the next |
| multiple | several statements in one batch: `results[]`, `counts[]` |
| prepared | prepare once, execute many |
| procedures | object / array params, defaults, output params, return code, prints |
| transactions | begin / commit / rollback with a reusable helper |
| table | table manager bulk insert / select / update / delete |
| bcp | the same insert through BCP, with timings |
| tvp | table valued parameters into a proc and a plain query |
| geography | spatial values as WKT, distances computed server side |
| errors | print / info messages, error details, a batch carrying on past an error |
| cancel | cancel a running or streaming query, query timeout |
| pool | concurrency, pooled transactions |

## run

```
npm install
set CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=tempdb;UID=sa;PWD=...;TrustServerCertificate=yes;
node api-tour.js                  # everything
node api-tour.js tvp bcp          # chosen sections
node api-tour.js --keep table     # leave the tour_ objects in place to inspect
```

(`export CONNECTION_STRING=...` on Linux / macOS.)

The tour needs msnodesqlv8 5.6.0 or later, which fixed several things it shows:
nvarchar output params, cancelling an ordinary (non polling) query, fast bulk
inserts, and batches carrying on past an error.

## re-running

Every object the tour creates is called `tour_*`. They are all dropped at the
start and at the end of each run, so a run stopped with ctrl-c or a crash is
tidied up next time. To use the same pattern in your own scripts, give your
objects a common prefix and sweep them at start-up.

## not covered

These need extra setup. The driver repository has tests for each:

- Always Encrypted: `test/encrypt.test.js`, `setup-always-encrypted.ps1`
- Sequelize: `app/msnodesqlv8-sequelize`
- Logging: `sql.logger.setLogLevel(sql.LogLevel.DEBUG)`, see `examples/logging-example.js`
