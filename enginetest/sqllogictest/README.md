# SQLLogicTest Converter and Checker

This package contains a tool to convert SQLLogicTest files from CockroachDB's format to the standard 
SQLLogicTest format, which is readable and runnable by our SQLLogicTest suite.

## Converter Usage
From within the `gms/enginetest/sqllogictest` directory, run the following command:
```shell
go run ./convert/convert.go <infile> <outfile>
```

CochroachDB's tests use PostgreSQL syntax, while we expect MySQL syntax; the converter does not
take this into account. This is where the Checker is useful.

## Checker Usage 
```shell
go run ./check/check.go <infile>
```

This will run the SQLLogicTests against a running MySQL/Dolt server.
You can use the error output to sanity check some of the results, and fix any queries.
Common differences are the use of `SELECT ... from ... AS a(x)` instead of `SELECT ... AS x ... from ... AS a`,
and missing `ORDER BY`.

It is likely you will have to modify the connection string.
Additionally, it is possible to use this on a running dolt sql server.

