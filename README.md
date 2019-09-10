# go-mysql-server
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<a href="https://travis-ci.com/src-d/go-mysql-server"><img alt="Build Status" src="https://travis-ci.com/src-d/go-mysql-server.svg?branch=master" /></a>
<a href="https://codecov.io/gh/src-d/go-mysql-server"><img alt="codecov" src="https://codecov.io/gh/src-d/go-mysql-server/branch/master/graph/badge.svg" /></a>
<a href="https://godoc.org/github.com/src-d/go-mysql-server"><img alt="GoDoc" src="https://godoc.org/github.com/src-d/go-mysql-server?status.svg" /></a>
[![Issues](http://img.shields.io/github/issues/src-d/go-mysql-server.svg)](https://github.com/src-d/go-mysql-server/issues)

**go-mysql-server** is a SQL engine which parses standard SQL (based on MySQL syntax), resolves and optimizes queries.
It provides simple interfaces to allow custom tabular data source implementations.

**go-mysql-server** also provides a server implementation compatible with the MySQL wire protocol.
That means it is compatible with MySQL ODBC, JDBC, or the default MySQL client shell interface.

## Scope of this project

These are the goals of **go-mysql-server**:

- Be a generic extensible SQL engine that performs queries on your data sources.
- Provide interfaces so you can implement your own custom data sources without providing any (except for the `mem` data source that is used for testing purposes).
- Have a runnable server you can use on your specific implementation.
- Parse and optimize queries while still allow specific implementations to add their own analysis steps and optimizations.
- Provide some common index driver implementations so the user does not have to bring their own index implementation, and still be able to do so if they need to.

What are not the goals of **go-mysql-server**:

- Be a drop-in MySQL database replacement.
- Be an application/server you can use directly.
- Provide any kind of backend implementation (other than the `mem` one used for testing) such as json, csv, yaml, ... That's for clients to implement and use.

What's the use case of **go-mysql-server**?

Having data in another format that you want as tabular data to query using SQL, such as git. As an example of this, we have [gitbase](https://github.com/src-d/gitbase).

## Installation

The import path for the package is `github.com/src-d/go-mysql-server`.

To install it, run:

```
go get github.com/src-d/go-mysql-server
```

## Documentation

* [go-mysql-server godoc](https://godoc.org/github.com/src-d/go-mysql-server)


## SQL syntax

We are continuously adding more functionality to go-mysql-server. We support a subset of what is supported in MySQL, to see what is currently included check the [SUPPORTED](./SUPPORTED.md) file.

## Third-party clients

We support and actively test against certain third-party clients to ensure compatibility between them and go-mysql-server. You can check out the list of supported third party clients in the [SUPPORTED_CLIENTS](./SUPPORTED_CLIENTS.md) file along with some examples on how to connect to go-mysql-server using them.

## Available functions

<!-- BEGIN FUNCTIONS -->
|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|`ARRAY_LENGTH(json)`|if the json representation is an array, this function returns its size.|
|`AVG(expr)`| returns the average value of expr in all rows.|
|`CEIL(number)`| returns the smallest integer value that is greater than or equal to `number`.|
|`CEILING(number)`| returns the smallest integer value that is greater than or equal to `number`.|
|`CHAR_LENGTH(str)`| returns the length of the string in characters.|
|`COALESCE(...)`| returns the first non-null value in a list.|
|`CONCAT(...)`| concatenates any group of fields into a single string.|
|`CONCAT_WS(sep, ...)`| concatenates any group of fields into a single string. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.|
|`CONNECTION_ID()`| returns the current connection ID.|
|`COUNT(expr)`|  returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.|
|`DATE_ADD(date, interval)`| adds the interval to the given `date`.|
|`DATE_SUB(date, interval)`| subtracts the interval from the given `date`.|
|`DAY(date)`| is a synonym for DAYOFMONTH().|
|`DATE(date)`| returns the date part of the given `date`.|
|`DAYOFMONTH(date)`| returns the day of the month (0-31).|
|`DAYOFWEEK(date)`| returns the day of the week of the given `date`.|
|`DAYOFYEAR(date)`| returns the day of the year of the given `date`.|
|`FIRST(expr)`| returns the first value in a sequence of elements of an aggregation.|
|`FLOOR(number)`| returns the largest integer value that is less than or equal to `number`.|
|`FROM_BASE64(str)`| decodes the base64-encoded string `str`.|
|`GREATEST(...)`| returns the greatest numeric or string value.|
|`HOUR(date)`| returns the hours of the given `date`.|
|`IFNULL(expr1, expr2)`| if `expr1` is not NULL, it returns `expr1`; otherwise it returns `expr2`.|
|`IS_BINARY(blob)`| returns whether a `blob` is a binary file or not.|
|`JSON_EXTRACT(json_doc, path, ...)`| extracts data from a json document using json paths. Extracting a string will result in that string being quoted. To avoid this, use `JSON_UNQUOTE(JSON_EXTRACT(json_doc, path, ...))`.|
|`JSON_UNQUOTE(json)`| unquotes JSON value and returns the result as a utf8mb4 string.|
|`LAST(expr)`| returns the last value in a sequence of elements of an aggregation.|
|`LEAST(...)`| returns the smaller numeric or string value.|
|`LENGTH(str)`| returns the length of the string in bytes.|
|`LN(X)`| returns the natural logarithm of `X`.|
|`LOG(X), LOG(B, X)`| if called with one parameter, this function returns the natural logarithm of `X`. If called with two parameters, this function returns the logarithm of `X` to the base `B`. If `X` is less than or equal to 0, or if `B` is less than or equal to 1, then NULL is returned.|
|`LOG10(X)`| returns the base-10 logarithm of `X`.|
|`LOG2(X)`| returns the base-2 logarithm of `X`.|
|`LOWER(str)`| returns the string `str` with all characters in lower case.|
|`LPAD(str, len, padstr)`| returns the string `str`, left-padded with the string `padstr` to a length of `len` characters.|
|`LTRIM(str)`| returns the string `str` with leading space characters removed.|
|`MAX(expr)`| returns the maximum value of `expr` in all rows.|
|`MID(str, pos, [len])`| returns a substring from the provided string starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`MIN(expr)`| returns the minimum value of `expr` in all rows.|
|`MINUTE(date)`| returns the minutes of the given `date`.|
|`MONTH(date)`| returns the month of the given `date`.|
|`NOW()`| returns the current timestamp.|
|`NULLIF(expr1, expr2)`| returns NULL if `expr1 = expr2` is true, otherwise returns `expr1`.|
|`POW(X, Y)`| returns the value of `X` raised to the power of `Y`.|
|`REGEXP_MATCHES(text, pattern, [flags])`| returns an array with the matches of the `pattern` in the given `text`. Flags can be given to control certain behaviours of the regular expression. Currently, only the `i` flag is supported, to make the comparison case insensitive.|
|`REPEAT(str, count)`| returns a string consisting of the string `str` repeated `count` times.|
|`REPLACE(str,from_str,to_str)`| returns the string `str` with all occurrences of the string `from_str` replaced by the string `to_str`.|
|`REVERSE(str)`| returns the string `str` with the order of the characters reversed.|
|`ROUND(number, decimals)`| rounds the `number` to `decimals` decimal places.|
|`RPAD(str, len, padstr)`| returns the string `str`, right-padded with the string `padstr` to a length of `len` characters.|
|`RTRIM(str)`| returns the string `str` with trailing space characters removed.|
|`SECOND(date)`| returns the seconds of the given `date`.|
|`SLEEP(seconds)`| waits for the specified number of seconds (can be fractional).|
|`SOUNDEX(str)`| returns the soundex of a string.|
|`SPLIT(str,sep)`| returns the parts of the string `str` split by the separator `sep` as a JSON array of strings.|
|`SQRT(X)`| returns the square root of a nonnegative number `X`.|
|`SUBSTR(str, pos, [len])`| returns a substring from the string `str` starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUBSTRING(str, pos, [len])`| returns a substring from the string `str` starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUM(expr)`| returns the sum of `expr` in all rows.|
|`TO_BASE64(str)`| encodes the string `str` in base64 format.|
|`TRIM(str)`| returns the string `str` with all spaces removed.|
|`UPPER(str)`| returns the string `str` with all characters in upper case.|
|`WEEKDAY(date)`| returns the weekday of the given `date`.|
|`YEAR(date)`| returns the year of the given `date`.|
|`YEARWEEK(date, mode)`| returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.|
<!-- END FUNCTIONS -->

## Configuration

The behaviour of certain parts of go-mysql-server can be configured using either environment variables or session variables.

Session variables are set using the following SQL queries:

```sql
SET <variable name> = <value>
```

<!-- BEGIN CONFIG -->
| Name | Type | Description |
|:-----|:-----|:------------|
|`INMEMORY_JOINS`|environment|If set it will perform all joins in memory. Default is off.|
|`inmemory_joins`|session|If set it will perform all joins in memory. Default is off. This has precedence over `INMEMORY_JOINS`.|
|`MAX_MEMORY`|environment|The maximum number of memory, in megabytes, that can be consumed by go-mysql-server. Any in-memory caches or computations will no longer try to use memory when the limit is reached. Note that this may cause certain queries to fail if there is not enough memory available, such as queries using DISTINCT, ORDER BY or GROUP BY with groupings.|
|`DEBUG_ANALYZER`|environment|If set, the analyzer will print debug messages. Default is off.|
|`PILOSA_INDEX_THREADS`|environment|Number of threads used in index creation. Default is the number of cores available in the machine.|
|`pilosa_index_threads`|environment|Number of threads used in index creation. Default is the number of cores available in the machine. This has precedence over `PILOSA_INDEX_THREADS`.|
<!-- END CONFIG -->

## Example

`go-mysql-server` contains a SQL engine and server implementation. So, if you want to start a server, first instantiate the engine and pass your `sql.Database` implementation.

It will be in charge of handling all the logic to retrieve the data from your source.
Here you can see an example using the in-memory database implementation:

```go
...

func main() {
    driver := sqle.NewDefault()
    driver.AddDatabase(createTestDatabase())

    config := server.Config{
        Protocol: "tcp",
        Address:  "localhost:3306",
        Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
    }

    s, err := server.NewDefaultServer(config, driver)
    if err != nil {
        panic(err)
    }

    s.Start()
}

func createTestDatabase() *memory.Database {
    const (
        dbName    = "test"
        tableName = "mytable"
    )

    db := memory.NewDatabase(dbName)
    table := memory.NewTable(tableName, sql.Schema{
        {Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
        {Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
        {Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
        {Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
    })

    db.AddTable(tableName, table)
    ctx := sql.NewEmptyContext()

    rows := []sql.Row{
        sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()),
        sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()),
        sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()),
        sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()),
	}

    for _, row := range rows {
        table.Insert(ctx, row)
    }

    return db
}

...
```

Then, you can connect to the server with any MySQL client:
```bash
> mysql --host=127.0.0.1 --port=3306 -u user -ppass test -e "SELECT * FROM mytable"
+----------+-------------------+-------------------------------+---------------------+
| name     | email             | phone_numbers                 | created_at          |
+----------+-------------------+-------------------------------+---------------------+
| John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 10:42:58 |
| John Doe | johnalt@doe.com   | []                            | 2018-04-18 10:42:58 |
| Jane Doe | jane@doe.com      | []                            | 2018-04-18 10:42:58 |
| Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 10:42:58 |
+----------+-------------------+-------------------------------+---------------------+
```

See the complete example [here](_example/main.go).

### Queries examples

```
SELECT count(name) FROM mytable
+---------------------+
| COUNT(mytable.name) |
+---------------------+
|                   4 |
+---------------------+

SELECT name,year(created_at) FROM mytable
+----------+--------------------------+
| name     | YEAR(mytable.created_at) |
+----------+--------------------------+
| John Doe |                     2018 |
| John Doe |                     2018 |
| Jane Doe |                     2018 |
| Evil Bob |                     2018 |
+----------+--------------------------+

SELECT email FROM mytable WHERE name = 'Evil Bob'
+-------------------+
| email             |
+-------------------+
| evilbob@gmail.com |
+-------------------+
```

## Custom data source implementation

To be able to create your own data source implementation you need to implement the following interfaces:

- `sql.Database` interface. This interface will provide tables from your data source.
  - If your database implementation supports adding more tables, you might want to add support for `sql.Alterable` interface

- `sql.Table` interface. It will be in charge of transforming any kind of data into an iterator of Rows. Depending on how much you want to optimize the queries, you also can implement other interfaces on your tables:
  - `sql.PushdownProjectionTable` interface will provide a way to get only the columns needed for the executed query.
  - `sql.PushdownProjectionAndFiltersTable` interface will provide the same functionality described before, but also will push down the filters used in the executed query. It allows to filter data in advance, and speed up queries.
  - `sql.Indexable` add index capabilities to your table. By implementing this interface you can create and use indexes on this table.
  - `sql.Inserter` can be implemented if your data source tables allow insertions.

- If you need some custom tree modifications, you can also implement your own `analyzer.Rules`.

You can see a really simple data source implementation on our `mem` package.

## Indexes

`go-mysql-server` exposes a series of interfaces to allow you to implement your own indexes so you can speedup your queries.

Taking a look at the main [index interface](https://github.com/src-d/go-mysql-server/blob/master/sql/index.go#L35), you must note a couple of constraints:

- This abstraction lets you create an index for multiple columns (one or more) or for **only one** expression (e.g. function applied on multiple columns).
- If you want to index an expression that is not a column you will only be able to index **one and only one** expression at a time.

## Custom index driver implementation

Index drivers provide different backends for storing and querying indexes. To implement a custom index driver you need to implement a few things:

- `sql.IndexDriver` interface, which will be the driver itself. Not that your driver must return an unique ID in the `ID` method. This ID is unique for your driver and should not clash with any other registered driver. It's the driver's responsibility to be fault tolerant and be able to automatically detect and recover from corruption in indexes.
- `sql.Index` interface, returned by your driver when an index is loaded or created.
  - Your `sql.Index` may optionally implement the `sql.AscendIndex` and/or `sql.DescendIndex` interfaces, if you want to support more comparison operators like `>`, `<`, `>=`, `<=` or `BETWEEN`.
- `sql.IndexLookup` interface, returned by your index in any of the implemented operations to get a subset of the indexed values.
  - Your `sql.IndexLookup` may optionally implement the `sql.Mergeable` and `sql.SetOperations` interfaces if you want to support set operations to merge your index lookups.
- `sql.IndexValueIter` interface, which will be returned by your `sql.IndexLookup` and should return the values of the index.
- Don't forget to register the index driver in your `sql.Catalog` using `catalog.RegisterIndexDriver(mydriver)` to be able to use it.

To create indexes using your custom index driver you need to use `USING driverid` on the index creation query. For example:

```sql
CREATE INDEX foo ON table USING driverid (col1, col2)
```

You can see an example of a driver implementation inside the `sql/index/pilosa` package, where the pilosa driver is implemented.

Index creation is synchronous by default, to make it asynchronous, use `WITH (async = true)`, for example:

```sql
CREATE INDEX foo ON table USING driverid (col1, col2) WITH (async = true)
```

### Old `pilosalib` driver

`pilosalib` driver was renamed to `pilosa` and now `pilosa` does not require an external pilosa server. `pilosa` is not supported on Windows.

### Metrics

`go-mysql-server` utilizes `github.com/go-kit/kit/metrics` module to expose metrics (counters, gauges, histograms) for certain packages (so far for `engine`, `analyzer`, `regex`, `pilosa`). If you already have metrics server (prometheus, statsd/statsite, influxdb, etc.) and you want to gather metrics also from `go-mysql-server` components, you will need to initialize some global variables by particular implementations to satisfy following interfaces:
```go
// Counter describes a metric that accumulates values monotonically.
type Counter interface {
	With(labelValues ...string) Counter
	Add(delta float64)
}

// Gauge describes a metric that takes specific values over time.
type Gauge interface {
	With(labelValues ...string) Gauge
	Set(value float64)
	Add(delta float64)
}

// Histogram describes a metric that takes repeated observations of the same
// kind of thing, and produces a statistical summary of those observations,
// typically expressed as quantiles or buckets.
type Histogram interface {
	With(labelValues ...string) Histogram
	Observe(value float64)
}
```

You can use one of `go-kit` implementations or try your own.
For instance, we want to expose metrics for _prometheus_ server. So, before we start _mysql engine_, we have to set up the following variables:
```go

import(
    "github.com/go-kit/kit/metrics/prometheus"
    promopts "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

//....

// engine metrics
sqle.QueryCounter = prometheus.NewCounterFrom(promopts.CounterOpts{
		Namespace: "go_mysql_server",
		Subsystem: "engine",
		Name:      "query_counter",
	}, []string{
		"query",
	})
sqle.QueryErrorCounter = prometheus.NewCounterFrom(promopts.CounterOpts{
    Namespace: "go_mysql_server",
    Subsystem: "engine",
    Name:      "query_error_counter",
}, []string{
    "query",
    "error",
})
sqle.QueryHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "engine",
    Name:      "query_histogram",
}, []string{
    "query",
    "duration",
})

// analyzer metrics
analyzer.ParallelQueryCounter = prometheus.NewCounterFrom(promopts.CounterOpts{
    Namespace: "go_mysql_server",
    Subsystem: "analyzer",
    Name:      "parallel_query_counter",
}, []string{
    "parallelism",
})

// regex metrics
regex.CompileHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "regex",
    Name:      "compile_histogram",
}, []string{
    "regex",
    "duration",
})
regex.MatchHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "regex",
    Name:      "match_histogram",
}, []string{
    "string",
    "duration",
})

// pilosa index driver metrics
pilosa.RowsGauge = prometheus.NewGaugeFrom(promopts.GaugeOpts{
    Namespace: "go_mysql_server",
    Subsystem: "index",
    Name:      "indexed_rows_gauge",
}, []string{
    "driver",
})
pilosa.TotalHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "index",
    Name:      "index_created_total_histogram",
}, []string{
    "driver",
    "duration",
})
pilosa.MappingHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "index",
    Name:      "index_created_mapping_histogram",
}, []string{
    "driver",
    "duration",
})
pilosa.BitmapHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{
    Namespace: "go_mysql_server",
    Subsystem: "index",
    Name:      "index_created_bitmap_histogram",
}, []string{
    "driver",
    "duration",
})
```
One _important note_ - internally we set some _labels_ for metrics, that's why have to pass those keys like "duration", "query", "driver", ... when we register metrics in `prometheus`. Other systems may have different requirements.

## Powered by go-mysql-server

* [gitbase](https://github.com/src-d/gitbase)
* [dolt](https://github.com/liquidata-inc/dolt)

## License

Apache License 2.0, see [LICENSE](/LICENSE)
