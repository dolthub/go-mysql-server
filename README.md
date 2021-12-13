# go-mysql-server

**go-mysql-server** is a SQL engine which parses standard SQL (based
on MySQL syntax) and executes queries on data sources of your
choice. A simple in-memory database and table implementation are
provided, and you can query any data source you want by implementing a
few interfaces.

**go-mysql-server** also provides a server implementation compatible
with the MySQL wire protocol. That means it is compatible with MySQL
ODBC, JDBC, or the default MySQL client shell interface.

[Dolt](https://www.doltdb.com), a SQL database with Git-style
versioning, is the main database implementation of this package.
Check out that project for reference implementations. Or, hop into the Dolt discord [here](https://discord.com/invite/RFwfYpu)
if you want to talk to the core developers behind GMS.

## Scope of this project

These are the goals of **go-mysql-server**:

- Be a generic extensible SQL engine that performs queries on your
  data sources.
- Provide a simple database implementation suitable for use in tests.
- Define interfaces you can implement to query your own data sources.
- Provide a runnable server speaking the MySQL wire protocol,
  connected to data sources of your choice.
- Optimize query plans.
- Allow implementors to add their own analysis steps and
  optimizations.
- Support indexed lookups and joins on data tables that support them.
- Support external index driver implementations such as pilosa.
- With few caveats and using a full database implementation, be a
  drop-in MySQL database replacement.

Non-goals of **go-mysql-server**:

- Be an application/server you can use directly.
- Provide any kind of backend implementation (other than the `memory`
  one used for testing) such as json, csv, yaml. That's for clients to
  implement and use.

What's the use case of **go-mysql-server**?

**go-mysql-server** has two primary uses case:

1. Stand-in for MySQL in a golang test environment, using the built-in
   `memory` database implementation.

2. Providing access to aribtrary data sources with SQL queries by
   implementing a handful of interfaces. The most complete real-world
   implementation is [Dolt](https://github.com/dolthub/dolt).

## Installation

The import path for the package is `github.com/dolthub/go-mysql-server`.

To install it, run:

```
go get github.com/dolthub/go-mysql-server
```

## Go Documentation

* [go-mysql-server godoc](https://godoc.org/github.com/dolthub/go-mysql-server)

## SQL syntax

The goal of **go-mysql-server** is to support 100% of the statements
that MySQL does. We are continuously adding more functionality to the
engine, but not everything is supported yet. To see what is currently
included check the [SUPPORTED](./SUPPORTED.md) file.

## Third-party clients

We support and actively test against certain third-party clients to
ensure compatibility between them and go-mysql-server. You can check
out the list of supported third party clients in the
[SUPPORTED_CLIENTS](./SUPPORTED_CLIENTS.md) file along with some
examples on how to connect to go-mysql-server using them.

## Available functions

<!-- BEGIN FUNCTIONS -->
|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|`ABS(expr)`| Returns the absolute value of an expression.|
|`ACOS(expr)`| Returns the arccos of an expression.|
|`ARRAY_LENGTH(expr)`| If the json representation is an array, this function returns its size.|
|`ASCII(expr)`| Returns the numeric value of the leftmost character.|
|`ASIN(expr)`| Returns the arcsin of an expression.|
|`ATAN(expr)`| Returns the arctan of an expression.|
|`AVG(expr)`| Returns the average value of expr in all rows.|
|`BIN(expr)`| Returns the binary representation of a number.|
|`BIN_TO_UUID(...)`| Converts a binary UUID to a string UUID and returns the result.|
|`BIT_LENGTH(expr)`| Returns the data length of the argument in bits.|
|`CEIL(expr)`| Returns the smallest integer value that is greater than or equal to number.|
|`CEILING(expr)`| Returns the smallest integer value that is greater than or equal to number.|
|`CHAR_LENGTH(expr)`| Returns the length of the string in characters.|
|`CHARACTER_LENGTH(expr)`| Returns the length of the string in characters.|
|`COALESCE(...)`| Returns the first non-null value in a list.|
|`CONCAT(...)`| Concatenates any group of fields into a single string.|
|`CONCAT_WS(...)`| Concatenates any group of fields into a single string. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.|
|`CONNECTION_ID()`| Returns the current connection ID.|
|`CONVERT_TZ(expr1, expr2, expr3)`| Converts a datetime value dt from the time zone given by from_tz to the time zone given by to_tz and returns the resulting value.|
|`COS(expr)`| Returns the cosine of an expression.|
|`COT(expr)`| Returns the arctangent of an expression.|
|`COUNT(expr)`| Returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.|
|`CRC32(expr)`| Returns the cyclic redundancy check value of a given string as a 32-bit unsigned value.|
|`CURDATE()`| Returns the current date.|
|`CURRENT_DATE()`| Returns the current date.|
|`CURRENT_TIME()`| Returns the current time.|
|`CURRENT_TIMESTAMP(...)`| Returns the current date and time.|
|`CURRENT_USER()`| Returns the authenticated user name and host name.|
|`CURTIME()`| Returns the current time.|
|`DATABASE()`| Returns the default (current) database name.|
|`DATE(expr)`| Returns the date part of the given date.|
|`DATE_ADD(...)`| Adds the interval to the given date.|
|`DATE_FORMAT(expr1, expr2)`| Format date as specified.|
|`DATE_SUB(...)`| Subtracts the interval from the given date.|
|`DATETIME(...)`| Returns a DATETIME value for the expression given (e.g. the string '2020-01-02').|
|`DAY(expr)`| Returns the day of the month (0-31).|
|`DAYNAME(expr)`| Returns the name of the weekday.|
|`DAYOFMONTH(expr)`| Returns the day of the month (0-31).|
|`DAYOFWEEK(expr)`| Returns the day of the week of the given date.|
|`DAYOFYEAR(expr)`| Returns the day of the year of the given date.|
|`DEGREES(expr)`| Returns the number of degrees in the radian expression given.|
|`EXPLODE(expr)`| Generates a new row in the result set for each element in the expressions provided.|
|`FIRST(expr)`| Returns the first value in a sequence of elements of an aggregation.|
|`FIRST_VALUE(expr)`| Returns value of argument from first row of window frame.|
|`FLOOR(expr)`| Returns the largest integer value that is less than or equal to number.|
|`FORMAT(...)`| Returns a number formatted to specified number of decimal places.|
|`FOUND_ROWS()`| For a SELECT with a LIMIT clause, returns the number of rows that would be returned were there no LIMIT clause.|
|`FROM_BASE64(expr)`| Decodes the base64-encoded string str.|
|`FROM_UNIXTIME(expr)`| Formats Unix timestamp as a date.|
|`GET_LOCK(expr1, expr2)`| Gets a named lock.|
|`GREATEST(...)`| Returns the greatest numeric or string value.|
|`GROUP_CONCAT()`| Returns a string result with the concatenated non-NULL values from a group.|
|`HEX(expr)`| Returns the hexadecimal representation of the string or numeric value.|
|`HOUR(expr)`| Returns the hours of the given date.|
|`IF(expr1, expr2, expr3)`| If expr1 evaluates to true, retuns expr2. Otherwise returns expr3.|
|`IFNULL(expr1, expr2)`| If expr1 is not NULL, it returns expr1; otherwise it returns expr2.|
|`INET6_ATON(expr)`| Returns the numeric value of an IPv6 address.|
|`INET6_NTOA(expr)`| Returns the IPv6 address from a numeric value.|
|`INET_ATON(expr)`| Returns the numeric value of an IP address.|
|`INET_NTOA(expr)`| Returns the IP address from a numeric value.|
|`INSTR(expr1, expr2)`| Returns the 1-based index of the first occurence of str2 in str1, or 0 if it does not occur.|
|`IS_BINARY(expr)`| Returns whether a blob is a binary file or not.|
|`IS_FREE_LOCK(expr)`| Returns whether the named lock is free.|
|`IS_IPV4(expr)`| Returns whether argument is an IPv4 address.|
|`IS_IPV4_COMPAT(expr)`| Returns whether argument is an IPv4-compatible address.|
|`IS_IPV4_MAPPED(expr)`| Returns whether argument is an IPv4-mapped address.|
|`IS_IPV6(expr)`| Returns whether argument is an IPv6 address.|
|`IS_USED_LOCK(expr)`| Returns whether the named lock is in use; return connection identifier if true.|
|`IS_UUID(expr)`| Returns whether argument is a valid UUID.|
|`ISNULL(expr)`| Returns whether a expr is null or not.|
|`LAST(expr)`| Returns the last value in a sequence of elements of an aggregation.|
|`LAST_INSERT_ID()`| Returns value of the AUTOINCREMENT column for the last INSERT.|
|`LCASE(expr)`| Returns the string str with all characters in lower case.|
|`LEAST(...)`| Returns the smaller numeric or string value.|
|`LEFT(expr1, expr2)`| Returns the first N characters in the string given.|
|`LENGTH(expr)`| Returns the length of the string in bytes.|
|`LN(expr)`| Returns the natural logarithm of X.|
|`LOAD_FILE(expr)`| Returns a LoadFile object.|
|`LOG(...)`| If called with one parameter, this function returns the natural logarithm of X. If called with two parameters, this function returns the logarithm of X to the base B. If X is less than or equal to 0, or if B is less than or equal to 1, then NULL is returned.|
|`LOG10(expr)`| Returns the base-10 logarithm of X.|
|`LOG2(expr)`| Returns the base-2 logarithm of X.|
|`LOWER(expr)`| Returns the string str with all characters in lower case.|
|`LPAD(...)`| Returns the string str, left-padded with the string padstr to a length of len characters.|
|`LTRIM(expr)`| Returns the string str with leading space characters removed.|
|`MAX(expr)`| Returns the maximum value of expr in all rows.|
|`MD5(expr)`| Calculates MD5 checksum.|
|`MICROSECOND(expr)`| Returns the microseconds from argument.|
|`MID(...)`| Returns a substring from the provided string starting at pos with a length of len characters. If no len is provided, all characters from pos until the end will be taken.|
|`MIN(expr)`| Returns the minimum value of expr in all rows.|
|`MINUTE(expr)`| Returns the minutes of the given date.|
|`MONTH(expr)`| Returns the month of the given date.|
|`MONTHNAME(expr)`| Returns the name of the month.|
|`NOW(...)`| Returns the current timestamp.|
|`NULLIF(expr1, expr2)`| Returns NULL if expr1 = expr2 is true, otherwise returns expr1.|
|`PERCENT_RANK()`| Returns percentage rank value.|
|`POW(expr1, expr2)`| Returns the value of X raised to the power of Y.|
|`POWER(expr1, expr2)`| Returns the value of X raised to the power of Y.|
|`RADIANS(expr)`| Returns the radian value of the degrees argument given.|
|`RAND(...)`| Returns a random number in the range 0 <= x < 1. If an argument is given, it is used to seed the random number generator.|
|`REGEXP_LIKE(...)`| Returns whether string matches regular expression.|
|`REGEXP_REPLACE(...)`| Replaces substrings matching regular expression.|
|`RELEASE_ALL_LOCKS()`| Release all current named locks.|
|`RELEASE_LOCK(expr)`| Release the named lock.|
|`REPEAT(expr1, expr2)`| Returns a string consisting of the string str repeated count times.|
|`REPLACE(expr1, expr2, expr3)`| Returns the string str with all occurrences of the string from_str replaced by the string to_str.|
|`REVERSE(expr)`| Returns the string str with the order of the characters reversed.|
|`RIGHT(expr1, expr2)`| Returns the specified rightmost number of characters.|
|`ROUND(...)`| Rounds the number to decimals decimal places.|
|`ROW_COUNT()`| Returns the number of rows updated.|
|`ROW_NUMBER()`| Returns the number of rows updated.|
|`RPAD(...)`| Returns the string str, right-padded with the string padstr to a length of len characters.|
|`RTRIM(expr)`| Returns the string str with trailing space characters removed.|
|`SCHEMA()`| Returns the default (current) database name.|
|`SECOND(expr)`| Returns the seconds of the given date.|
|`SHA(expr)`| Calculates an SHA-1 160-bit checksum.|
|`SHA1(expr)`| Calculates an SHA-1 160-bit checksum.|
|`SHA2(expr1, expr2)`| Calculates an SHA-2 checksum.|
|`SIGN(expr)`| Returns the sign of the argument.|
|`SIN(expr)`| Returns the sine of the expression given.|
|`SLEEP(expr)`| Waits for the specified number of seconds (can be fractional).|
|`SOUNDEX(expr)`| Returns the soundex of a string.|
|`SPLIT(expr1, expr2)`| Returns the parts of the string str split by the separator sep as a JSON array of strings.|
|`SQRT(expr)`| Returns the square root of a nonnegative number X.|
|`STR_TO_DATE(...)`| Parses the date/datetime/timestamp expression according to the format specifier.|
|`SUBSTR(...)`| Returns a substring from the provided string starting at pos with a length of len characters. If no len is provided, all characters from pos until the end will be taken.|
|`SUBSTRING(...)`| Returns a substring from the provided string starting at pos with a length of len characters. If no len is provided, all characters from pos until the end will be taken.|
|`SUBSTRING_INDEX(expr1, expr2, expr3)`| Returns a substring after count appearances of delim. If count is negative, counts from the right side of the string.|
|`SUM(expr)`| Returns the sum of expr in all rows.|
|`TAN(expr)`| Returns the tangent of the expression given.|
|`TIME_TO_SEC(expr)`| Returns the argument converted to seconds.|
|`TIMEDIFF(expr1, expr2)`| Returns expr1 − expr2 expressed as a time value. expr1 and expr2 are time or date-and-time expressions, but both must be of the same type.|
|`TIMESTAMP(...)`| Returns a timestamp value for the expression given (e.g. the string '2020-01-02').|
|`TO_BASE64(expr)`| Encodes the string str in base64 format.|
|`UCASE(expr)`| Converts string to uppercase.|
|`UNHEX(expr)`| Returns a string containing hex representation of a number.|
|`UNIX_TIMESTAMP(...)`| Returns the datetime argument to the number of seconds since the Unix epoch. With no argument, returns the number of seconds since the Unix epoch for the current time.|
|`UPPER(expr)`| Converts string to uppercase.|
|`USER()`| Returns the authenticated user name and host name.|
|`UTC_TIMESTAMP(...)`| Returns the current UTC timestamp.|
|`UUID()`| Returns a Universal Unique Identifier (UUID).|
|`UUID_TO_BIN(...)`| Converts string UUID to binary.|
|`VALUES(expr)`| Defines the values to be used during an INSERT.|
|`WEEK(...)`| Returns the week number.|
|`WEEKDAY(expr)`| Returns the weekday of the given date.|
|`WEEKOFYEAR(expr)`| Returns the calendar week of the date (1-53).|
|`YEAR(expr)`| Returns the year of the given date.|
|`YEARWEEK(...)`| Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.|
<!-- END FUNCTIONS -->

## Configuration

The behaviour of certain parts of go-mysql-server can be configured
using either environment variables or session variables.

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
<!-- END CONFIG -->

## Example

`go-mysql-server` contains a SQL engine and server implementation. So,
if you want to start a server, first instantiate the engine and pass
your `sql.Database` implementation.

It will be in charge of handling all the logic to retrieve the data
from your source. Here you can see an example using the in-memory
database implementation:

```go
package main
import (
	"time"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)
// Example of how to implement a MySQL server based on a Engine:
//
// ```
// > mysql --host=127.0.0.1 --port=3306 -u root mydb -e "SELECT * FROM mytable"
// +----------+-------------------+-------------------------------+---------------------+
// | name     | email             | phone_numbers                 | created_at          |
// +----------+-------------------+-------------------------------+---------------------+
// | John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 09:41:13 |
// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |
// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |
// | Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 09:41:13 |
// +----------+-------------------+-------------------------------+---------------------+
// ```
func main() {
	engine := sqle.NewDefault(
		sql.NewDatabaseProvider(
			createTestDatabase(),
			information_schema.NewInformationSchemaDatabase(),
		))
	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("root", "", auth.AllPermissions),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	s.Start()
}
func createTestDatabase() *memory.Database {
	const (
		dbName    = "mydb"
		tableName = "mytable"
	)
	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	}))
	
	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()
	table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()))
	table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()))
	return db
}
```

Then, you can connect to the server with any MySQL client:

```bash
> mysql --host=127.0.0.1 --port=3306 -u root mydb -e "SELECT * FROM mytable"
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

To create your own data source implementation you need to implement
the following interfaces:

- `sql.Database` interface. This interface will provide tables from
  your data source. You can also implement other interfaces on your
  database to unlock additional functionality:
  - `sql.TableCreator` to support creating new tables
  - `sql.TableDropper` to support dropping  tables
  - `sql.TableRenamer` to support renaming tables
  - `sql.ViewCreator` to support creating persisted views on your tables
  - `sql.ViewDropper` to support dropping persisted views

- `sql.Table` interface. This interface will provide rows of values
  from your data source. You can also implement other interfaces on
  your table to unlock additional functionality:
  - `sql.InsertableTable` to allow your data source to be updated with
    `INSERT` statements.
  - `sql.UpdateableTable` to allow your data source to be updated with
    `UPDATE` statements.
  - `sql.DeletableTable` to allow your data source to be updated with
    `DELETE` statements.
  - `sql.ReplaceableTable` to allow your data source to be updated with
    `REPLACE` statements.
  - `sql.AlterableTable` to allow your data source to have its schema
    modified by adding, dropping, and altering columns.
  - `sql.IndexedTable` to declare your table's native indexes to speed
    up query execution.
  - `sql.IndexAlterableTable` to accept the creation of new native
    indexes.
  - `sql.ForeignKeyAlterableTable` to signal your support of foreign
    key constraints in your table's schema and data.
  - `sql.ProjectedTable` to return rows that only contain a subset of
    the columns in the table. This can make query execution faster.
  - `sql.FilteredTable` to filter the rows returned by your table to
    those matching a given expression. This can make query execution
    faster (if your table implementation can filter rows more
    efficiently than checking an expression on every row in a table).

You can see a really simple data source implementation in the `memory`
package.

## Testing your data source implementation

**go-mysql-server** provides a suite of engine tests that you can use
to validate that your implementation works as expected. See the
`enginetest` package for details and examples.

## Indexes

`go-mysql-server` exposes a series of interfaces to allow you to
implement your own indexes so you can speed up your queries.

## Native indexes

Tables can declare that they support native indexes, which means that
they support efficiently returning a subset of their rows that match
an expression. The `memory` package contains an example of this
behavior, but please note that it is only for example purposes and
doesn't actually make queries faster (although we could change this in
the future).

Integrators should implement the `sql.IndexedTable` interface to
declare which indexes their tables support and provide a means of
returning a subset of the rows based on an `sql.IndexLookup` provided
by their `sql.Index` implementation. There are a variety of extensions
to `sql.Index` that can be implemented, each of which unlocks
additional capabilities:

- `sql.Index`. Base-level interface, supporting equality lookups for
  an index.
- `sql.AscendIndex`. Adds support for `>` and `>=` indexed lookups.
- `sql.DescendIndex`. Adds support for `<` and `<=` indexed lookups.
- `sql.NegateIndex`. Adds support for negating other index lookups.
- `sql.MergeableIndexLookup`. Adds support for merging two
  `sql.IndexLookup`s together to create a new one, representing `AND`
  and `OR` expressions on indexed columns.

## Custom index driver implementation

Index drivers provide different backends for storing and querying
indexes, without the need for a table to store and query its own
native indexes. To implement a custom index driver you need to
implement a few things:

- `sql.IndexDriver` interface, which will be the driver itself. Not
  that your driver must return an unique ID in the `ID` method. This
  ID is unique for your driver and should not clash with any other
  registered driver. It's the driver's responsibility to be fault
  tolerant and be able to automatically detect and recover from
  corruption in indexes.
- `sql.Index` interface, returned by your driver when an index is
  loaded or created.
- `sql.IndexValueIter` interface, which will be returned by your
  `sql.IndexLookup` and should return the values of the index.
- Don't forget to register the index driver in your `sql.Context`
  using `context.RegisterIndexDriver(mydriver)` to be able to use it.

To create indexes using your custom index driver you need to use
extension syntax `USING driverid` on the index creation statement. For
example:

```sql
CREATE INDEX foo ON table USING driverid (col1, col2)
```

go-mysql-server does not provide a production index driver
implementation. We previously provided a pilosa implementation, but
removed it due to the difficulty of supporting it on all platforms
(pilosa doesn't work on Windows).

You can see an example of a driver implementation in the memory
package.

### Metrics

`go-mysql-server` utilizes `github.com/go-kit/kit/metrics` module to
expose metrics (counters, gauges, histograms) for certain packages (so
far for `engine`, `analyzer`, `regex`). If you already have
metrics server (prometheus, statsd/statsite, influxdb, etc.) and you
want to gather metrics also from `go-mysql-server` components, you
will need to initialize some global variables by particular
implementations to satisfy following interfaces:

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

You can use one of `go-kit` implementations or try your own.  For
instance, we want to expose metrics for _prometheus_ server. So,
before we start _mysql engine_, we have to set up the following
variables:

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
```

One _important note_ - internally we set some _labels_ for metrics,
that's why have to pass those keys like "duration", "query", "driver",
... when we register metrics in `prometheus`. Other systems may have
different requirements.

## Powered by go-mysql-server

* [dolt](https://github.com/dolthub/dolt)
* [gitbase](https://github.com/src-d/gitbase) (defunct)

## Acknowledgements

**go-mysql-server** was originally developed by the {source-d} organzation, and this repository was originally forked from [src-d](https://github.com/src-d/go-mysql-server). We want to thank the entire {source-d} development team for their work on this project, especially Miguel Molina (@erizocosmico) and Juanjo Álvarez Martinez (@juanjux).

## License

Apache License 2.0, see [LICENSE](/LICENSE)