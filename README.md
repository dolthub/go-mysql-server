# go-mysql-server
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<a href="https://travis-ci.org/src-d/go-mysql-server"><img alt="Build Status" src="https://travis-ci.org/src-d/go-mysql-server.svg?branch=master" /></a>
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

The import path for the package is `gopkg.in/src-d/go-mysql-server.v0`.

To install it, run:

```
go get gopkg.in/src-d/go-mysql-server.v0
```

## Documentation

* [go-mysql-server godoc](https://godoc.org/github.com/src-d/go-mysql-server)


## SQL syntax

We are continuously adding more functionality to go-mysql-server. We support a subset of what is supported in MySQL, to see what is currently included check the [SUPPORTED](./SUPPORTED.md) file.

## Third-party clients

We support and actively test against certain third-party clients to ensure compatibility between them and go-mysql-server. You can check out the list of supported third party clients in the [SUPPORTED_CLIENTS](./SUPPORTED_CLIENTS.md) file along with some examples on how to connect to go-mysql-server using them.

## Custom functions

- `COUNT(expr)`: Returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.
- `MIN(expr)`: Returns the minimum value of expr.
- `MAX(expr)`: Returns the maximum value of expr.
- `AVG(expr)`: Returns the average value of expr.
- `SUM(expr)`: Returns the sum of expr.
- `IS_BINARY(blob)`: Returns whether a BLOB is a binary file or not.
- `SUBSTRING(str, pos)`, `SUBSTRING(str, pos, len)` : Return a substring from the provided string.
- `SUBSTR(str, pos)`, `SUBSTR(str, pos, len)` : Return a substring from the provided string.
- `MID(str, pos)`, `MID(str, pos, len)` : Return a substring from the provided string.
- Date and Timestamp functions: `YEAR(date)`, `MONTH(date)`, `DAY(date)`, `WEEKDAY(date)`, `HOUR(date)`, `MINUTE(date)`, `SECOND(date)`, `DAYOFWEEK(date)`, `DAYOFYEAR(date)`, `NOW()`.
- `ARRAY_LENGTH(json)`: If the json representation is an array, this function returns its size.
- `SPLIT(str,sep)`: Receives a string and a separator and returns the parts of the string split by the separator as a JSON array of strings.
- `CONCAT(...)`: Concatenate any group of fields into a single string.
- `CONCAT_WS(sep, ...)`: Concatenate any group of fields into a single string. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.
- `COALESCE(...)`: The function returns the first non-null value in a list.
- `LOWER(str)`, `UPPER(str)`: Receives a string and modify it changing all the chars to upper or lower case.
- `CEILING(number)`, `CEIL(number)`: Return the smallest integer value that is greater than or equal to `number`.
- `FLOOR(number)`: Return the largest integer value that is less than or equal to `number`.
- `ROUND(number, decimals)`: Round the `number` to `decimals` decimal places.
- `CONNECTION_ID()`: Return the current connection ID.
- `SOUNDEX(str)`: Returns the soundex of a string.
- `JSON_EXTRACT(json_doc, path, ...)`:  Extracts data from a json document using json paths.
- `LN(X)`: Return the natural logarithm of X.
- `LOG2(X)`: Returns the base-2 logarithm of X.
- `LOG10(X)`: Returns the base-10 logarithm of X.
- `LOG(X), LOG(B, X)`: If called with one parameter, this function returns the natural logarithm of X. If called with two parameters, this function returns the logarithm of X to the base B. If X is less than or equal to 0, or if B is less than or equal to 1, then NULL is returned.
- `RPAD(str, len, padstr)`: Returns the string str, right-padded with the string padstr to a length of len characters.
- `LPAD(str, len, padstr)`: Return the string argument, left-padded with the specified string.
- `SQRT(X)`: Returns the square root of a nonnegative number X.
- `POW(X, Y)`, `POWER(X, Y)`: Returns the value of X raised to the power of Y.
- `TRIM(str)`: Returns the string str with all spaces removed.
- `LTRIM(str)`: Returns the string str with leading space characters removed.
- `RTRIM(str)`: Returns the string str with trailing space characters removed.
- `REVERSE(str)`: Returns the string str with the order of the characters reversed.
- `REPEAT(str, count)`: Returns a string consisting of the string str repeated count times.
- `REPLACE(str,from_str,to_str)`: Returns the string str with all occurrences of the string from_str replaced by the string to_str.
- `IFNULL(expr1, expr2)`: If expr1 is not NULL, IFNULL() returns expr1; otherwise it returns expr2.
- `NULLIF(expr1, expr2)`: Returns NULL if expr1 = expr2 is true, otherwise returns expr1.

## Example

`go-mysql-server` contains a SQL engine and server implementation. So, if you want to start a server, first instantiate the engine and pass your `sql.Database` implementation.

It will be in charge of handling all the logic to retrieve the data from your source.
Here you can see an example using the in-memory database implementation:

```go
...

func main() {
    driver := sqle.NewDefault()
    driver.AddDatabase(createTestDatabase())

    auth := mysql.NewAuthServerStatic()
    auth.Entries["user"] = []*mysql.AuthServerStaticEntry{{
        Password: "pass",
    }}

    config := server.Config{
        Protocol: "tcp",
        Address:  "localhost:3306",
        Auth:     auth,
    }

    s, err := server.NewDefaultServer(config, driver)
    if err != nil {
        panic(err)
    }

    s.Start()
}

func createTestDatabase() *mem.Database {
    const (
        dbName    = "test"
        tableName = "mytable"
    )

    db := mem.NewDatabase(dbName)
    table := mem.NewTable(tableName, sql.Schema{
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

`pilosalib` driver was renamed to `pilosa` and now `pilosa` does not require an external pilosa server.

## Powered by go-mysql-server

* [gitbase](https://github.com/src-d/gitbase)

## License

Apache License 2.0, see [LICENSE](/LICENSE)
