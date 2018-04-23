# go-mysql-server

<a href="https://travis-ci.org/src-d/go-mysql-server"><img alt="Build Status" src="https://travis-ci.org/src-d/go-mysql-server.svg?branch=master" /></a>
<a href="https://codecov.io/gh/src-d/go-mysql-server"><img alt="codecov" src="https://codecov.io/gh/src-d/go-mysql-server/branch/master/graph/badge.svg" /></a>
<a href="https://godoc.org/github.com/src-d/go-mysql-server"><img alt="GoDoc" src="https://godoc.org/github.com/src-d/go-mysql-server?status.svg" /></a>

**go-mysql-server** is an extensible MySQL server implementation in Go.

## Installation

The import path for the package is `gopkg.in/src-d/go-mysql-server.v0`.

To install it, run:

```
go get gopkg.in/src-d/go-mysql-server.v0
```

## Documentation

* [go-mysql-server godoc](https://godoc.org/github.com/src-d/go-mysql-server)


## SQL syntax

We are continuously adding more functionality to go-mysql-server. We support a subset of what is supported in MySQL, currently including:

|                        |                                     Supported                                     |
|:----------------------:|:---------------------------------------------------------------------------------:|
| Comparison expressions | !=, ==, >, <, >=,<=, BETWEEN, REGEXP, IN, NOT IN |
| Null check expressions  | IS NULL, IS NOT NULL |
| Grouping expressions | COUNT, MIN, MAX ,AVG |
| Standard expressions  | ALIAS, LITERAL, STAR (*) |
| Statements  | CROSS JOIN, INNER JOIN, DESCRIBE, FILTER (WHERE), GROUP BY, LIMIT/OFFSET, SELECT, SHOW TABLES, SORT, DISTINCT, CREATE TABLE, INSERT |
| Functions | SUBSTRING, ARRAY_LENGTH |
| Time functions | YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFYEAR |

## Custom functions

- `IS_BINARY(blob)`: returns whether a BLOB is a binary file or not

## Example

`go-mysql-server` has a sql engine and a server implementation, so to start a server you must instantiate the engine and give it your `sql.Database` implementation that will be in charge to handle all the logic about retrieving the data from your source :

```go
...

func main() {
	driver := sqle.New()
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

	db := mem.NewDatabase(dbName).(*mem.Database)
	table := mem.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)
	table.Insert(sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()))
	table.Insert(sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()))
	return db
}

...
```

Then, you can connect to the server with any mysql client:
```bash
> mysql --host=127.0.0.1 --port=3306 -u user -ppass db -e "SELECT * FROM mytable"
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


## Powered by go-mysql-server

* [gitquery](https://github.com/src-d/gitquery)

## License

Apache License 2.0, see [LICENSE](/LICENSE)
