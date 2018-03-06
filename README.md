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
| Comparison expressions | !=, ==, >, <, >=,<=, BETWEEN                                |
| Null check expressions | IS NULL, IS NOT NULL                               |
|  Grouping expressions  | COUNT, MIN, MAX ,AVG                                  |
|  Standard expressions  | ALIAS, LITERAL, STAR (*)                             |
| Statements       | CROSS JOIN, INNER JOIN, DESCRIBE, FILTER (WHERE), GROUP BY, LIMIT, SELECT, SHOW TABLES, SORT, DISTINCT  |
| Functions | SUBSTRING |
| Time functions | YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFYEAR |

## Custom functions

- `IS_BINARY(blob)`: returns whether a BLOB is a binary file or not

## Powered by go-mysql-server

* [gitquery](https://github.com/src-d/gitquery)

## License

go-mysql-server is licensed under the [MIT License](/LICENSE).
