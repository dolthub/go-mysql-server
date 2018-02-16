# sqle

<a href="https://codebeat.co/projects/github-com-sqle-sqle"><img alt="codebeat badge" src="https://codebeat.co/badges/10f09016-1074-43d3-916a-4b4e628e79c0" /></a>
<a href="https://travis-ci.org/src-d/go-mysql-server"><img alt="Build Status" src="https://travis-ci.org/src-d/go-mysql-server.svg?branch=master" /></a>
<a href="https://codecov.io/gh/src-d/go-mysql-server"><img alt="codecov" src="https://codecov.io/gh/src-d/go-mysql-server/branch/master/graph/badge.svg" /></a>
<a href="https://godoc.org/github.com/src-d/go-mysql-server"><img alt="GoDoc" src="https://godoc.org/github.com/src-d/go-mysql-server?status.svg" /></a>

## Installation

The import path for the package is `gopkg.in/src-d/go-mysql-server.v0`.

To install it, run:

```
go get gopkg.in/src-d/go-mysql-server.v0
```

## Documentation

* [sqle godoc](https://godoc.org/github.com/src-d/go-mysql-server)


## SQL syntax

We are continuously adding more functionality to gitql. We support a subset of the SQL standard, currently including:

|                        |                                     Supported                                     |
|:----------------------:|:---------------------------------------------------------------------------------:|
| Comparison expressions |                                !=, ==, >, <, >=,<=                                |
| Null check expressions |                                IS NULL, IS NOT NULL                               |
|  Grouping expressions  |                                    COUNT, FIRST                                   |
|  Standard expressions  |                              ALIAS, LITERAL, STAR (*)                             |
|       Statements       | CROSS JOIN, DESCRIBE, FILTER (WHERE), GROUP BY, LIMIT, SELECT, SHOW TABLES, SORT  |

## Powered by sqle

* [gitql](https://github.com/sqle/gitql)

## License

sqle is licensed under the [MIT License](https://github.com/sqle/sqle/blob/master/LICENSE).

