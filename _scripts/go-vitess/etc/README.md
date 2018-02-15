# go-vitess [![GoDoc](https://godoc.org/gopkg.in/src-d/go-vitess.v0?status.svg)](https://godoc.org/gopkg.in/src-d/go-vitess.v0)

`go-vitess` is an automatic filter-branch done by an [script](https://github.com/src-d/go-mysql-server/blob/master/_scripts/go-vitess/Makefile), of the great  [Vitess](github.com/youtube/vitess) project.

The goal is keeping the `github.com/youtube/vitess/go/mysql` package and all the dependent packages as a standalone versioned golang library, to be used by other projects.

It holds all the packages to create your own MySQL server and a full SQL parser.

## Installation

```sh
go get -v -u gopkg.in/src-d/go-vitess.v0/...
```

## Contributions

Since the code belongs to the upstream of [Vitess](github.com/youtube/vitess),
the issue neither pull requests aren't accepted to this repository.

## License

Apache License 2.0, see [LICENSE.md](LICENSE.md).
