package sqle

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
)

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
}

// New creates a new Engine.
func New() *Engine {
	c := sql.NewCatalog()
	c.RegisterFunctions(function.Defaults)

	a := analyzer.New(c)
	return &Engine{c, a}
}

// Query executes a query without attaching to any context.
func (e *Engine) Query(
	ctx *sql.Context,
	query string,
) (sql.Schema, sql.RowIter, error) {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err := analyzed.RowIter(ctx)
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

// AddDatabase adds the given database to the catalog.
func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.Databases = append(e.Catalog.Databases, db)
	e.Analyzer.CurrentDatabase = db.Name()
}
