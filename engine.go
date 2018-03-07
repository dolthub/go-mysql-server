package sqle

import (
	"errors"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
)

var (
	// ErrNotSupported is thrown when a feature which is not supported is used.
	ErrNotSupported = errors.New("feature not supported yet")
)

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
}

// New creates a new Engine.
func New() *Engine {
	c := sql.NewCatalog()
	err := expression.RegisterDefaults(c)
	if err != nil {
		panic(err)
	}

	a := analyzer.New(c)
	return &Engine{c, a}
}

// Query executes a query without attaching to any session.
func (e *Engine) Query(query string) (sql.Schema, sql.RowIter, error) {
	parsed, err := parse.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err := analyzed.RowIter()
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
