package sqle // import "gopkg.in/src-d/go-mysql-server.v0"

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
}

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`.
func New(c *sql.Catalog, a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
	}

	c.RegisterFunctions(function.Defaults)
	c.RegisterFunction("version", sql.FunctionN(function.NewVersion(versionPostfix)))

	return &Engine{c, a}
}

// NewDefault creates a new default Engine.
func NewDefault() *Engine {
	c := sql.NewCatalog()
	a := analyzer.NewDefault(c)

	return New(c, a, nil)
}

// Query executes a query without attaching to any context.
func (e *Engine) Query(
	ctx *sql.Context,
	query string,
) (sql.Schema, sql.RowIter, error) {
	span, ctx := ctx.Span("query", opentracing.Tag{Key: "query", Value: query})
	defer span.Finish()

	logrus.WithField("query", query).Debug("executing query")

	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(ctx, parsed)
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

// Init performs all the initialization requirements for the engine to work.
func (e *Engine) Init() error {
	return e.Catalog.LoadIndexes(e.Catalog.Databases)
}
