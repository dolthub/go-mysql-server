package sqle // import "github.com/src-d/go-mysql-server"

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression/function"
	"github.com/src-d/go-mysql-server/sql/parse"
	"github.com/src-d/go-mysql-server/sql/plan"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// Auth used for authentication and authorization.
	Auth auth.Auth
}

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
	Auth     auth.Auth
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`.
func New(c *sql.Catalog, a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
	}

	c.MustRegister(
		sql.FunctionN{
			Name: "version",
			Fn:   function.NewVersion(versionPostfix),
		},
		sql.Function0{
			Name: "database",
			Fn:   function.NewDatabase(c),
		})
	c.MustRegister(function.Defaults...)

	// use auth.None if auth is not specified
	var au auth.Auth
	if cfg == nil || cfg.Auth == nil {
		au = new(auth.None)
	} else {
		au = cfg.Auth
	}

	return &Engine{c, a, au}
}

// NewDefault creates a new default Engine.
func NewDefault() *Engine {
	c := sql.NewCatalog()
	a := analyzer.NewDefault(c)

	return New(c, a, nil)
}

// Query executes a query.
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

	var perm = auth.ReadPerm
	var typ = sql.QueryProcess
	switch parsed.(type) {
	case *plan.CreateIndex:
		typ = sql.CreateIndexProcess
		perm = auth.ReadPerm | auth.WritePerm
	case *plan.InsertInto, *plan.DropIndex, *plan.UnlockTables, *plan.LockTables:
		perm = auth.ReadPerm | auth.WritePerm
	}

	err = e.Auth.Allowed(ctx, perm)
	if err != nil {
		return nil, nil, err
	}

	ctx, err = e.Catalog.AddProcess(ctx, typ, query)
	defer func() {
		if err != nil && ctx != nil {
			e.Catalog.Done(ctx.Pid())
		}
	}()

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
	e.Catalog.AddDatabase(db)
}

// Init performs all the initialization requirements for the engine to work.
func (e *Engine) Init() error {
	return e.Catalog.LoadIndexes(e.Catalog.AllDatabases())
}
