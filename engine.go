package sqle

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/metrics/discard"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"

	"github.com/liquidata-inc/go-mysql-server/auth"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/analyzer"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
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
	LS       *sql.LockSubsystem
}

var (
	// QueryCounter describes a metric that accumulates number of queries monotonically.
	QueryCounter = discard.NewCounter()

	// QueryErrorCounter describes a metric that accumulates number of failed queries monotonically.
	QueryErrorCounter = discard.NewCounter()

	// QueryHistogram describes a queries latency.
	QueryHistogram = discard.NewHistogram()
)

func observeQuery(ctx *sql.Context, query string) func(err error) {
	logrus.WithField("query", query).Debug("executing query")
	span, _ := ctx.Span("query", opentracing.Tag{Key: "query", Value: query})

	t := time.Now()
	return func(err error) {
		if err != nil {
			QueryErrorCounter.With("query", query, "error", err.Error()).Add(1)
		} else {
			QueryCounter.With("query", query).Add(1)
			QueryHistogram.With("query", query, "duration", "seconds").Observe(time.Since(t).Seconds())
		}

		span.Finish()
	}
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`.
func New(c *sql.Catalog, a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
	}

	ls := sql.NewLockSubsystem()

	c.MustRegister(
		sql.FunctionN{
			Name: "version",
			Fn:   function.NewVersion(versionPostfix),
		},
		sql.Function0{
			Name: "database",
			Fn:   function.NewDatabase(c),
		},
		sql.Function0{
			Name: "schema",
			Fn:   function.NewDatabase(c),
		})

	c.MustRegister(function.Defaults...)
	c.MustRegister(function.GetLockingFuncs(ls)...)

	// use auth.None if auth is not specified
	var au auth.Auth
	if cfg == nil || cfg.Auth == nil {
		au = new(auth.None)
	} else {
		au = cfg.Auth
	}

	return &Engine{c, a, au, ls}
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
	var (
		parsed, analyzed sql.Node
		iter             sql.RowIter
		err              error
	)

	finish := observeQuery(ctx, query)
	defer finish(err)

	parsed, err = parse.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	var perm = auth.ReadPerm
	var typ = sql.QueryProcess
	switch parsed.(type) {
	case *plan.CreateIndex:
		typ = sql.CreateIndexProcess
		perm = auth.ReadPerm | auth.WritePerm
	case *plan.CreateForeignKey, *plan.DropForeignKey, *plan.AlterIndex, *plan.CreateView,
		*plan.DeleteFrom, *plan.DropIndex, *plan.DropView,
		*plan.InsertInto, *plan.LockTables, *plan.UnlockTables,
		*plan.Update:
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

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, nil, err
	}

	iter, err = analyzed.RowIter(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

// ApplyDefaults applies the default values of the given column indices to the given row, and returns a new row with the updated values.
// This assumes that the given row has placeholder `nil` values for the default entries, and also that each column in a table is
// present and in the order as represented by the schema. If no columns are given, then the given row is returned. Column indices should
// be sorted and in ascending order, however this is not enforced.
func (e *Engine) ApplyDefaults(ctx *sql.Context, database, table string, cols []int, row sql.Row) (sql.Row, error) {
	if len(cols) == 0 {
		return row, nil
	}
	newRow := row.Copy()
	if database == "" {
		database = ctx.GetCurrentDatabase()
	}
	tbl, err := e.Catalog.Table(ctx, database, table)
	if err != nil {
		return nil, err
	}
	tblSch := tbl.Schema()
	if len(tblSch) != len(row) {
		return nil, fmt.Errorf("any row given to ApplyDefaults must be of the same length as the table it represents")
	}
	for _, col := range cols {
		if col < 0 || col > len(tblSch) {
			return nil, fmt.Errorf("column index `%d` is out of bounds, table schema has `%d` number of columns", col, len(tblSch))
		}
		val, err := tblSch[col].Default.Eval(ctx, newRow)
		if err != nil {
			return nil, err
		}
		newRow[col], err = tblSch[col].Type.Convert(val)
		if err != nil {
			return nil, err
		}
	}
	return newRow, nil
}

// Async returns true if the query is async. If there are any errors with the
// query it returns false
func (e *Engine) Async(ctx *sql.Context, query string) bool {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return false
	}

	asyncNode, ok := parsed.(sql.AsyncNode)
	return ok && asyncNode.IsAsync()
}

// AddDatabase adds the given database to the catalog.
func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.AddDatabase(db)
}
