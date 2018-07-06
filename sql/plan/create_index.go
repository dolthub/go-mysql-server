package plan

import (
	"fmt"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/sirupsen/logrus"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

var (
	// ErrNotIndexable is returned when the table is not indexable.
	ErrNotIndexable = errors.NewKind("the table is not indexable")

	// ErrInvalidIndexDriver is returned when the index driver can't be found.
	ErrInvalidIndexDriver = errors.NewKind("invalid driver index %q")

	// ErrTableNotNameable is returned when the table name can't be obtained.
	ErrTableNotNameable = errors.NewKind("can't get the name from the table")
)

// CreateIndex is a node to create an index.
type CreateIndex struct {
	Name            string
	Table           sql.Node
	Exprs           []sql.Expression
	Driver          string
	Config          map[string]string
	Catalog         *sql.Catalog
	CurrentDatabase string
	Async           bool
}

// NewCreateIndex creates a new CreateIndex node.
func NewCreateIndex(
	name string,
	table sql.Node,
	exprs []sql.Expression,
	driver string,
	config map[string]string,
) *CreateIndex {
	return &CreateIndex{
		Name:   name,
		Table:  table,
		Exprs:  exprs,
		Driver: driver,
		Config: config,
		Async:  config["async"] != "false",
	}
}

// Children implements the Node interface.
func (c *CreateIndex) Children() []sql.Node { return []sql.Node{c.Table} }

// Resolved implements the Node interface.
func (c *CreateIndex) Resolved() bool {
	if !c.Table.Resolved() {
		return false
	}

	for _, e := range c.Exprs {
		if !e.Resolved() {
			return false
		}
	}

	return true
}

// RowIter implements the Node interface.
func (c *CreateIndex) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	table, ok := c.Table.(sql.Indexable)
	if !ok {
		return nil, ErrNotIndexable.New()
	}

	nameable, ok := c.Table.(sql.Nameable)
	if !ok {
		return nil, ErrTableNotNameable.New()
	}

	var driver sql.IndexDriver
	if c.Driver == "" {
		driver = c.Catalog.DefaultIndexDriver()
	} else {
		driver = c.Catalog.IndexDriver(c.Driver)
	}

	if driver == nil {
		return nil, ErrInvalidIndexDriver.New(c.Driver)
	}

	columns, exprs, exprHashes, err := getColumnsAndPrepareExpressions(c.Exprs)
	if err != nil {
		return nil, err
	}

	index, err := driver.Create(
		c.CurrentDatabase,
		nameable.Name(),
		c.Name,
		exprHashes,
		c.Config,
	)
	if err != nil {
		return nil, err
	}

	iter, err := getIndexKeyValueIter(ctx, table, columns, exprs)
	if err != nil {
		return nil, err
	}

	created, ready, err := c.Catalog.AddIndex(index)
	if err != nil {
		return nil, err
	}

	log := logrus.WithFields(logrus.Fields{
		"id":     index.ID(),
		"driver": index.Driver(),
	})

	createIndex := func() {
		c.createIndex(ctx, log, driver, index, iter, created, ready)
	}

	log.WithField("async", c.Async).Info("starting to save the index")

	if c.Async {
		go createIndex()
	} else {
		createIndex()
	}

	return sql.RowsToRowIter(), nil
}

func (c *CreateIndex) createIndex(
	ctx *sql.Context,
	log *logrus.Entry,
	driver sql.IndexDriver,
	index sql.Index,
	iter sql.IndexKeyValueIter,
	done chan<- struct{},
	ready <-chan struct{},
) {
	span, ctx := ctx.Span("plan.createIndex",
		opentracing.Tags{
			"index":  index.ID(),
			"table":  index.Table(),
			"driver": index.Driver(),
		})

	l := log.WithField("id", index.ID())

	err := driver.Save(ctx, index, newLoggingKeyValueIter(ctx, l, iter))
	close(done)

	if err != nil {
		span.FinishWithOptions(opentracing.FinishOptions{
			LogRecords: []opentracing.LogRecord{
				{
					Timestamp: time.Now(),
					Fields: []otlog.Field{
						otlog.String("error", err.Error()),
					},
				},
			},
		})

		logrus.WithField("err", err).Error("unable to save the index")

		deleted, err := c.Catalog.DeleteIndex(index.Database(), index.ID(), true)
		if err != nil {
			logrus.WithField("err", err).Error("unable to delete the index")
		} else {
			<-deleted
		}
	} else {
		<-ready
		span.Finish()
		log.Info("index successfully created")
	}
}

// Schema implements the Node interface.
func (c *CreateIndex) Schema() sql.Schema { return nil }

func (c *CreateIndex) String() string {
	var exprs = make([]string, len(c.Exprs))
	for i, e := range c.Exprs {
		exprs[i] = e.String()
	}

	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateIndex(%s)", c.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("USING %s", c.Driver),
		fmt.Sprintf("Expressions (%s)", strings.Join(exprs, ", ")),
		c.Table.String(),
	)
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (c *CreateIndex) Expressions() []sql.Expression {
	return c.Exprs
}

// TransformExpressions implements the Expressioner interface.
func (c *CreateIndex) TransformExpressions(fn sql.TransformExprFunc) (sql.Node, error) {
	var exprs = make([]sql.Expression, len(c.Exprs))
	var err error
	for i, e := range c.Exprs {
		exprs[i], err = e.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	nc := *c
	nc.Exprs = exprs

	return &nc, nil
}

// TransformExpressionsUp implements the Node interface.
func (c *CreateIndex) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	table, err := c.Table.TransformExpressionsUp(fn)
	if err != nil {
		return nil, err
	}

	var exprs = make([]sql.Expression, len(c.Exprs))
	for i, e := range c.Exprs {
		exprs[i], err = e.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	nc := *c
	nc.Table = table
	nc.Exprs = exprs

	return &nc, nil
}

// TransformUp implements the Node interface.
func (c *CreateIndex) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	table, err := c.Table.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	nc := *c
	nc.Table = table

	return fn(&nc)
}

// getColumnsAndPrepareExpressions extracts the unique columns required by all
// those expressions and fixes the indexes of the GetFields in the expressions
// to match a row with only the returned columns in that same order.
func getColumnsAndPrepareExpressions(
	exprs []sql.Expression,
) ([]string, []sql.Expression, []sql.ExpressionHash, error) {
	var columns []string
	var seen = make(map[string]int)
	var expressions = make([]sql.Expression, len(exprs))
	var expressionHashes = make([]sql.ExpressionHash, len(exprs))

	for i, e := range exprs {
		ex, err := e.TransformUp(func(e sql.Expression) (sql.Expression, error) {
			gf, ok := e.(*expression.GetField)
			if !ok {
				return e, nil
			}

			var idx int
			if i, ok := seen[gf.Name()]; ok {
				idx = i
			} else {
				idx = len(columns)
				columns = append(columns, gf.Name())
				seen[gf.Name()] = idx
			}

			return expression.NewGetFieldWithTable(
				idx,
				gf.Type(),
				gf.Table(),
				gf.Name(),
				gf.IsNullable(),
			), nil
		})

		if err != nil {
			return nil, nil, nil, err
		}

		expressions[i] = ex
		expressionHashes[i] = sql.NewExpressionHash(ex)
	}

	return columns, expressions, expressionHashes, nil
}

type evalKeyValueIter struct {
	ctx   *sql.Context
	iter  sql.IndexKeyValueIter
	exprs []sql.Expression
}

func (eit *evalKeyValueIter) Next() ([]interface{}, []byte, error) {
	vals, loc, err := eit.iter.Next()
	if err != nil {
		return nil, nil, err
	}
	row := sql.NewRow(vals...)
	evals := make([]interface{}, len(eit.exprs))
	for i, ex := range eit.exprs {
		eval, err := ex.Eval(eit.ctx, row)
		if err != nil {
			return nil, nil, err
		}

		evals[i] = eval
	}

	return evals, loc, nil
}

func (eit *evalKeyValueIter) Close() error {
	return eit.iter.Close()
}

func getIndexKeyValueIter(ctx *sql.Context, table sql.Indexable, columns []string, exprs []sql.Expression) (*evalKeyValueIter, error) {
	iter, err := table.IndexKeyValueIter(ctx, columns)
	if err != nil {
		return nil, err
	}

	return &evalKeyValueIter{ctx, iter, exprs}, nil
}

type loggingKeyValueIter struct {
	ctx   *sql.Context
	span  opentracing.Span
	log   *logrus.Entry
	iter  sql.IndexKeyValueIter
	rows  uint64
	start time.Time
}

func newLoggingKeyValueIter(
	ctx *sql.Context,
	log *logrus.Entry,
	iter sql.IndexKeyValueIter,
) sql.IndexKeyValueIter {
	return &loggingKeyValueIter{
		ctx:   ctx,
		log:   log,
		iter:  iter,
		start: time.Now(),
	}
}

func (i *loggingKeyValueIter) Next() ([]interface{}, []byte, error) {
	if i.span == nil {
		i.span, _ = i.ctx.Span("plan.createIndex.iterator",
			opentracing.Tags{
				"start": i.rows,
			},
		)
	}

	i.rows++
	if i.rows%sql.IndexBatchSize == 0 {
		duration := time.Since(i.start)

		i.log.WithFields(logrus.Fields{
			"duration": duration,
			"rows":     i.rows,
		}).Debugf("still creating index")

		if i.span != nil {
			i.span.LogKV("duration", duration.String())
			i.span.Finish()
			i.span = nil
		}

		i.start = time.Now()
	}

	val, loc, err := i.iter.Next()
	if err != nil {
		i.span.LogKV("error", err)
		i.span.Finish()
		i.span = nil
	}

	return val, loc, err
}

func (i *loggingKeyValueIter) Close() error {
	return i.iter.Close()
}
