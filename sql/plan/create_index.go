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

	// ErrExprTypeNotIndexable is returned when the expression type cannot be
	// indexed, such as BLOB or JSON.
	ErrExprTypeNotIndexable = errors.NewKind("expression %q with type %s cannot be indexed")
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
	async, ok := config["async"]
	return &CreateIndex{
		Name:   name,
		Table:  table,
		Exprs:  exprs,
		Driver: driver,
		Config: config,
		Async:  async != "false" || !ok,
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

func getIndexableTable(t sql.Table) (sql.IndexableTable, error) {
	switch t := t.(type) {
	case sql.TableWrapper:
		return getIndexableTable(t.Underlying())
	case sql.IndexableTable:
		return t, nil
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

// RowIter implements the Node interface.
func (c *CreateIndex) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	table, ok := c.Table.(*ResolvedTable)
	if !ok {
		return nil, ErrNotIndexable.New()
	}

	indexable, err := getIndexableTable(table.Table)
	if err != nil {
		return nil, err
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

	columns, exprs, err := getColumnsAndPrepareExpressions(c.Exprs)
	if err != nil {
		return nil, err
	}

	for _, e := range exprs {
		if e.Type() == sql.Blob || e.Type() == sql.JSON {
			return nil, ErrExprTypeNotIndexable.New(e, e.Type())
		}
	}

	index, err := driver.Create(
		c.CurrentDatabase,
		table.Name(),
		c.Name,
		exprs,
		c.Config,
	)
	if err != nil {
		return nil, err
	}

	iter, err := indexable.IndexKeyValues(ctx, columns)
	if err != nil {
		return nil, err
	}

	iter = &evalPartitionKeyValueIter{
		ctx:     ctx,
		columns: columns,
		exprs:   exprs,
		iter:    iter,
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
		c.Catalog.ProcessList.Done(ctx.Pid())
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
	iter sql.PartitionIndexKeyValueIter,
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

	err := driver.Save(ctx, index, newLoggingPartitionKeyValueIter(ctx, l, iter))
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
) ([]string, []sql.Expression, error) {
	var columns []string
	var seen = make(map[string]int)
	var expressions = make([]sql.Expression, len(exprs))

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
			return nil, nil, err
		}

		expressions[i] = ex
	}

	return columns, expressions, nil
}

type evalPartitionKeyValueIter struct {
	table   sql.IndexableTable
	iter    sql.PartitionIndexKeyValueIter
	columns []string
	exprs   []sql.Expression
	ctx     *sql.Context
}

func (i *evalPartitionKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, iter, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	return p, &evalKeyValueIter{
		ctx:     i.ctx,
		columns: i.columns,
		exprs:   i.exprs,
		iter:    iter,
	}, nil
}

func (i *evalPartitionKeyValueIter) Close() error {
	return i.iter.Close()
}

type evalKeyValueIter struct {
	ctx     *sql.Context
	iter    sql.IndexKeyValueIter
	columns []string
	exprs   []sql.Expression
}

func (i *evalKeyValueIter) Next() ([]interface{}, []byte, error) {
	vals, loc, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	row := sql.NewRow(vals...)
	evals := make([]interface{}, len(i.exprs))
	for j, ex := range i.exprs {
		eval, err := ex.Eval(i.ctx, row)
		if err != nil {
			return nil, nil, err
		}

		evals[j] = eval
	}

	return evals, loc, nil
}

func (i *evalKeyValueIter) Close() error {
	return i.iter.Close()
}

type loggingPartitionKeyValueIter struct {
	ctx  *sql.Context
	log  *logrus.Entry
	iter sql.PartitionIndexKeyValueIter
}

func newLoggingPartitionKeyValueIter(
	ctx *sql.Context,
	log *logrus.Entry,
	iter sql.PartitionIndexKeyValueIter,
) *loggingPartitionKeyValueIter {
	return &loggingPartitionKeyValueIter{
		ctx:  ctx,
		log:  log,
		iter: iter,
	}
}

func (i *loggingPartitionKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, iter, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	return p, newLoggingKeyValueIter(i.ctx, i.log, iter), nil
}

func (i *loggingPartitionKeyValueIter) Close() error {
	return i.iter.Close()
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
) *loggingKeyValueIter {
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
