package plan

import (
	"fmt"
	"strings"
	// "time"

	// opentracing "github.com/opentracing/opentracing-go"
	// otlog "github.com/opentracing/opentracing-go/log"
	// "github.com/sirupsen/logrus"
	"github.com/src-d/go-mysql-server/sql"
	// "github.com/src-d/go-mysql-server/sql/expression"
	// errors "gopkg.in/src-d/go-errors.v1"
)

type CreateView struct {
    Database     sql.Database
    Name   string
    Columns []string
    Definition *SubqueryAlias
}

func NewCreateView(
	database     sql.Database,
	name   string,
	columns []string,
	definition *SubqueryAlias,
) *CreateView {
	return &CreateView{
		Database: database,
		Name: name,
		Columns: columns,
		Definition: definition,
	}
}

// Children implements the Node interface.
func (c *CreateView) Children() []sql.Node { return nil }

// Resolved implements the Node interface.
func (c *CreateView) Resolved() bool {
	_, ok := c.Database.(sql.UnresolvedDatabase)
	return !ok && c.Definition.Resolved()
}
//
// func getIndexableTable(t sql.Table) (sql.IndexableTable, error) {
// 	switch t := t.(type) {
// 	case sql.IndexableTable:
// 		return t, nil
// 	case sql.TableWrapper:
// 		return getIndexableTable(t.Underlying())
// 	default:
// 		return nil, ErrNotIndexable.New()
// 	}
// }
//
// func getChecksumable(t sql.Table) sql.Checksumable {
// 	switch t := t.(type) {
// 	case sql.Checksumable:
// 		return t
// 	case sql.TableWrapper:
// 		return getChecksumable(t.Underlying())
// 	default:
// 		return nil
// 	}
// }
//
// RowIter implements the Node interface.
func (c *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	// TODO: add it to the register

	// table, ok := c.Table.(*ResolvedTable)
	// if !ok {
	// 	return nil, ErrNotIndexable.New()
	// }
	//
	// indexable, err := getIndexableTable(table.Table)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// var driver sql.IndexDriver
	// if c.Driver == "" {
	// 	driver = c.Catalog.DefaultIndexDriver()
	// } else {
	// 	driver = c.Catalog.IndexDriver(c.Driver)
	// }
	//
	// if driver == nil {
	// 	return nil, ErrInvalidIndexDriver.New(c.Driver)
	// }
	//
	// columns, exprs, err := getColumnsAndPrepareExpressions(c.Exprs)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// for _, e := range exprs {
	// 	if e.Type() == sql.Blob || e.Type() == sql.JSON {
	// 		return nil, ErrExprTypeNotIndexable.New(e, e.Type())
	// 	}
	// }
	//
	// if ch := getChecksumable(table.Table); ch != nil {
	// 	c.Config[sql.ChecksumKey], err = ch.Checksum()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	//
	// index, err := driver.Create(
	// 	c.CurrentDatabase,
	// 	table.Name(),
	// 	c.Name,
	// 	exprs,
	// 	c.Config,
	// )
	// if err != nil {
	// 	return nil, err
	// }
	//
	// iter, err := indexable.IndexKeyValues(ctx, columns)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// iter = &evalPartitionKeyValueIter{
	// 	ctx:     ctx,
	// 	columns: columns,
	// 	exprs:   exprs,
	// 	iter:    iter,
	// }
	//
	// created, ready, err := c.Catalog.AddIndex(index)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// log := logrus.WithFields(logrus.Fields{
	// 	"id":     index.ID(),
	// 	"driver": index.Driver(),
	// })
	//
	// createIndex := func() {
	// 	c.createIndex(ctx, log, driver, index, iter, created, ready)
	// 	c.Catalog.ProcessList.Done(ctx.Pid())
	// }
	//
	// log.WithField("async", c.Async).Info("starting to save the index")
	//
	// if c.Async {
	// 	go createIndex()
	// } else {
	// 	createIndex()
	// }

	return sql.RowsToRowIter(), nil
}

// Schema implements the Node interface.
func (c *CreateView) Schema() sql.Schema { return nil }

func (create *CreateView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateView(%s)", create.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Columns (%s)", strings.Join(create.Columns, ", ")),
		fmt.Sprintf("As (%s)", create.Definition.String()),
	)
	return pr.String()
}

// WithChildren implements the Node interface.
func (c *CreateView) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	return c, nil
}
