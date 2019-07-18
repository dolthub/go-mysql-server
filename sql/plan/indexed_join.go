package plan

import (
	"errors"
	"github.com/opentracing/opentracing-go"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"reflect"
)

type IndexedJoin struct {
	BinaryNode
	Cond sql.Expression
	Index sql.Index
	leftTableExpr sql.Expression
}

func (ij *IndexedJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexedJoin(%s)", ij.Cond)
	_ = pr.WriteChildren(ij.Left.String(), ij.Right.String())
	return pr.String()
}

func (ij *IndexedJoin) Schema() sql.Schema {
	return append(ij.Left.Schema(), ij.Right.Schema()...)
}

func (ij *IndexedJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var indexedTable sql.IndexableTable
	foundIndexedTable := false
	Inspect(ij.Right, func(node sql.Node) bool {
		// TODO: this is a bit fragile and only works for two table joins
		if rt, ok := node.(*ResolvedTable); ok {
			if it, ok := rt.Table.(sql.IndexableTable); ok {
				indexedTable = it
				foundIndexedTable = true
				return false
			}
		}
		return true
	})

	if !foundIndexedTable {
		return nil, errors.New("expected an IndexableTable, couldn't find one")
	}

	return indexedJoinRowIter(ctx, ij.Left, indexedTable, ij.leftTableExpr, ij.Cond, ij.Index)
}

func (ij *IndexedJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(ij, len(children), 2)
	}
	return NewIndexedJoin(children[0], children[1], ij.Cond, ij.leftTableExpr, ij.Index), nil
}

func NewIndexedJoin(primaryTable, indexedTable sql.Node, cond sql.Expression, leftTableExpr sql.Expression, index sql.Index) *IndexedJoin {
	return &IndexedJoin{
		BinaryNode: BinaryNode{primaryTable, indexedTable},
		Cond:       cond,
		Index:      index,
		leftTableExpr: leftTableExpr,
	}
}

func indexedJoinRowIter(
		ctx *sql.Context,
		left sql.Node,
		right sql.IndexableTable,
		leftTableExpr sql.Expression,
		cond sql.Expression,
		index sql.Index,
) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := left.(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(left).String()
	}

	if rightTable, ok := right.(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(right).String()
	}

	span, ctx := ctx.Span("plan.indexedJoin", opentracing.Tags{
		"left":  leftName,
		"right": rightName,
	})

	l, err := left.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &indexedJoinIter{
		primary:       l,
		secondaryTbl:  right,
		ctx:           ctx,
		cond:          cond,
		leftTableExpr: leftTableExpr,
		index:         index,
	}), nil
}

// indexedJoinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type indexedJoinIter struct {
	primary       sql.RowIter
	primaryRow    sql.Row
	index         sql.Index
	secondaryTbl  sql.IndexableTable
	secondary     sql.RowIter
	leftTableExpr sql.Expression
	cond          sql.Expression

	ctx           *sql.Context
	foundMatch    bool
	rowSize       int
}

func (i *indexedJoinIter) loadPrimary() error {
	if i.primaryRow == nil {
		r, err := i.primary.Next()
		if err != nil {
			return err
		}

		i.primaryRow = r
		i.foundMatch = false
	}

	return nil
}

func (i *indexedJoinIter) loadSecondary() (sql.Row, error) {
	if i.secondary == nil {
		// evaluate the primary row against the left-hand condition to get the right-hand lookup key
		key, err := i.leftTableExpr.Eval(i.ctx, i.primaryRow)
		if err != nil {
			return nil, err
		}

		lookup, err := i.index.Get(key)
		if err != nil {
			return nil, err
		}
		indexLookup := i.secondaryTbl.WithIndexLookup(lookup)
		// TODO: this only works on a single partition, we will need partition info to do this correctly
		i.secondary, err = indexLookup.PartitionRows(i.ctx, nil)
		if err != nil {
			return nil, err
		}
	}

	rightRow, err := i.secondary.Next()
	if err != nil {
		if err == io.EOF {
			i.secondary = nil
			i.primaryRow = nil
			return nil, io.EOF
		}
		return nil, err
	}

	return rightRow, nil
}


func (i *indexedJoinIter) Next() (sql.Row, error) {
	for {
		if err := i.loadPrimary(); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary()
		if err != nil {
			if err == io.EOF {
				continue
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		v, err := i.cond.Eval(i.ctx, row)
		if err != nil {
			return nil, err
		}

		if v == false {
			continue
		}

		i.foundMatch = true
		return row, nil
	}
}

// buildRow builds the resulting row using the rows from the primary and
// secondary branches depending on the join type.
func (i *indexedJoinIter) buildRow(primary, secondary sql.Row) sql.Row {
	var row sql.Row
	if i.rowSize > 0 {
		row = make(sql.Row, i.rowSize)
	} else {
		row = make(sql.Row, len(primary)+len(secondary))
		i.rowSize = len(row)
	}

	copy(row, primary)
	copy(row[len(primary):], secondary)

	return row
}

func (i *indexedJoinIter) Close() (err error) {
	i.secondary = nil

	if i.primary != nil {
		if err = i.primary.Close(); err != nil {
			if i.secondary != nil {
				_ = i.secondary.Close()
			}
			return err
		}

	}

	if i.secondary != nil {
		err = i.secondary.Close()
	}

	return err
}

