package plan

import (
	"github.com/opentracing/opentracing-go"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"reflect"
)

type IndexedJoin struct {
	BinaryNode
	Cond sql.Expression
	Index sql.Index
}

func (ij *IndexedJoin) String() string {
	// TODO: better String()
	return "IndexedJoin of tables "
}

func (ij *IndexedJoin) Schema() sql.Schema {
	return append(ij.Left.Schema(), ij.Right.Schema()...)
}

func (ij *IndexedJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var indexedTable sql.IndexableTable
	Inspect(ij.Right, func(node sql.Node) bool {
		if rt, ok := node.(*ResolvedTable); ok {
			if it, ok := rt.Table.(sql.IndexableTable); ok {
				indexedTable = it
				return false
			}
		}
		return true
	})

	return indexedJoinRowIter(ctx, ij.Left, indexedTable, ij.Cond, ij.Index)
}

func (ij *IndexedJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	// TODO: bounds checking
	return NewIndexedJoin(children[0], children[1], ij.Cond, ij.Index), nil
}

func NewIndexedJoin(primaryTable, indexedTable sql.Node, cond sql.Expression, index sql.Index) *IndexedJoin {
	return &IndexedJoin{
		BinaryNode: BinaryNode{primaryTable, indexedTable},
		Cond:       cond,
		Index:      index,
	}
}

func indexedJoinRowIter(
		ctx *sql.Context,
		left sql.Node,
		right sql.IndexableTable,
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
		primary:      l,
		secondaryTbl: right,
		ctx:          ctx,
		cond:         cond,
		index:        index,
	}), nil
}

// indexedJoinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type indexedJoinIter struct {
	primary      sql.RowIter
	secondaryTbl sql.IndexableTable
	secondary    sql.RowIter
	ctx          *sql.Context
	cond         sql.Expression
	index        sql.Index

	primaryRow sql.Row
	foundMatch bool
	rowSize    int
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
		// TODO: better checking, this only works for certain phrasings
		c := i.cond.Children()[1]
		key, err := c.Eval(sql.NewEmptyContext(), i.primaryRow)
		if err != nil {
			return nil, err
		}

		lookup, err := i.index.Get(key)
		if err != nil {
			return nil, err
		}
		indexLookup := i.secondaryTbl.WithIndexLookup(lookup)
		// TODO: this only works on a single partition, we will need partition info to do this correctly
		i.secondary, err = indexLookup.PartitionRows(sql.NewEmptyContext(), nil)
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

