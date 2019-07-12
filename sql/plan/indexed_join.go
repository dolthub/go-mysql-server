package plan

import (
	"github.com/opentracing/opentracing-go"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"reflect"
)

func indexedJoinRowIter(
		ctx *sql.Context,
		typ joinType,
		left sql.Node,
		right sql.IndexableTable,
		cond sql.Expression,
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

	span, ctx := ctx.Span("plan."+typ.String(), opentracing.Tags{
		"left":  leftName,
		"right": rightName,
	})

	l, err := left.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &indexedJoinIter{
		typ:               typ,
		primary:           l,
		secondaryTbl:      right,
		secondaryProvider: makeIndexProvider(right),
		ctx:               ctx,
		cond:              cond,
	}), nil
}

func makeIndexProvider(tbl sql.IndexableTable) sql.IndexRowIterProvider {
	return nil
}

// joinIter is a generic iterator for all join types.
type indexedJoinIter struct {
	typ               joinType
	primary           sql.RowIter
	secondaryTbl      sql.IndexableTable
	secondaryProvider sql.IndexRowIterProvider
	secondary         sql.RowIter
	ctx               *sql.Context
	cond              sql.Expression

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

func (i *indexedJoinIter) loadSecondary() (row sql.Row, err error) {
	if i.secondary == nil {
		var iter sql.RowIter
		iter, err = i.secondaryProvider.RowIter(i.ctx, nil)
		if err != nil {
			return nil, err
		}

		i.secondary = iter
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
				if !i.foundMatch && (i.typ == leftJoin || i.typ == rightJoin) {
					return i.buildRow(primary, nil), nil
				}
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

