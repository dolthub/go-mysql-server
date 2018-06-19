package plan

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ErrUnableSort is thrown when something happens on sorting
var ErrUnableSort = errors.NewKind("unable to sort")

// Sort is the sort node.
type Sort struct {
	UnaryNode
	SortFields []SortField
}

// SortOrder represents the order of the sort (ascending or descending).
type SortOrder byte

const (
	// Ascending order.
	Ascending SortOrder = 1
	// Descending order.
	Descending SortOrder = 2
)

func (s SortOrder) String() string {
	switch s {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	default:
		return "invalid SortOrder"
	}
}

// NullOrdering represents how to order based on null values.
type NullOrdering byte

const (
	// NullsFirst puts the null values before any other values.
	NullsFirst NullOrdering = iota
	// NullsLast puts the null values after all other values.
	NullsLast NullOrdering = 2
)

// SortField is a field by which the query will be sorted.
type SortField struct {
	// Column to order by.
	Column sql.Expression
	// Order type.
	Order SortOrder
	// NullOrdering defining how nulls will be ordered.
	NullOrdering NullOrdering
}

// NewSort creates a new Sort node.
func NewSort(sortFields []SortField, child sql.Node) *Sort {
	return &Sort{
		UnaryNode:  UnaryNode{child},
		SortFields: sortFields,
	}
}

// Resolved implements the Resolvable interface.
func (s *Sort) Resolved() bool {
	return s.UnaryNode.Child.Resolved() && s.expressionsResolved()
}

func (s *Sort) expressionsResolved() bool {
	for _, f := range s.SortFields {
		if !f.Column.Resolved() {
			return false
		}
	}
	return true
}

// RowIter implements the Node interface.
func (s *Sort) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := s.UnaryNode.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(s, i)), nil
}

// TransformUp implements the Transformable interface.
func (s *Sort) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := s.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewSort(s.SortFields, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (s *Sort) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var sfs = make([]SortField, len(s.SortFields))
	for i, sf := range s.SortFields {
		col, err := sf.Column.TransformUp(f)
		if err != nil {
			return nil, err
		}
		sfs[i] = SortField{col, sf.Order, sf.NullOrdering}
	}

	child, err := s.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewSort(sfs, child), nil
}

func (s *Sort) String() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(s.SortFields))
	for i, f := range s.SortFields {
		fields[i] = fmt.Sprintf("%s %s", f.Column, f.Order)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(fields, ", "))
	_ = pr.WriteChildren(s.Child.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (s *Sort) Expressions() []sql.Expression {
	var exprs = make([]sql.Expression, len(s.SortFields))
	for i, f := range s.SortFields {
		exprs[i] = f.Column
	}
	return exprs
}

// TransformExpressions implements the Expressioner interface.
func (s *Sort) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	var sortFields = make([]SortField, len(s.SortFields))
	for i, field := range s.SortFields {
		transformed, err := field.Column.TransformUp(f)
		if err != nil {
			return nil, err
		}
		sortFields[i] = SortField{
			Column:       transformed,
			Order:        field.Order,
			NullOrdering: field.NullOrdering,
		}
	}

	return NewSort(sortFields, s.Child), nil
}

type sortIter struct {
	s          *Sort
	childIter  sql.RowIter
	sortedRows []sql.Row
	idx        int
}

func newSortIter(s *Sort, child sql.RowIter) *sortIter {
	return &sortIter{
		s:          s,
		childIter:  child,
		sortedRows: nil,
		idx:        -1,
	}
}

func (i *sortIter) Next() (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeSortedRows()
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}
	if i.idx >= len(i.sortedRows) {
		return nil, io.EOF
	}
	row := i.sortedRows[i.idx]
	i.idx++
	return row, nil
}

func (i *sortIter) Close() error {
	i.sortedRows = nil
	return i.childIter.Close()
}

func (i *sortIter) computeSortedRows() error {
	var rows []sql.Row
	for {
		childRow, err := i.childIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rows = append(rows, childRow)
	}

	sorter := &sorter{
		sortFields: i.s.SortFields,
		rows:       rows,
		lastError:  nil,
	}
	sort.Stable(sorter)
	if sorter.lastError != nil {
		return sorter.lastError
	}
	i.sortedRows = rows
	return nil
}

type sorter struct {
	sortFields []SortField
	rows       []sql.Row
	lastError  error
	ctx        *sql.Context
}

func (s *sorter) Len() int {
	return len(s.rows)
}

func (s *sorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func (s *sorter) Less(i, j int) bool {
	if s.lastError != nil {
		return false
	}

	a := s.rows[i]
	b := s.rows[j]
	for _, sf := range s.sortFields {
		typ := sf.Column.Type()
		av, err := sf.Column.Eval(s.ctx, a)
		if err != nil {
			s.lastError = ErrUnableSort.Wrap(err)
			return false
		}

		bv, err := sf.Column.Eval(s.ctx, b)
		if err != nil {
			s.lastError = ErrUnableSort.Wrap(err)
			return false
		}

		if av == nil {
			return sf.NullOrdering == NullsFirst
		}

		if bv == nil {
			return sf.NullOrdering != NullsFirst
		}

		if sf.Order == Descending {
			av, bv = bv, av
		}

		cmp, err := typ.Compare(av, bv)
		if err != nil {
			s.lastError = err
			return false
		}

		switch cmp {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return false
}
