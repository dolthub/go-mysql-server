package plan

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
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

func (s SortField) DebugString() string {
	nullOrdering := "nullsFirst"
	if s.NullOrdering == NullsLast {
		nullOrdering = "nullsLast"
	}
	return fmt.Sprintf("%s %s %s", sql.DebugString(s.Column), s.Order, nullOrdering)
}

// NewSort creates a new Sort node.
func NewSort(sortFields []SortField, child sql.Node) *Sort {
	return &Sort{
		UnaryNode:  UnaryNode{child},
		SortFields: sortFields,
	}
}

var _ sql.Expressioner = (*Sort)(nil)

// Resolved implements the Resolvable interface.
func (s *Sort) Resolved() bool {
	for _, f := range s.SortFields {
		if !f.Column.Resolved() {
			return false
		}
	}
	return s.Child.Resolved()
}

// RowIter implements the Node interface.
func (s *Sort) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := s.UnaryNode.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(ctx, s, i, row)), nil
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

func (s *Sort) DebugString() string {
	pr := sql.NewTreePrinter()
	var fields = make([]string, len(s.SortFields))
	for i, f := range s.SortFields {
		fields[i] = sql.DebugString(f)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(fields, ", "))
	_ = pr.WriteChildren(sql.DebugString(s.Child))
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

// WithChildren implements the Node interface.
func (s *Sort) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	return NewSort(s.SortFields, children[0]), nil
}

// WithExpressions implements the Expressioner interface.
func (s *Sort) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.SortFields) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.SortFields))
	}

	var fields = make([]SortField, len(s.SortFields))
	for i, expr := range exprs {
		fields[i] = SortField{
			Column:       expr,
			NullOrdering: s.SortFields[i].NullOrdering,
			Order:        s.SortFields[i].Order,
		}
	}

	return NewSort(fields, s.Child), nil
}

type sortIter struct {
	ctx        *sql.Context
	s          *Sort
	row        sql.Row
	childIter  sql.RowIter
	sortedRows []sql.Row
	idx        int
}

func newSortIter(ctx *sql.Context, s *Sort, child sql.RowIter, row sql.Row) *sortIter {
	return &sortIter{
		ctx:       ctx,
		s:         s,
		childIter: child,
		row:       row,
		idx:       -1,
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
	cache, dispose := i.ctx.Memory.NewRowsCache()
	defer dispose()

	for {
		row, err := i.childIter.Next()

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cache.Add(row); err != nil {
			return err
		}
	}

	rows := cache.Get()
	sorter := &Sorter{
		SortFields: i.s.SortFields,
		Rows:       rows,
		LastError:  nil,
		Ctx:        i.ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return sorter.LastError
	}
	i.sortedRows = rows
	return nil
}

type Sorter struct {
	SortFields []SortField
	Rows       []sql.Row
	LastError  error
	Ctx        *sql.Context
}

func (s *Sorter) Len() int {
	return len(s.Rows)
}

func (s *Sorter) Swap(i, j int) {
	s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
}

func (s *Sorter) Less(i, j int) bool {
	if s.LastError != nil {
		return false
	}

	a := s.Rows[i]
	b := s.Rows[j]
	for _, sf := range s.SortFields {
		typ := sf.Column.Type()
		av, err := sf.Column.Eval(s.Ctx, a)
		if err != nil {
			s.LastError = ErrUnableSort.Wrap(err)
			return false
		}

		bv, err := sf.Column.Eval(s.Ctx, b)
		if err != nil {
			s.LastError = ErrUnableSort.Wrap(err)
			return false
		}

		if sf.Order == Descending {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		} else if av == nil {
			return sf.NullOrdering == NullsFirst
		} else if bv == nil {
			return sf.NullOrdering != NullsFirst
		}

		cmp, err := typ.Compare(av, bv)
		if err != nil {
			s.LastError = err
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
