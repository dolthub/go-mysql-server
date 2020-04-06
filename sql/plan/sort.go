package plan

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
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
func (s *Sort) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := s.UnaryNode.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(ctx, s, i)), nil
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
	childIter  sql.RowIter
	sortedRows []sql.Row
	idx        int
}

func newSortIter(ctx *sql.Context, s *Sort, child sql.RowIter) *sortIter {
	return &sortIter{
		ctx:       ctx,
		s:         s,
		childIter: child,
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
	sorter := &sorter{
		sortFields: i.s.SortFields,
		rows:       rows,
		lastError:  nil,
		ctx: i.ctx,
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
