package plan

import (
	"io"
	"sort"

	"github.com/gitql/gitql/sql"
)

type Sort struct {
	UnaryNode
	sortFields []SortField
}

type SortOrder byte

const (
	Ascending  SortOrder = 1
	Descending SortOrder = 2
)

type SortField struct {
	Column sql.Expression
	Order  SortOrder
}

func NewSort(sortFields []SortField, child sql.Node) *Sort {
	return &Sort{
		UnaryNode:  UnaryNode{child},
		sortFields: sortFields,
	}
}

func (s *Sort) Resolved() bool {
	return s.UnaryNode.Child.Resolved() && s.expressionsResolved()
}

func (p *Sort) expressionsResolved() bool {
	for _, f := range p.sortFields {
		if !f.Column.Resolved() {
			return false
		}
	}
	return true
}

func (s *Sort) RowIter() (sql.RowIter, error) {

	i, err := s.UnaryNode.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return newSortIter(s, i), nil
}

func (s *Sort) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := s.UnaryNode.Child.TransformUp(f)
	n := NewSort(s.sortFields, c)

	return f(n)
}

func (s *Sort) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := s.UnaryNode.Child.TransformExpressionsUp(f)
	sfs := []SortField{}
	for _, sf := range s.sortFields {
		sfs = append(sfs, SortField{sf.Column.TransformUp(f), sf.Order})
	}
	n := NewSort(sfs, c)

	return n
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

func (i *sortIter) computeSortedRows() error {
	rows := []sql.Row{}
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
	sort.Sort(&sorter{
		sortFields: i.s.sortFields,
		rows:       rows,
	})
	i.sortedRows = rows
	return nil
}

type sorter struct {
	sortFields []SortField
	rows       []sql.Row
}

func (s *sorter) Len() int {
	return len(s.rows)
}

func (s *sorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func (s *sorter) Less(i, j int) bool {
	a := s.rows[i]
	b := s.rows[j]
	for _, sf := range s.sortFields {
		typ := sf.Column.Type()
		av := sf.Column.Eval(a)
		bv := sf.Column.Eval(b)

		if sf.Order == Descending {
			av, bv = bv, av
		}

		switch typ.Compare(av, bv) {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return false
}
