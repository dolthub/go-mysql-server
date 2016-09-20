package plan

import (
	"fmt"
	"io"
	"sort"

	"github.com/mvader/gitql/sql"
)

type Sort struct {
	fieldIndexes []int
	fieldTypes   []sql.Type
	child        sql.Node
}

type SortOrder byte

const (
	Ascending  SortOrder = 1
	Descending SortOrder = 2
)

type SortField struct {
	Column string
	Order  SortOrder
}

func NewSort(sortFields []SortField, child sql.Node) *Sort {
	indexes := []int{}
	types := []sql.Type{}
	childSchema := child.Schema()
	for _, sortField := range sortFields {
		found := false
		for idx, field := range childSchema {
			if field.Name == sortField.Column {
				indexes = append(indexes, idx)
				types = append(types, field.Type)
				found = true
				break
			}
		}
		if found == false {
			panic(fmt.Errorf("Field %s not found in child", sortField.Column))
		}
	}
	return &Sort{
		fieldIndexes: indexes,
		fieldTypes:   types,
		child:        child,
	}
}

func (s *Sort) Resolved() bool {
	return s.child.Resolved()
}

func (s *Sort) Children() []sql.Node {
	return []sql.Node{s.child}
}

func (s *Sort) Schema() sql.Schema {
	return s.child.Schema()
}

func (s *Sort) RowIter() (sql.RowIter, error) {
	i, err := s.child.RowIter()
	if err != nil {
		return nil, err
	}
	return newSortIter(s, i), nil
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
		println("computing sorted rows")
		err := i.computeSortedRows()
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}
	println("sorted rows: ", i.sortedRows)
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
		indexes: i.s.fieldIndexes,
		types:   i.s.fieldTypes,
		rows:    rows,
	})
	i.sortedRows = rows
	return nil
}

type sorter struct {
	indexes []int
	types   []sql.Type
	rows    []sql.Row
}

func (s *sorter) Len() int {
	return len(s.rows)
}

func (s *sorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func (s *sorter) Less(i, j int) bool {
	a := s.rows[i].Fields()
	b := s.rows[j].Fields()
	for i, idx := range s.indexes {
		typ := s.types[i]
		av := a[idx]
		bv := b[idx]
		if typ.Compare(av, bv) == -1 {
			return true
		}
	}
	return false
}
