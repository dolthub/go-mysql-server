package testutil

import (
	"github.com/src-d/go-mysql-server/sql"
)

type DescendIndexLookup struct {
	id  string
	Gt  []interface{}
	Lte []interface{}
}

func (DescendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("descendIndexLookup.Values is a placeholder")
}

func (l *DescendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *DescendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *DescendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &MergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (DescendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Difference is not implemented")
}

func (DescendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Intersection is not implemented")
}

