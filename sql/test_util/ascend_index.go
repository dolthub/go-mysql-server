package testutil

import (
	"github.com/src-d/go-mysql-server/sql"
)

type AscendIndexLookup struct {
	id  string
	Gte []interface{}
	Lt  []interface{}
}

func (AscendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func (l *AscendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *AscendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *AscendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &MergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (AscendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Difference is not implemented")
}

func (AscendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Intersection is not implemented")
}
