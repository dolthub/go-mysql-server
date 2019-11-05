package memory

import (
	"github.com/src-d/go-mysql-server/sql"
)

type NegateIndexLookup struct {
	Lookup MergeableLookup
	Index ExpressionsIndex
}

func (l *NegateIndexLookup) ID() string              { return "not " + l.Lookup.ID() }

func (*NegateIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func (l *NegateIndexLookup) Indexes() []string {
	return []string{l.ID()}
}

func (*NegateIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *NegateIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(l.Index, l, lookups...)
}

func (*NegateIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("negateIndexLookup.Difference is not implemented")
}

func (l *NegateIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	return intersection(l.Index, l, indexes...)
}