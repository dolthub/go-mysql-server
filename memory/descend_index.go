package memory

import (
	"github.com/src-d/go-mysql-server/sql"
)

type DescendIndexLookup struct {
	id  string
	Gt  []interface{}
	Lte []interface{}
}

func (l DescendIndexLookup) ID() string { return l.id }
func (l DescendIndexLookup) GetUnions() []MergeableLookup { return nil }
func (l DescendIndexLookup) GetIntersections() []MergeableLookup { return nil }

func (DescendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func (l *DescendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *DescendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *DescendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	var unions []MergeableLookup
	unions = append(unions, l)
	for _, idx := range lookups {
		unions = append(unions, idx.(MergeableLookup))
	}

	return &MergedIndexLookup{
		Unions: unions,
	}
}

func (DescendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Difference is not implemented")
}

func (DescendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Intersection is not implemented")
}

