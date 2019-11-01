package memory

import (
	"github.com/src-d/go-mysql-server/sql"
)

type NegateIndexLookup struct {
	Lookup         MergeableLookup
}

func (l *NegateIndexLookup) ID() string              { return "not " + l.Lookup.ID() }
func (l *NegateIndexLookup) GetUnions() []MergeableLookup        { return l.Lookup.GetUnions() }
func (l *NegateIndexLookup) GetIntersections() []MergeableLookup { return l.Lookup.GetIntersections() }

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
	var unions []MergeableLookup
	unions = append(unions, l)
	for _, idx := range lookups {
		unions = append(unions, idx.(MergeableLookup))
	}

	return &MergedIndexLookup{
		Union: unions,
	}
}

func (*NegateIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("negateIndexLookup.Difference is not implemented")
}

func (l *NegateIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections []MergeableLookup
	intersections = append(intersections, l)
	for _, idx := range indexes {
		intersections = append(intersections, idx.(MergeableLookup))
	}

	return &MergedIndexLookup{
		Intersection: intersections,
	}
}