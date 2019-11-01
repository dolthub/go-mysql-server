package memory

import (
	"github.com/src-d/go-mysql-server/sql"
)

type NegateIndexLookup struct {
	Lookup         *MergeableIndexLookup
	intersections []string
	unions        []string
}

func (l *NegateIndexLookup) ID() string              { return "not " + l.Lookup.ID() }
func (l *NegateIndexLookup) GetUnions() []string        { return l.unions }
func (l *NegateIndexLookup) GetIntersections() []string { return l.intersections }

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
	return &MergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (*NegateIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("negateIndexLookup.Difference is not implemented")
}

func (l *NegateIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		intersections = append(intersections, idx.(MergeableLookup).ID())
		intersections = append(intersections, idx.(MergeableLookup).GetIntersections()...)
		unions = append(unions, idx.(MergeableLookup).GetUnions()...)
	}
	return &MergeableIndexLookup{
		Index:    l.Lookup.Index,
		Unions:  append(l.unions, unions...),
		Intersections: append(l.intersections, intersections...),
	}
}