// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type NegateIndexLookup struct {
	Lookup MergeableLookup
	Index  ExpressionsIndex
}

var _ memoryIndexLookup = (*NegateIndexLookup)(nil)
var _ sql.IndexLookup = (*NegateIndexLookup)(nil)

func (l *NegateIndexLookup) ID() string     { return "not " + l.Lookup.ID() }
func (l *NegateIndexLookup) String() string { return "not " + l.Lookup.ID() }

func (l *NegateIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *NegateIndexLookup) EvalExpression() sql.Expression {
	return expression.NewNot(l.Lookup.(memoryIndexLookup).EvalExpression())
}

func (l *NegateIndexLookup) Indexes() []string {
	return []string{l.ID()}
}

func (*NegateIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (l *NegateIndexLookup) Union(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return union(l.Index, l, lookups...), nil
}

func (l *NegateIndexLookup) Intersection(indexes ...sql.IndexLookup) (sql.IndexLookup, error) {
	return intersection(l.Index, l, indexes...), nil
}
