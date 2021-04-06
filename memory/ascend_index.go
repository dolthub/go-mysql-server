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

type AscendIndexLookup struct {
	id    string
	Gte   []interface{}
	Lt    []interface{}
	Index ExpressionsIndex
}

var _ memoryIndexLookup = (*AscendIndexLookup)(nil)

func (l *AscendIndexLookup) ID() string     { return l.id }
func (l *AscendIndexLookup) String() string { return l.id }

func (l *AscendIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *AscendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *AscendIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (l *AscendIndexLookup) Union(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return union(l.Index, l, lookups...), nil
}

func (l *AscendIndexLookup) EvalExpression() sql.Expression {
	var columnExprs []sql.Expression
	for i, indexExpr := range l.Index.ColumnExpressions() {
		var ltExpr, gtExpr sql.Expression
		hasLt := len(l.Lt) > 0
		hasGte := len(l.Gte) > 0

		if hasLt {
			lt, typ := getType(l.Lt[i])
			ltExpr = expression.NewLessThan(indexExpr, expression.NewLiteral(lt, typ))
		}
		if hasGte {
			gte, typ := getType(l.Gte[i])
			gtExpr = expression.NewGreaterThanOrEqual(indexExpr, expression.NewLiteral(gte, typ))
		}

		switch {
		case hasLt && hasGte:
			columnExprs = append(columnExprs, ltExpr, gtExpr)
		case hasLt:
			columnExprs = append(columnExprs, ltExpr)
		case hasGte:
			columnExprs = append(columnExprs, gtExpr)
		default:
			panic("Either Lt or Gte must be set")
		}
	}

	return and(columnExprs...)
}

func (l *AscendIndexLookup) Intersection(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return intersection(l.Index, l, lookups...), nil
}
