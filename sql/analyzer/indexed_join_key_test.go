// Copyright 2026 Dolthub, Inc.
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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestKeyForExprNullSafeEquals(t *testing.T) {
	ctx := sql.NewEmptyContext()
	indexedCol := expression.NewGetFieldWithTable(1, 1, types.Int64, "db", "t", "c", true)
	otherIndexedCol := expression.NewGetFieldWithTable(2, 1, types.Int64, "db", "t", "d", true)
	outerCol := expression.NewGetFieldWithTable(3, 2, types.Int64, "db", "a", "c", true)
	regularEquality := expression.NewEquals(indexedCol, outerCol)
	reversedEquality := expression.NewEquals(outerCol, indexedCol)
	nullSafeEquality := expression.NewNullSafeEquals(indexedCol, outerCol)
	selfEquality := expression.NewNullSafeEquals(indexedCol, indexedCol)
	unrelatedEquality := expression.NewNullSafeEquals(otherIndexedCol, outerCol)
	lookupTableEquality := expression.NewNullSafeEquals(indexedCol, otherIndexedCol)

	for _, tt := range []struct {
		name       string
		filters    []sql.Expression
		filter     sql.Expression
		matchesNil bool
	}{
		{
			name:    "regular equality",
			filters: []sql.Expression{regularEquality},
			filter:  regularEquality,
		},
		{
			name:    "reversed regular equality",
			filters: []sql.Expression{reversedEquality},
			filter:  reversedEquality,
		},
		{
			name:       "null safe equality",
			filters:    []sql.Expression{nullSafeEquality},
			filter:     nullSafeEquality,
			matchesNil: true,
		},
		{
			name:    "null safe equality misses indexed column",
			filters: []sql.Expression{unrelatedEquality, regularEquality},
			filter:  regularEquality,
		},
		{
			name:    "null safe self equality before regular equality",
			filters: []sql.Expression{selfEquality, regularEquality},
			filter:  regularEquality,
		},
		{
			name:    "null safe key references lookup table",
			filters: []sql.Expression{lookupTableEquality, reversedEquality},
			filter:  reversedEquality,
		},
		{
			name:       "rejected equality before accepted null safe equality",
			filters:    []sql.Expression{selfEquality, nullSafeEquality},
			filter:     nullSafeEquality,
			matchesNil: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key, filter, matchesNil := keyForExpr(ctx, indexedCol.Id(), indexedCol.TableId(), tt.filters, nil)
			require.Same(t, outerCol, key)
			require.Same(t, tt.filter, filter)
			require.Equal(t, tt.matchesNil, matchesNil)
		})
	}
}
