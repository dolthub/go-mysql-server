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

package analyzer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestAddRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostAnalyzeRule(-1, generateIndexScans).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPreValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPreValidationRule(-1, generateIndexScans).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPostValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostValidationRule(-1, generateIndexScans).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestRemoveOnceBeforeRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceBeforeRule(applyDefaultSelectLimitId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveDefaultRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveDefaultRule(resolveSubqueriesId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveOnceAfterRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceAfterRule(loadTriggersId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveValidationRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveValidationRule(validateResolvedId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveAfterAllRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveAfterAllRule(TrackProcessId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func countRules(batches []*Batch) int {
	var count int
	for _, b := range batches {
		count = count + len(b.Rules)
	}
	return count

}

func TestDeepCopyNode(t *testing.T) {
	tests := []struct {
		node sql.Node
		exp  sql.Node
	}{
		{
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewLiteral(1, types.Int64),
				},
				plan.NewNaturalJoin(
					plan.NewInnerJoin(
						plan.NewUnresolvedTable("mytable", ""),
						plan.NewUnresolvedTable("mytable2", ""),
						expression.NewEquals(
							expression.NewUnresolvedQualifiedColumn("mytable", "i"),
							expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewBindVar("v1"),
							expression.NewBindVar("v2"),
						),
						plan.NewUnresolvedTable("mytable3", ""),
					),
				),
			),
		},
		{
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewLiteral(1, types.Int64),
				},
				plan.NewSetOp(
					plan.UnionType,
					plan.NewProject(
						[]sql.Expression{
							expression.NewLiteral(1, types.Int64),
						},
						plan.NewUnresolvedTable("mytable", ""),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewBindVar("v1"),
							expression.NewBindVar("v2"),
						},
						plan.NewUnresolvedTable("mytable", ""),
					),
					false, nil, nil, nil),
			),
		},
		{
			node: plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(1, types.Int64),
				),
				plan.NewWindow(
					[]sql.Expression{
						aggregation.NewSum(
							expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false),
						),
						expression.NewGetFieldWithTable(1, types.Int64, "db", "a", "x", false),
						expression.NewBindVar("v1"),
					},
					plan.NewProject(
						[]sql.Expression{
							expression.NewBindVar("v2"),
						},
						plan.NewUnresolvedTable("x", ""),
					),
				),
			),
		},
		{
			node: plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(1, types.Int64),
				),
				plan.NewSubqueryAlias("cte1", "select x from a",
					plan.NewProject(
						[]sql.Expression{
							expression.NewBindVar("v1"),
							expression.NewUnresolvedColumn("v2"),
						},
						plan.NewUnresolvedTable("a", ""),
					),
				),
			),
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("DeepCopyTest_%d", i), func(t *testing.T) {
			cop, err := DeepCopyNode(tt.node)
			require.NoError(t, err)
			cop, _, err = plan.ApplyBindings(cop, map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int64),
				"v2": expression.NewLiteral("x", types.Text),
			})
			require.NoError(t, err)
			require.NotEqual(t, cop, tt.node)
		})
	}
}
