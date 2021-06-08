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
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestResolveGenerators(t *testing.T) {
	testCases := []struct {
		name     string
		node     sql.Node
		expected sql.Node
		err      *errors.Kind
	}{
		{
			name: "regular explode",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(sql.NewEmptyContext(), expression.NewGetField(1, sql.CreateArray(sql.Int64), "b", false)),
					expression.NewGetField(2, sql.Int64, "c", false),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: plan.NewGenerate(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Int64, "a", false),
						function.NewGenerate(sql.NewEmptyContext(), expression.NewGetField(1, sql.CreateArray(sql.Int64), "b", false)),
						expression.NewGetField(2, sql.Int64, "c", false),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
				expression.NewGetField(1, sql.CreateArray(sql.Int64), "EXPLODE(b)", false),
			),
			err: nil,
		},
		{
			name: "explode with alias",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					expression.NewAlias("x", function.NewExplode(
						sql.NewEmptyContext(),
						expression.NewGetField(1, sql.CreateArray(sql.Int64), "b", false),
					)),
					expression.NewGetField(2, sql.Int64, "c", false),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: plan.NewGenerate(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Int64, "a", false),
						expression.NewAlias("x", function.NewGenerate(
							sql.NewEmptyContext(),
							expression.NewGetField(1, sql.CreateArray(sql.Int64), "b", false),
						)),
						expression.NewGetField(2, sql.Int64, "c", false),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
				expression.NewGetField(1, sql.CreateArray(sql.Int64), "x", false),
			),
			err: nil,
		},
		{
			name: "non array type on explode",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(sql.NewEmptyContext(), expression.NewGetField(1, sql.Int64, "b", false)),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: nil,
			err:      errExplodeNotArray,
		},
		{
			name: "more than one generator",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(sql.NewEmptyContext(), expression.NewGetField(1, sql.CreateArray(sql.Int64), "b", false)),
					function.NewExplode(sql.NewEmptyContext(), expression.NewGetField(2, sql.CreateArray(sql.Int64), "c", false)),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: nil,
			err:      errMultipleGenerators,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := resolveGenerators(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
