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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestMergeUnionSchemas(t *testing.T) {
	testCases := []struct {
		name string
		in   sql.Node
		out  sql.Node
		err  error
	}{
		{
			"Unresolved is unchanged",
			plan.NewUnion(
				plan.NewUnresolvedTable("mytable", ""),
				plan.NewUnresolvedTable("mytable", ""),
			),
			plan.NewUnion(
				plan.NewUnresolvedTable("mytable", ""),
				plan.NewUnresolvedTable("mytable", ""),
			),
			nil,
		},
		{
			"Matching Schemas is Unchanged",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(3), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
			),
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(3), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
			),
			nil,
		},
		{
			"Mismatched Lengths is error",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewLiteral(int64(3), sql.Int64),
						expression.NewLiteral(int64(6), sql.Int64),
					},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
			),
			nil,
			errors.New("this is an error"),
		},
		{
			"Mismatched Types Coerced to Strings",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int32(3), sql.Int32)},
					plan.NewResolvedTable(dualTable, nil, nil),
				),
			),
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("1", expression.NewConvert(
							expression.NewGetField(0, sql.Int64, "1", false), "char")),
					},
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
						plan.NewResolvedTable(dualTable, nil, nil),
					),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("3", expression.NewConvert(
							expression.NewGetField(0, sql.Int32, "3", false), "char")),
					},
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int32(3), sql.Int32)},
						plan.NewResolvedTable(dualTable, nil, nil),
					),
				),
			),
			nil,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			out, _, err := mergeUnionSchemas(sql.NewEmptyContext(), nil, c.in, nil, DefaultRuleSelector)
			if c.err == nil {
				require.NoError(err)
				require.NotNil(out)
				require.Equal(c.out, out)
			} else {
				require.Error(err)
			}
		})
	}
}
