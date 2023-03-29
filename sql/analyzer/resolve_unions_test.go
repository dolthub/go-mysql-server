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
	"github.com/dolthub/go-mysql-server/sql/types"
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
			plan.NewUnion(plan.NewUnresolvedTable("mytable", ""), plan.NewUnresolvedTable("mytable", ""), false, nil, nil),
			plan.NewUnion(plan.NewUnresolvedTable("mytable", ""), plan.NewUnresolvedTable("mytable", ""), false, nil, nil),
			nil,
		},
		{
			"Matching Schemas is Unchanged",
			plan.NewUnion(plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(1), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(3), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), false, nil, nil),
			plan.NewUnion(plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(1), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(3), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), false, nil, nil),
			nil,
		},
		{
			"Mismatched Lengths is error",
			plan.NewUnion(plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(1), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), plan.NewProject(
				[]sql.Expression{
					expression.NewLiteral(int64(3), types.Int64),
					expression.NewLiteral(int64(6), types.Int64),
				},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), false, nil, nil),
			nil,
			errors.New("this is an error"),
		},
		{
			"Mismatched Types Coerced to Strings",
			plan.NewUnion(plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int64(1), types.Int64)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int32(3), types.Int32)},
				plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
			), false, nil, nil),
			plan.NewUnion(plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("1", expression.NewConvert(
						expression.NewGetField(0, types.Int64, "1", false), "signed")),
				},
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), types.Int64)},
					plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
				),
			), plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("3", expression.NewConvert(
						expression.NewGetField(0, types.Int32, "3", false), "signed")),
				},
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int32(3), types.Int32)},
					plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
				),
			), false, nil, nil),
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
