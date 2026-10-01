// Copyright 2022 Dolthub, Inc.
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

package memo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJoinBaseCopiesCachedNullRejection(t *testing.T) {
	ctx := sql.NewEmptyContext()
	for name, nullable := range map[string]bool{"non-nullable": false, "nullable": true} {
		t.Run(name, func(t *testing.T) {
			filter := &countedJoinFilter{
				Expression: expression.NewEquals(
					expression.NewGetField(0, types.Int64, "left_key", nullable),
					expression.NewGetField(1, types.Int64, "right_key", false),
				),
			}
			join := newJoinBase(ctx, nil, nil, plan.JoinTypeLeftOuterExcludeNulls, []sql.Expression{filter})
			require.Equal(t, nullable, join.DropsNullRejection)
			require.Positive(t, filter.visits)
			visits := filter.visits

			// Physical alternatives copy the base; its filter must not be scanned again.
			for i := 0; i < 2; i++ {
				join = join.Copy()
				require.Equal(t, nullable, join.DropsNullRejection)
				require.Equal(t, visits, filter.visits)
			}
		})
	}
}

type countedJoinFilter struct {
	sql.Expression
	visits int
}

func (f *countedJoinFilter) Children() []sql.Expression {
	f.visits++
	return f.Expression.Children()
}
