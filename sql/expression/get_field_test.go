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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGetFieldString(t *testing.T) {
	tests := []struct {
		expr     *GetField
		expected string
	}{
		{NewGetField(0, types.Int64, "normal_name", false), "normal_name"},
		{NewGetFieldWithTable(0, 0, types.Int64, "", "table_name", "column_name", false), "table_name.column_name"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		exprtest.AssertColumnRoundTrip(t, test.expr)
	}
}

func TestGetFieldDescribeUsesSeparateDebugName(t *testing.T) {
	expr := NewGetField(2, types.Int64, "sum(x) over ()", false).WithDebugName("sum\n └─ x\n")

	require.Equal(t, "sum(x) over ()", expr.String())
	require.Equal(t, "sum(x) over ()", sql.Describe(nil, expr, sql.DescribeOptions{Estimates: true}))
	require.Equal(t, "sum\n └─ x\n:2!null", sql.Describe(nil, expr, sql.DescribeOptions{Debug: true}))
}
