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

package sql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestRangeCutCompare(t *testing.T) {
	type tc struct {
		left, right *sql.Bound
		res         int
	}
	ctx := sql.NewEmptyContext()
	for _, testcase := range []tc{
		{sql.NewAboveAllBound(), sql.NewBelowNullBound(), 1},
		{sql.NewAboveAllBound(), sql.NewAboveNullBound(), 1},
		{sql.NewAboveAllBound(), sql.NewBound(1, sql.Above), 1},
		{sql.NewAboveAllBound(), sql.NewBound(1, sql.Below), 1},
		{sql.NewAboveAllBound(), sql.NewAboveAllBound(), 0},

		{sql.NewBound(1, sql.Above), sql.NewAboveNullBound(), 1},
		{sql.NewBound(1, sql.Above), sql.NewBelowNullBound(), 1},
		{sql.NewBound(1, sql.Above), sql.NewBound(1, sql.Above), 0},
		{sql.NewBound(1, sql.Above), sql.NewBound(1, sql.Below), 1},
		{sql.NewBound(2, sql.Above), sql.NewBound(1, sql.Above), 1},

		{sql.NewBound(1, sql.Below), sql.NewAboveNullBound(), 1},
		{sql.NewBound(1, sql.Below), sql.NewBelowNullBound(), 1},
		{sql.NewBound(1, sql.Below), sql.NewBound(1, sql.Below), 0},
		{sql.NewBound(2, sql.Below), sql.NewBound(1, sql.Below), 1},

		{sql.NewAboveNullBound(), sql.NewBelowNullBound(), 1},

		{sql.NewBelowNullBound(), sql.NewBelowNullBound(), 0},
	} {
		t.Run(fmt.Sprintf("%s/%s = %d", testcase.left.String(), testcase.right.String(), testcase.res), func(t *testing.T) {
			res, err := testcase.left.Compare(ctx, testcase.right, types.Int8)
			assert.NoError(t, err)
			assert.Equal(t, testcase.res, res, "forward Compare")

			res, err = testcase.right.Compare(ctx, testcase.left, types.Int8)
			assert.NoError(t, err)
			assert.Equal(t, -testcase.res, res, "reverse Compare")
		})
	}
}
