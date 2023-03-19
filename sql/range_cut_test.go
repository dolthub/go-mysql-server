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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestRangeCutCompare(t *testing.T) {
	type tc struct {
		left, right sql.RangeCut
		res         int
	}
	for _, testcase := range []tc{
		{sql.AboveAll{}, sql.BelowNull{}, 1},
		{sql.AboveAll{}, sql.AboveNull{}, 1},
		{sql.AboveAll{}, sql.Above{1}, 1},
		{sql.AboveAll{}, sql.Below{1}, 1},
		{sql.AboveAll{}, sql.AboveAll{}, 0},

		{sql.Above{1}, sql.AboveNull{}, 1},
		{sql.Above{1}, sql.BelowNull{}, 1},
		{sql.Above{1}, sql.Above{1}, 0},
		{sql.Above{1}, sql.Below{1}, 1},
		{sql.Above{2}, sql.Above{1}, 1},

		{sql.Below{1}, sql.AboveNull{}, 1},
		{sql.Below{1}, sql.BelowNull{}, 1},
		{sql.Below{1}, sql.Below{1}, 0},
		{sql.Below{2}, sql.Below{1}, 1},

		{sql.AboveNull{}, sql.BelowNull{}, 1},

		{sql.BelowNull{}, sql.BelowNull{}, 0},
	} {
		t.Run(fmt.Sprintf("%s/%s = %d", testcase.left.String(), testcase.right.String(), testcase.res), func(t *testing.T) {
			res, err := testcase.left.Compare(testcase.right, types.Int8)
			assert.NoError(t, err)
			assert.Equal(t, testcase.res, res, "forward Compare")

			res, err = testcase.right.Compare(testcase.left, types.Int8)
			assert.NoError(t, err)
			assert.Equal(t, -testcase.res, res, "reverse Compare")
		})
	}
}
