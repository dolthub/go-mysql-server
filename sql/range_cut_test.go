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

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRangeCutCompare(t *testing.T) {
	type tc struct {
		left, right RangeCut
		res         int
	}
	for _, testcase := range []tc{
		{AboveAll{}, AboveNull{}, 1},
		{AboveAll{}, BelowNull{}, 1},
		{AboveAll{}, Above{1}, 1},
		{AboveAll{}, Below{1}, 1},
		{AboveAll{}, AboveAll{}, 0},

		{Above{1}, AboveNull{}, 1},
		{Above{1}, BelowNull{}, 1},
		{Above{1}, Above{1}, 0},
		{Above{1}, Below{1}, 1},
		{Above{2}, Above{1}, 1},

		{Below{1}, AboveNull{}, 1},
		{Below{1}, BelowNull{}, 1},
		{Below{1}, Below{1}, 0},
		{Below{2}, Below{1}, 1},

		{AboveNull{}, BelowNull{}, 1},

		{BelowNull{}, BelowNull{}, 0},
	} {
		t.Run(fmt.Sprintf("%s/%s = %d", testcase.left.String(), testcase.right.String(), testcase.res), func(t *testing.T) {
			res, err := testcase.left.Compare(testcase.right, Int8)
			assert.NoError(t, err)
			assert.Equal(t, testcase.res, res, "forward Compare")

			res, err = testcase.right.Compare(testcase.left, Int8)
			assert.NoError(t, err)
			assert.Equal(t, -testcase.res, res, "reverse Compare")
		})
	}
}
