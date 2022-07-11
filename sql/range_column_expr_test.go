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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryIntersect(t *testing.T) {
	res, ok, err := LessThanRangeColumnExpr(6, Int8).TryIntersect(GreaterThanRangeColumnExpr(-1, Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, RangeType_OpenOpen, res.Type())

	res, ok, err = NotNullRangeColumnExpr(Int8).TryIntersect(AllRangeColumnExpr(Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, RangeType_GreaterThan, res.Type())
	assert.False(t, RangeCutIsBinding(res.LowerBound))

	_, ok, err = NotNullRangeColumnExpr(Int8).TryIntersect(NullRangeColumnExpr(Int8))
	assert.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = NullRangeColumnExpr(Int8).TryIntersect(NotNullRangeColumnExpr(Int8))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestTryUnion(t *testing.T) {
	res, ok, err := NotNullRangeColumnExpr(Int8).TryUnion(NullRangeColumnExpr(Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, RangeType_All, res.Type())
	res, ok, err = NullRangeColumnExpr(Int8).TryUnion(NotNullRangeColumnExpr(Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, RangeType_All, res.Type())
}
