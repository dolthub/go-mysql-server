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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestTryIntersect(t *testing.T) {
	res, ok, err := sql.LessThanRangeColumnExpr(6, types.Int8).TryIntersect(sql.GreaterThanRangeColumnExpr(-1, types.Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, sql.RangeType_OpenOpen, res.Type())

	res, ok, err = sql.NotNullRangeColumnExpr(types.Int8).TryIntersect(sql.AllRangeColumnExpr(types.Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, sql.RangeType_GreaterThan, res.Type())
	assert.False(t, sql.RangeCutIsBinding(res.LowerBound))

	_, ok, err = sql.NotNullRangeColumnExpr(types.Int8).TryIntersect(sql.NullRangeColumnExpr(types.Int8))
	assert.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = sql.NullRangeColumnExpr(types.Int8).TryIntersect(sql.NotNullRangeColumnExpr(types.Int8))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestTryUnion(t *testing.T) {
	res, ok, err := sql.NotNullRangeColumnExpr(types.Int8).TryUnion(sql.NullRangeColumnExpr(types.Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, sql.RangeType_All, res.Type())
	res, ok, err = sql.NullRangeColumnExpr(types.Int8).TryUnion(sql.NotNullRangeColumnExpr(types.Int8))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, sql.RangeType_All, res.Type())
}
