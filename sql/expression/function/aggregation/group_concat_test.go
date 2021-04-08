// Copyright 2021 Dolthub, Inc.
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

package aggregation

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGroupConcat_FunctionName(t *testing.T) {
	assert := require.New(t)

	m, err := NewGroupConcat("field", nil, ",", nil)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field)", m.String())

	m, err = NewGroupConcat("field", nil, "-", nil)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field separator '-')", m.String())

	sf := sql.SortFields{
		{expression.NewUnresolvedColumn("field"), sql.Ascending, 0},
		{expression.NewUnresolvedColumn("field2"), sql.Descending, 0},
	}

	m, err = NewGroupConcat("field", sf, "-", nil)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field order by field ASC, field2 DESC separator '-')", m.String())
}

//func TestGroupConcat_PastMaxLen(t *testing.T) {
//	var rows []sql.Row
//
//	for i := 0; i < 1050; i ++ {
//		rows = append(rows, sql.Row{i})
//	}
//
//	gc, err := NewGroupConcat("", nil, ",", nil)
//	buf := gc.NewBuffer()
//	for _, row := range rows {
//		require.NoError(t, gc.Update(sql.NewEmptyContext(), buf, row))
//	}
//
//	result, err := gc.Eval(sql.NewEmptyContext(), buf)
//	rs := result.(string)
//	require.NoError(t, err)
//	require.Equal(t,1024, len(rs))
//}