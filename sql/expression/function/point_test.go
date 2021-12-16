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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestPoint(t *testing.T) {
	testCases := []struct {
		ex1      interface{}
		ex2      interface{}
		expected interface{}
	}{
		{1, 2, sql.PointValue{X: 1, Y: 2}},
		{nil, 2, nil},
		{1, nil, nil},
		{nil, nil, nil},
	}

	f := NewPoint(
		expression.NewGetField(0, sql.LongText, "ex1", true),
		expression.NewGetField(1, sql.LongText, "ex2", true),
	)
	require.Equal(t, sql.PointValue{}, f.Type())

	var3 := sql.PointValue{}
	f = NewPoint(
		expression.NewGetField(0, var3, "ex1", true),
		expression.NewGetField(1, var3, "ex2", true),
	)
	require.Equal(t, var3, f.Type())

	for _, tc := range testCases {
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.ex1, tc.ex2))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
