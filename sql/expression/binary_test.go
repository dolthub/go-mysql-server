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

package expression

import (
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestBinary(t *testing.T) {
	require := require.New(t)

	// Validate Binary is reflexive
	e := NewBinary(NewGetField(0, sql.Text, "foo", true))
	require.Equal(eval(t, e, sql.NewRow("hi")), eval(t, e, sql.NewRow("hi")))

	// Go through assorted test cases
	testCases := []struct {
		val      interface{}
		valType  sql.Type
		expected string
	}{
		{"hi", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), "hi"},
		{int8(1), sql.Int8, "1"},
		{true, sql.Boolean, "true"},
		{"hello", sql.LongText, "hello"},
	}

	for _, tt := range testCases {
		f := NewBinary(NewLiteral(tt.val, tt.valType))
		require.Equal(tt.expected, eval(t, f, sql.Row{nil}))
	}

	// Try with nil case
	e = NewBinary(NewLiteral(nil, sql.Null))
	require.Equal(nil, eval(t, e, sql.Row{nil}))
}
