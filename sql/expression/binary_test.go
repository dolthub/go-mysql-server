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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBinary(t *testing.T) {
	require := require.New(t)

	e := NewBinary(NewGetField(0, sql.Text, "foo", true))

	// Validate Binary is reflexive
	require.Equal(eval(t, e, sql.NewRow("hi")), eval(t, e, sql.NewRow("hi")))


	x := NewBinary(NewLiteral([]byte("hi"), sql.MustCreateBinary(query.Type_VARBINARY, int64(16))))
	require.Equal([]byte("hi"), eval(t, x, sql.Row{nil}))

	// Validate Binary is not equal for two terms that look equal but are the same in a collation
}
