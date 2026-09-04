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

package sql

import (
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
)

func TestColumnDefaultValueString(t *testing.T) {
	var implicitDefault *ColumnDefaultValue
	require.Equal(t, "", implicitDefault.String())

	explicitNull, err := NewColumnDefaultValue(UnresolvedColumnDefault{ExprString: "NULL"}, nil, true, false, true)
	require.NoError(t, err)
	require.Equal(t, "NULL", explicitNull.String())
	statement, err := sqlparser.Parse("SELECT " + explicitNull.String())
	require.NoError(t, err)
	parsed := statement.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	require.IsType(t, &sqlparser.NullVal{}, parsed)
	require.Equal(t, "NULL", explicitNull.Expr.(UnresolvedColumnDefault).ExprString)
}
