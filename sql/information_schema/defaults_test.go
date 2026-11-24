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

package information_schema

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestLiteralDefaultsMatchParser checks that the literal column defaults of
// the schemas equal the defaults the parser builds from their SQL text.
func TestLiteralDefaultsMatchParser(t *testing.T) {
	ctx := sql.NewEmptyContext()
	varchar := types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default)
	cases := []struct {
		sql  string
		typ  sql.Type
		want *sql.ColumnDefaultValue
	}{
		{"0", types.Uint64, zeroDefault(types.Uint64)},
		{"0", types.Int32, zeroDefault(types.Int32)},
		{"0", types.Float32, zeroDefault(types.Float32)},
		{"0", types.Datetime, zeroDefault(types.Datetime)},
		{`""`, types.LongText, stringDefault("", types.LongText)},
		{`"SQL"`, varchar, stringDefault("SQL", varchar)},
		{`"0000-00-00 00:00:00"`, types.Datetime, stringDefault("0000-00-00 00:00:00", types.Datetime)},
	}
	for _, c := range cases {
		parsed := planbuilder.MustStringToColumnDefaultValue(ctx, c.sql, c.typ, false)
		require.Equal(t, c.want, parsed, "default %s of %s", c.sql, c.typ)
	}
}
