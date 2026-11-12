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

package planbuilder

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestResolveColumnDefaultExpression(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12340
	tests := []struct {
		name      string
		generated string
	}{
		{
			name:      "quoted timestampdiff unit saved by earlier versions",
			generated: "(TIMESTAMPDIFF('MICROSECOND', `start_at`, `end_at`))",
		},
		{
			name:      "bare timestampdiff unit",
			generated: "(TIMESTAMPDIFF(MICROSECOND, `start_at`, `end_at`))",
		},
		{
			name:      "quoted unit with lowercase function name",
			generated: "(timestampdiff('MICROSECOND', `start_at`, `end_at`))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newGeneratedColumnBuilder(tt.generated)
			defer b.Reset()

			query := "show create table gen"
			stmt, err := sqlparser.Parse(query)
			require.NoError(t, err)

			_, _, err = b.BindOnly(stmt, query, nil)
			require.NoError(t, err)
		})
	}

	t.Run("unrelated syntax error still fails", func(t *testing.T) {
		b := newGeneratedColumnBuilder("(a +)")
		defer b.Reset()

		query := "show create table gen"
		stmt, err := sqlparser.Parse(query)
		require.NoError(t, err)

		_, _, err = b.BindOnly(stmt, query, nil)
		require.Error(t, err)
	})
}

// newGeneratedColumnBuilder returns a builder over a catalog with table gen,
// whose elapsed column stores |generated| as its text.
func newGeneratedColumnBuilder(generated string) *Builder {
	db := memory.NewDatabase("mydb")
	cat := newGeneratedColumnCatalog(db, generated)
	pro := memory.NewDBProvider(db)
	vars := variables.NewEngineVariables()
	sess := memory.NewSession(sql.NewBaseSessionWithVariables(vars), pro)

	ctx := sql.NewContextWithVariables(context.Background(), vars, sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")
	return New(ctx, cat, nil)
}

// newGeneratedColumnCatalog returns a catalog with table gen, whose elapsed
// column stores |generated| as its text, as the engine saves it.
func newGeneratedColumnCatalog(db *memory.Database, generated string) *sql.MapCatalog {
	cat := newTestCatalog(db)
	ctx := sql.NewEmptyContext()
	gen := memory.NewTable(ctx, db, "gen", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64},
		{Name: "start_at", Type: types.DatetimeMaxPrecision},
		{Name: "end_at", Type: types.DatetimeMaxPrecision},
		{Name: "elapsed", Type: types.Int64, Generated: sql.NewUnresolvedColumnDefaultValue(generated)},
	}), nil)
	cat.Tables["gen"] = gen
	db.AddTable("gen", gen)
	return cat
}
