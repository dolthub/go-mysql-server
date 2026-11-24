// Copyright 2025 Dolthub, Inc.
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

package hash

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func BenchmarkHashOf(b *testing.B) {
	ctx := sql.NewEmptyContext()
	row := sql.NewRow(1, "1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum, err := HashOf(ctx, nil, row)
		if err != nil {
			b.Fatal(err)
		}
		if sum != 11268758894040352165 {
			b.Fatalf("got %v", sum)
		}
	}
}

func BenchmarkParallelHashOf(b *testing.B) {
	ctx := sql.NewEmptyContext()
	row := sql.NewRow(1, "1")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sum, err := HashOf(ctx, nil, row)
			if err != nil {
				b.Fatal(err)
			}
			if sum != 11268758894040352165 {
				b.Fatalf("got %v", sum)
			}
		}
	})
}

func TestHashOfDecimals(t *testing.T) {
	tests := []struct {
		left  string
		right string
		equal bool
	}{
		{"-0", "0", true},
		{"1", "1.0", true},
		{"1.0", "1.00", true},
		{"2.50", "2.5", true},
		{"10", "10.000", true},
		{"0", "-0.0", true},
		{"0.00", "0", true},
		{"-1.50", "-1.5", true},
		{"10", "1", false},
		{"1.01", "1.1", false},
		{"100", "1.00", false},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		t.Run(test.left+" "+test.right, func(t *testing.T) {
			left, _, err := apd.NewFromString(test.left)
			require.NoError(t, err)
			right, _, err := apd.NewFromString(test.right)
			require.NoError(t, err)

			leftHash, err := HashOf(ctx, nil, sql.NewRow(left))
			require.NoError(t, err)
			rightHash, err := HashOf(ctx, nil, sql.NewRow(right))
			require.NoError(t, err)
			require.Equal(t, test.equal, leftHash == rightHash)
		})
	}
}

func TestHashOfNegativeZero(t *testing.T) {
	dec, _, _ := apd.NewFromString("0.000")
	dec.Negative = true
	require.Equal(t, dec.Text('f'), "-0.000")
	fmt.Printf("%s\n", dec.Text('f'))
	res := new(apd.Decimal)
	res, _ = res.Reduce(dec)
	require.Equal(t, res.Text('f'), "0")
}

func TestHashOfStringCollations(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12503
	ctx := sql.NewEmptyContext()
	aiCiType := types.MustCreateString(sqltypes.Text, 100, sql.Collation_utf8mb4_0900_ai_ci)
	generalCiType := types.MustCreateString(sqltypes.Text, 100, sql.Collation_utf8mb4_general_ci)
	charAiCiType := types.MustCreateString(sqltypes.Char, 100, sql.Collation_utf8mb4_0900_ai_ci)
	binType := types.MustCreateString(sqltypes.Text, 100, sql.Collation_utf8mb4_0900_bin)
	varBinaryType := types.MustCreateBinary(sqltypes.VarBinary, 100)

	tests := []struct {
		name  string
		sch   sql.Schema
		left  sql.Row
		right sql.Row
		equal bool
	}{
		{
			name:  "utf8mb4_0900_ai_ci case insensitive",
			sch:   sql.Schema{{Type: aiCiType}},
			left:  sql.NewRow("a"),
			right: sql.NewRow("A"),
			equal: true,
		},
		{
			name:  "utf8mb4_0900_ai_ci accent insensitive",
			sch:   sql.Schema{{Type: aiCiType}},
			left:  sql.NewRow("é"),
			right: sql.NewRow("e"),
			equal: true,
		},
		{
			name:  "utf8mb4_0900_ai_ci NO PAD trailing space",
			sch:   sql.Schema{{Type: aiCiType}},
			left:  sql.NewRow("a"),
			right: sql.NewRow("a "),
			equal: false,
		},
		{
			name:  "utf8mb4_general_ci PAD SPACE trailing space",
			sch:   sql.Schema{{Type: generalCiType}},
			left:  sql.NewRow("a"),
			right: sql.NewRow("a "),
			equal: true,
		},
		{
			name:  "multi-column utf8mb4_0900_ai_ci different byte lengths",
			sch:   sql.Schema{{Type: types.Int32}, {Type: aiCiType}},
			left:  sql.NewRow(int32(1), "é"),
			right: sql.NewRow(int32(1), "e"),
			equal: true,
		},
		{
			name:  "multi-column utf8mb4_general_ci PAD SPACE trailing space",
			sch:   sql.Schema{{Type: types.Int32}, {Type: generalCiType}},
			left:  sql.NewRow(int32(1), "a"),
			right: sql.NewRow(int32(1), "a "),
			equal: true,
		},
		{
			name:  "CHAR NO PAD trims trailing space",
			sch:   sql.Schema{{Type: charAiCiType}},
			left:  sql.NewRow("a"),
			right: sql.NewRow("a "),
			equal: true,
		},
		{
			name:  "multi-column VARBINARY preserves column boundary across zero byte",
			sch:   sql.Schema{{Type: varBinaryType}, {Type: varBinaryType}},
			left:  sql.NewRow([]byte("a\x00b"), []byte("c")),
			right: sql.NewRow([]byte("a"), []byte("b\x00c")),
			equal: false,
		},
		{
			name:  "multi-column utf8mb4_0900_bin preserves column boundary across zero weight byte",
			sch:   sql.Schema{{Type: binType}, {Type: binType}},
			left:  sql.NewRow("x", "\x01y"),
			right: sql.NewRow("xĀ", "y"),
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftHash, err := HashOf(ctx, tt.sch, tt.left)
			require.NoError(t, err)
			rightHash, err := HashOf(ctx, tt.sch, tt.right)
			require.NoError(t, err)
			require.Equal(t, tt.equal, leftHash == rightHash)
		})
	}
}
