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
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/apd/v3"
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

// minutes is a Hashable value in which equal values may have different printed forms, e.g. {1, 0} and {0, 60}.
type minutes struct {
	hours int
	mins  int
}

func (m minutes) WriteHashKey(w io.Writer) error {
	_, err := w.Write([]byte(strconv.Itoa(m.hours*60 + m.mins)))
	return err
}

// minutesType is a type whose values are minutes, which it converts without changing them.
type minutesType struct {
	types.JsonType
}

func (t minutesType) Convert(ctx context.Context, v any) (any, sql.ConvertInRange, error) {
	return v, sql.InRange, nil
}

func (t minutesType) Promote() sql.Type {
	return t
}

func TestHashOfHashable(t *testing.T) {
	ctx := sql.NewEmptyContext()
	hashOf := func(v any) uint64 {
		h, err := HashOf(ctx, nil, sql.NewRow(v))
		require.NoError(t, err)
		return h
	}

	require.Equal(t, hashOf(minutes{hours: 1}), hashOf(minutes{mins: 60}))
	require.NotEqual(t, hashOf(minutes{hours: 1}), hashOf(minutes{mins: 61}))
	require.Equal(t, hashOf([]any{minutes{hours: 1}, 2}), hashOf([]any{minutes{mins: 60}, 2}))

	simpleHashOf := func(v any) uint64 {
		h, _, err := HashOfSimple(ctx, v, minutesType{})
		require.NoError(t, err)
		return h
	}
	require.Equal(t, simpleHashOf(minutes{hours: 1}), simpleHashOf(minutes{mins: 60}))
	require.NotEqual(t, simpleHashOf(minutes{hours: 1}), simpleHashOf(minutes{mins: 61}))
}

func TestHashOfSlices(t *testing.T) {
	ctx := sql.NewEmptyContext()
	dec := func(s string) *apd.Decimal {
		d, _, err := apd.NewFromString(s)
		require.NoError(t, err)
		return d
	}
	hashOf := func(v any) uint64 {
		h, err := HashOf(ctx, nil, sql.NewRow(v))
		require.NoError(t, err)
		return h
	}

	require.Equal(t, hashOf([]any{dec("1.0")}), hashOf([]any{dec("1.00")}))
	require.Equal(t, hashOf([]any{dec("1.0"), nil}), hashOf([]any{dec("1"), nil}))
	require.Equal(t, hashOf([]any{[]any{dec("2.50")}}), hashOf([]any{[]any{dec("2.5")}}))
	require.NotEqual(t, hashOf([]any{dec("1.0")}), hashOf([]any{dec("1.1")}))
	require.NotEqual(t, hashOf([]any{"a", "b"}), hashOf([]any{"ab"}))
	require.NotEqual(t, hashOf([]any{1}), hashOf([]any{1, nil}))
}

func TestHashesCanonically(t *testing.T) {
	dec, _, err := apd.NewFromString("1.0")
	require.NoError(t, err)

	require.True(t, HashesCanonically(minutes{hours: 1}))
	require.True(t, HashesCanonically(dec))
	require.True(t, HashesCanonically([]any{dec, nil, 1, "a"}))
	require.True(t, HashesCanonically([]any{[]any{minutes{mins: 1}}}))
	require.False(t, HashesCanonically("a"))
	require.False(t, HashesCanonically(struct{}{}))
	require.False(t, HashesCanonically([]any{dec, struct{}{}}))
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
