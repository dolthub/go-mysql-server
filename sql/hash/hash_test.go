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
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
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
