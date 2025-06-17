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

package function

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Last Run: 06/17/2025
// BenchmarkRegexpSubstr
// BenchmarkRegexpSubstr-14    	     100	  95661410 ns/op
// BenchmarkRegexpSubstr-14    	   10000	    999559 ns/op
func BenchmarkRegexpSubstr(b *testing.B) {
	ctx := sql.NewEmptyContext()
	data := make([]sql.Row, 100)
	for i := range data {
		data[i] = sql.Row{fmt.Sprintf("test%d", i)}
	}

	for i := 0; i < b.N; i++ {
		f, err := NewRegexpSubstr(
			expression.NewGetField(0, types.LongText, "text", false),
			expression.NewLiteral("^test[0-9]$", types.LongText),
		)
		require.NoError(b, err)
		var total int
		for _, row := range data {
			res, err := f.Eval(ctx, row)
			require.NoError(b, err)
			if res != nil && res.(string)[:4] == "test" {
				total++
			}
		}
		require.Equal(b, 10, total)
		f.(*RegexpSubstr).Dispose()
	}
}
