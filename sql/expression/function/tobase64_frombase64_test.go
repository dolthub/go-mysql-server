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

func TestBase64(t *testing.T) {
	ctx := sql.NewEmptyContext()
	fTo := NewToBase64(ctx, expression.NewGetField(0, sql.LongText, "", false))
	fFrom := NewFromBase64(ctx, expression.NewGetField(0, sql.LongText, "", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		// Use a MySQL server to get expected values if updating/adding to this!
		{"null input", sql.NewRow(nil), nil, false},
		{"single_line", sql.NewRow("foo"), string("Zm9v"), false},
		{"multi_line", sql.NewRow(
			"Gallia est omnis divisa in partes tres, quarum unam " +
				"incolunt Belgae, aliam Aquitani, tertiam qui ipsorum lingua Celtae, " +
				"nostra Galli appellantur"),
			"R2FsbGlhIGVzdCBvbW5pcyBkaXZpc2EgaW4gcGFydGVzIHRyZXMsIHF1YXJ1bSB1bmFtIGluY29s\n" +
				"dW50IEJlbGdhZSwgYWxpYW0gQXF1aXRhbmksIHRlcnRpYW0gcXVpIGlwc29ydW0gbGluZ3VhIENl\n" +
				"bHRhZSwgbm9zdHJhIEdhbGxpIGFwcGVsbGFudHVy", false},
		{"empty_input", sql.NewRow(""), string(""), false},
		{"symbols", sql.NewRow("!@#$% %^&*()_+\r\n\t{};"), string("IUAjJCUgJV4mKigpXysNCgl7fTs="),
			false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			v, err := fTo.Eval(ctx, tt.row)

			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)

				ctx = sql.NewEmptyContext()
				v2, err := fFrom.Eval(ctx, sql.NewRow(v))
				require.NoError(err)
				require.Equal(sql.NewRow(v2), tt.row)
			}
		})
	}
}
