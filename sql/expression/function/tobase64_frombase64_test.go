package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestBase64(t *testing.T) {
	fTo := NewToBase64(expression.NewGetField(0, sql.Text, "", false))
	fFrom := NewFromBase64(expression.NewGetField(0, sql.Text, "", false))

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
