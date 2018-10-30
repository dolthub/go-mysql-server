package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSoundex(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
	}{
		{"text nil", sql.Text, sql.NewRow(nil), nil},
		{"text empty", sql.Text, sql.NewRow(""), ""},
		{"text ignored character", sql.Text, sql.NewRow("-"), ""},
		{"text runes", sql.Text, sql.NewRow("日本語"), "日000"},
		{"text Hello ok", sql.Text, sql.NewRow("Hello"), "H400"},
		{"text Quadratically ok", sql.Text, sql.NewRow("Quadratically"), "Q36324"},
		{"text Lee ok", sql.Text, sql.NewRow("Lee"), "L000"},
		{"text McKnockitter ok", sql.Text, sql.NewRow("McKnockitter"), "M25236"},
		{"text Honeyman ok", sql.Text, sql.NewRow("Honeyman"), "H500"},
		{"text Munn ok", sql.Text, sql.NewRow("Munn"), "M000"},
		{"text Poppett ok", sql.Text, sql.NewRow("Poppett"), "P300"},
		{"text Peachman ok", sql.Text, sql.NewRow("Peachman"), "P250"},
		{"text Cochrane ok", sql.Text, sql.NewRow("Cochrane"), "C650"},
		{"text Chesley ok", sql.Text, sql.NewRow("Chesley"), "C400"},
		{"text Tachenion ok", sql.Text, sql.NewRow("Tachenion"), "T250"},
		{"text Wilcox ok", sql.Text, sql.NewRow("Wilcox"), "W420"},
		{"binary ok", sql.Blob, sql.NewRow([]byte("Harvey")), "H610"},
		{"other type", sql.Int32, sql.NewRow(int32(1)), ""},
	}

	for _, tt := range testCases {
		f := NewSoundex(expression.NewGetField(0, tt.rowType, "", true))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})

		req := require.New(t)
		req.True(f.IsNullable())
		req.Equal(tt.rowType, f.Type())
	}
}
