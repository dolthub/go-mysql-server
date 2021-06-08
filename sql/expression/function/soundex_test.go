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

func TestSoundex(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
	}{
		{"text nil", sql.LongText, sql.NewRow(nil), nil},
		{"text empty", sql.LongText, sql.NewRow(""), "0000"},
		{"text ignored character", sql.LongText, sql.NewRow("-"), "0000"},
		{"text runes", sql.LongText, sql.NewRow("日本語"), "日000"},
		{"text Hello ok", sql.LongText, sql.NewRow("Hello"), "H400"},
		{"text Quadratically ok", sql.LongText, sql.NewRow("Quadratically"), "Q36324"},
		{"text Lee ok", sql.LongText, sql.NewRow("Lee"), "L000"},
		{"text McKnockitter ok", sql.LongText, sql.NewRow("McKnockitter"), "M25236"},
		{"text Honeyman ok", sql.LongText, sql.NewRow("Honeyman"), "H500"},
		{"text Munn ok", sql.LongText, sql.NewRow("Munn"), "M000"},
		{"text Poppett ok", sql.LongText, sql.NewRow("Poppett"), "P300"},
		{"text Peachman ok", sql.LongText, sql.NewRow("Peachman"), "P250"},
		{"text Cochrane ok", sql.LongText, sql.NewRow("Cochrane"), "C650"},
		{"text Chesley ok", sql.LongText, sql.NewRow("Chesley"), "C400"},
		{"text Tachenion ok", sql.LongText, sql.NewRow("Tachenion"), "T250"},
		{"text Wilcox ok", sql.LongText, sql.NewRow("Wilcox"), "W420"},
		{"binary ok", sql.LongText, sql.NewRow([]byte("Harvey")), "H610"},
		{"string one", sql.LongText, sql.NewRow("1"), "0000"},
		{"other type", sql.LongText, sql.NewRow(int32(1)), "0000"},
	}

	for _, tt := range testCases {
		f := NewSoundex(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", true))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})

		req := require.New(t)
		req.True(f.IsNullable())
		req.Equal(tt.rowType, f.Type())
	}
}
