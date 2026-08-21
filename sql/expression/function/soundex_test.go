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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSoundex(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
	}{
		{"text nil", types.LongText, sql.NewRow(nil), nil},
		{"text empty", types.LongText, sql.NewRow(""), "0000"},
		{"text ignored character", types.LongText, sql.NewRow("-"), "0000"},
		{"text runes", types.LongText, sql.NewRow("日本語"), "日000"},
		{"text Hello ok", types.LongText, sql.NewRow("Hello"), "H400"},
		{"text Quadratically ok", types.LongText, sql.NewRow("Quadratically"), "Q36324"},
		{"text Lee ok", types.LongText, sql.NewRow("Lee"), "L000"},
		{"text McKnockitter ok", types.LongText, sql.NewRow("McKnockitter"), "M25236"},
		{"text Honeyman ok", types.LongText, sql.NewRow("Honeyman"), "H500"},
		{"text Munn ok", types.LongText, sql.NewRow("Munn"), "M000"},
		{"text Poppett ok", types.LongText, sql.NewRow("Poppett"), "P300"},
		{"text Peachman ok", types.LongText, sql.NewRow("Peachman"), "P250"},
		{"text Cochrane ok", types.LongText, sql.NewRow("Cochrane"), "C650"},
		{"text Chesley ok", types.LongText, sql.NewRow("Chesley"), "C400"},
		{"text Tachenion ok", types.LongText, sql.NewRow("Tachenion"), "T250"},
		{"text Wilcox ok", types.LongText, sql.NewRow("Wilcox"), "W420"},
		{"binary ok", types.LongText, sql.NewRow([]byte("Harvey")), "H610"},
		{"string one", types.LongText, sql.NewRow("1"), "0000"},
		{"other type", types.LongText, sql.NewRow(int32(1)), "0000"},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range testCases {
		f := NewSoundex(ctx, expression.NewGetField(0, tt.rowType, "", true))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})

		req := require.New(t)
		req.True(f.IsNullable(ctx))
		req.Equal(tt.rowType, f.Type(ctx))
	}
}

// TestSoundexNonASCIIInitial covers dolthub/dolt#11546. MySQL uppercases SOUNDEX input
// with soundex_toupper() (sql/item_strfunc.cc), which is deliberately ASCII-only:
//
//	static int soundex_toupper(int ch) {
//	  return (ch >= 'a' && ch <= 'z') ? ch - 'a' + 'A' : ch;
//	}
//
// The failing cases here used to run through strings.ToUpper, which case-folds the whole
// Unicode range, so the retained first character came back uppercased -- or, for U+017F
// and U+0131, silently rewritten into an ASCII letter -- where MySQL returns it verbatim.
func TestSoundexNonASCIIInitial(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected interface{}
	}{
		// The exact case reported in dolthub/dolt#11546.
		{"lowercase accented initial is preserved", "é", "é000"},
		{"uppercase accented initial is unchanged", "É", "É000"},
		{"accented initial with ASCII tail", "émile", "é540"},
		{"uppercase accented initial with ASCII tail", "Émile", "É540"},
		{"tilde n", "ñu", "ñ000"},
		{"leading garbage is still skipped", "-é", "é000"},
		{"repeated accented letter still collapses", "éé", "é000"},
		// unicode.ToUpper maps U+017F LATIN SMALL LETTER LONG S to 'S' and U+0131 LATIN
		// SMALL LETTER DOTLESS I to 'I'. Both changed the emitted initial, and U+017F also
		// changed its own soundex code from '0' (not a letter A-Z) to '2' (as 'S').
		{"long s stays long s", "ſ", "ſ000"},
		{"dotless i stays dotless i", "ı", "ı000"},
		// Guards. A non-ASCII letter that is not the first letter contributes only its
		// code, so it never reached the emitted-verbatim path and does not move here.
		{"non-initial accented letter", "Ré", "R000"},
		{"caseless script initial", "日本語", "日000"},
		{"ASCII is unaffected", "hello", "H400"},
		// U+00DF has no simple uppercase mapping in Go's tables, so strings.ToUpper leaves
		// it alone and it already matched MySQL. Pinned so a future change to the folding
		// rule cannot break it quietly.
		{"sharp s is not case-folded", "ß", "ß000"},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range testCases {
		f := NewSoundex(ctx, expression.NewGetField(0, types.LongText, "", true))
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.NewRow(tt.input)))
		})
	}
}
