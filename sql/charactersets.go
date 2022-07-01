// Copyright 2022 Dolthub, Inc.
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

package sql

import "gopkg.in/src-d/go-errors.v1"

var ErrCharacterSetNotSupported = errors.NewKind("Unknown character set: %v")

// CharacterSet represents the character set of a string.
type CharacterSet struct {
	ID               CharacterSetID
	Name             string
	DefaultCollation CollationID
	BinaryCollation  CollationID
	Description      string
	MaxLength        uint8
}

// CharacterSetsIterator iterates over every character set available.
type CharacterSetsIterator struct {
	idx int
}

// CharacterSetID represents a character set. Unlike collations, this ID is not intended for storage and may change as
// new character sets are added. It is recommended to use the character set's name if persistence is desired.
type CharacterSetID uint16

// The character sets below are ordered alphabetically to make it easier to visually parse them.
// As each ID acts as an index to the `characterSetArray`, they are explicitly defined.
// It is recommended not to change any IDs when adding a new character set, as it will also require adjusting the array.
// Instead, give it the next highest number, and insert it into the correct position alphabetically.

const (
	CharacterSet_armscii8 CharacterSetID = 0
	CharacterSet_ascii    CharacterSetID = 1
	CharacterSet_big5     CharacterSetID = 2
	CharacterSet_binary   CharacterSetID = 3
	CharacterSet_cp1250   CharacterSetID = 4
	CharacterSet_cp1251   CharacterSetID = 5
	CharacterSet_cp1256   CharacterSetID = 6
	CharacterSet_cp1257   CharacterSetID = 7
	CharacterSet_cp850    CharacterSetID = 8
	CharacterSet_cp852    CharacterSetID = 9
	CharacterSet_cp866    CharacterSetID = 10
	CharacterSet_cp932    CharacterSetID = 11
	CharacterSet_dec8     CharacterSetID = 12
	CharacterSet_eucjpms  CharacterSetID = 13
	CharacterSet_euckr    CharacterSetID = 14
	CharacterSet_gb18030  CharacterSetID = 15
	CharacterSet_gb2312   CharacterSetID = 16
	CharacterSet_gbk      CharacterSetID = 17
	CharacterSet_geostd8  CharacterSetID = 18
	CharacterSet_greek    CharacterSetID = 19
	CharacterSet_hebrew   CharacterSetID = 20
	CharacterSet_hp8      CharacterSetID = 21
	CharacterSet_keybcs2  CharacterSetID = 22
	CharacterSet_koi8r    CharacterSetID = 23
	CharacterSet_koi8u    CharacterSetID = 24
	CharacterSet_latin1   CharacterSetID = 25
	CharacterSet_latin2   CharacterSetID = 26
	CharacterSet_latin5   CharacterSetID = 27
	CharacterSet_latin7   CharacterSetID = 28
	CharacterSet_macce    CharacterSetID = 29
	CharacterSet_macroman CharacterSetID = 30
	CharacterSet_sjis     CharacterSetID = 31
	CharacterSet_swe7     CharacterSetID = 32
	CharacterSet_tis620   CharacterSetID = 33
	CharacterSet_ucs2     CharacterSetID = 34
	CharacterSet_ujis     CharacterSetID = 35
	CharacterSet_utf16    CharacterSetID = 36
	CharacterSet_utf16le  CharacterSetID = 37
	CharacterSet_utf32    CharacterSetID = 38
	CharacterSet_utf8mb3  CharacterSetID = 39
	CharacterSet_utf8mb4  CharacterSetID = 40

	CharacterSet_utf8 = CharacterSet_utf8mb3
)

// characterSetArray contains the details of every character set, indexed by their ID. This allows for character sets to
// be efficiently passed around (since only an uint16 is needed), while still being able to quickly access all of their
// properties (index lookups are significantly faster than map lookups).
var characterSetArray = [41]CharacterSet{
	/*00*/ {CharacterSet_armscii8, "armscii8", Collation_armscii8_general_ci, Collation_armscii8_bin, "ARMSCII-8 Armenian", 1},
	/*01*/ {CharacterSet_ascii, "ascii", Collation_ascii_general_ci, Collation_ascii_bin, "US ASCII", 1},
	/*02*/ {CharacterSet_big5, "big5", Collation_big5_chinese_ci, Collation_big5_bin, "Big5 Traditional Chinese", 2},
	/*03*/ {CharacterSet_binary, "binary", Collation_binary, Collation_binary, "Binary pseudo charset", 1},
	/*04*/ {CharacterSet_cp1250, "cp1250", Collation_cp1250_general_ci, Collation_cp1250_bin, "Windows Central European", 1},
	/*05*/ {CharacterSet_cp1251, "cp1251", Collation_cp1251_general_ci, Collation_cp1251_bin, "Windows Cyrillic", 1},
	/*06*/ {CharacterSet_cp1256, "cp1256", Collation_cp1256_general_ci, Collation_cp1256_bin, "Windows Arabic", 1},
	/*07*/ {CharacterSet_cp1257, "cp1257", Collation_cp1257_general_ci, Collation_cp1257_bin, "Windows Baltic", 1},
	/*08*/ {CharacterSet_cp850, "cp850", Collation_cp850_general_ci, Collation_cp850_bin, "DOS West European", 1},
	/*09*/ {CharacterSet_cp852, "cp852", Collation_cp852_general_ci, Collation_cp852_bin, "DOS Central European", 1},
	/*10*/ {CharacterSet_cp866, "cp866", Collation_cp866_general_ci, Collation_cp866_bin, "DOS Russian", 1},
	/*11*/ {CharacterSet_cp932, "cp932", Collation_cp932_japanese_ci, Collation_cp932_bin, "SJIS for Windows Japanese", 2},
	/*12*/ {CharacterSet_dec8, "dec8", Collation_dec8_swedish_ci, Collation_dec8_bin, "DEC West European", 1},
	/*13*/ {CharacterSet_eucjpms, "eucjpms", Collation_eucjpms_japanese_ci, Collation_eucjpms_bin, "UJIS for Windows Japanese", 3},
	/*14*/ {CharacterSet_euckr, "euckr", Collation_euckr_korean_ci, Collation_euckr_bin, "EUC-KR Korean", 2},
	/*15*/ {CharacterSet_gb18030, "gb18030", Collation_gb18030_chinese_ci, Collation_gb18030_bin, "China National Standard GB18030", 4},
	/*16*/ {CharacterSet_gb2312, "gb2312", Collation_gb2312_chinese_ci, Collation_gb2312_bin, "GB2312 Simplified Chinese", 2},
	/*17*/ {CharacterSet_gbk, "gbk", Collation_gbk_chinese_ci, Collation_gbk_bin, "GBK Simplified Chinese", 2},
	/*18*/ {CharacterSet_geostd8, "geostd8", Collation_geostd8_general_ci, Collation_geostd8_bin, "GEOSTD8 Georgian", 1},
	/*19*/ {CharacterSet_greek, "greek", Collation_greek_general_ci, Collation_greek_bin, "ISO 8859-7 Greek", 1},
	/*20*/ {CharacterSet_hebrew, "hebrew", Collation_hebrew_general_ci, Collation_hebrew_bin, "ISO 8859-8 Hebrew", 1},
	/*21*/ {CharacterSet_hp8, "hp8", Collation_hp8_english_ci, Collation_hp8_bin, "HP West European", 1},
	/*22*/ {CharacterSet_keybcs2, "keybcs2", Collation_keybcs2_general_ci, Collation_keybcs2_bin, "DOS Kamenicky Czech-Slovak", 1},
	/*23*/ {CharacterSet_koi8r, "koi8r", Collation_koi8r_general_ci, Collation_koi8r_bin, "KOI8-R Relcom Russian", 1},
	/*24*/ {CharacterSet_koi8u, "koi8u", Collation_koi8u_general_ci, Collation_koi8u_bin, "KOI8-U Ukrainian", 1},
	/*25*/ {CharacterSet_latin1, "latin1", Collation_latin1_swedish_ci, Collation_latin1_bin, "cp1252 West European", 1},
	/*26*/ {CharacterSet_latin2, "latin2", Collation_latin2_general_ci, Collation_latin2_bin, "ISO 8859-2 Central European", 1},
	/*27*/ {CharacterSet_latin5, "latin5", Collation_latin5_turkish_ci, Collation_latin5_bin, "ISO 8859-9 Turkish", 1},
	/*28*/ {CharacterSet_latin7, "latin7", Collation_latin7_general_ci, Collation_latin7_bin, "ISO 8859-13 Baltic", 1},
	/*29*/ {CharacterSet_macce, "macce", Collation_macce_general_ci, Collation_macce_bin, "Mac Central European", 1},
	/*30*/ {CharacterSet_macroman, "macroman", Collation_macroman_general_ci, Collation_macroman_bin, "Mac West European", 1},
	/*31*/ {CharacterSet_sjis, "sjis", Collation_sjis_japanese_ci, Collation_sjis_bin, "Shift-JIS Japanese", 2},
	/*32*/ {CharacterSet_swe7, "swe7", Collation_swe7_swedish_ci, Collation_swe7_bin, "7bit Swedish", 1},
	/*33*/ {CharacterSet_tis620, "tis620", Collation_tis620_thai_ci, Collation_tis620_bin, "TIS620 Thai", 1},
	/*34*/ {CharacterSet_ucs2, "ucs2", Collation_ucs2_general_ci, Collation_ucs2_bin, "UCS-2 Unicode", 2},
	/*35*/ {CharacterSet_ujis, "ujis", Collation_ujis_japanese_ci, Collation_ujis_bin, "EUC-JP Japanese", 3},
	/*36*/ {CharacterSet_utf16, "utf16", Collation_utf16_general_ci, Collation_utf16_bin, "UTF-16 Unicode", 4},
	/*37*/ {CharacterSet_utf16le, "utf16le", Collation_utf16le_general_ci, Collation_utf16le_bin, "UTF-16LE Unicode", 4},
	/*38*/ {CharacterSet_utf32, "utf32", Collation_utf32_general_ci, Collation_utf32_bin, "UTF-32 Unicode", 4},
	/*39*/ {CharacterSet_utf8mb3, "utf8mb3", Collation_utf8mb3_general_ci, Collation_utf8mb3_bin, "UTF-8 Unicode", 3},
	/*40*/ {CharacterSet_utf8mb4, "utf8mb4", Collation_utf8mb4_0900_ai_ci, Collation_utf8mb4_bin, "UTF-8 Unicode", 4},
}

// characterSetStringToID maps a character set's name to its ID.
var characterSetStringToID = map[string]CharacterSetID{
	"armscii8": CharacterSet_armscii8,
	"ascii":    CharacterSet_ascii,
	"big5":     CharacterSet_big5,
	"binary":   CharacterSet_binary,
	"cp1250":   CharacterSet_cp1250,
	"cp1251":   CharacterSet_cp1251,
	"cp1256":   CharacterSet_cp1256,
	"cp1257":   CharacterSet_cp1257,
	"cp850":    CharacterSet_cp850,
	"cp852":    CharacterSet_cp852,
	"cp866":    CharacterSet_cp866,
	"cp932":    CharacterSet_cp932,
	"dec8":     CharacterSet_dec8,
	"eucjpms":  CharacterSet_eucjpms,
	"euckr":    CharacterSet_euckr,
	"gb18030":  CharacterSet_gb18030,
	"gb2312":   CharacterSet_gb2312,
	"gbk":      CharacterSet_gbk,
	"geostd8":  CharacterSet_geostd8,
	"greek":    CharacterSet_greek,
	"hebrew":   CharacterSet_hebrew,
	"hp8":      CharacterSet_hp8,
	"keybcs2":  CharacterSet_keybcs2,
	"koi8r":    CharacterSet_koi8r,
	"koi8u":    CharacterSet_koi8u,
	"latin1":   CharacterSet_latin1,
	"latin2":   CharacterSet_latin2,
	"latin5":   CharacterSet_latin5,
	"latin7":   CharacterSet_latin7,
	"macce":    CharacterSet_macce,
	"macroman": CharacterSet_macroman,
	"sjis":     CharacterSet_sjis,
	"swe7":     CharacterSet_swe7,
	"tis620":   CharacterSet_tis620,
	"ucs2":     CharacterSet_ucs2,
	"ujis":     CharacterSet_ujis,
	"utf16":    CharacterSet_utf16,
	"utf16le":  CharacterSet_utf16le,
	"utf32":    CharacterSet_utf32,
	"utf8":     CharacterSet_utf8mb3,
	"utf8mb3":  CharacterSet_utf8mb3,
	"utf8mb4":  CharacterSet_utf8mb4,
}

// SupportedCharsets contains all non-binary character sets that are currently supported.
var SupportedCharsets = []CharacterSetID{
	CharacterSet_utf8mb4,
}

// ParseCharacterSet takes in a string representing a CharacterSet and returns the result if a match is found, or an
// error if not.
func ParseCharacterSet(str string) (CharacterSetID, error) {
	if cs, ok := characterSetStringToID[str]; ok {
		return cs, nil
	}
	return Collation_Default.CharacterSet(), ErrCharacterSetNotSupported.New(str)
}

// Name returns the name of this CharacterSet.
func (cs CharacterSetID) Name() string {
	return characterSetArray[cs].Name
}

// DefaultCollation returns the default CollationID for this CharacterSet.
func (cs CharacterSetID) DefaultCollation() CollationID {
	return characterSetArray[cs].DefaultCollation
}

// BinaryCollation returns the binary CollationID for this CharacterSet.
func (cs CharacterSetID) BinaryCollation() CollationID {
	return characterSetArray[cs].BinaryCollation
}

// Description returns the plain-English description of the CharacterSet.
func (cs CharacterSetID) Description() string {
	return characterSetArray[cs].Description
}

// MaxLength returns the maximum size of a single character in the CharacterSet.
func (cs CharacterSetID) MaxLength() int64 {
	return int64(characterSetArray[cs].MaxLength)
}

// String returns the string representation of the CharacterSet.
func (cs CharacterSetID) String() string {
	return characterSetArray[cs].Name
}

// NewCharacterSetsIterator returns a new CharacterSetsIterator.
func NewCharacterSetsIterator() *CharacterSetsIterator {
	return &CharacterSetsIterator{0}
}

// Next returns the next character set. If all character sets have been iterated over, returns false.
func (csi *CharacterSetsIterator) Next() (CharacterSet, bool) {
	if csi.idx >= len(characterSetArray) {
		return CharacterSet{}, false
	}
	csi.idx++
	return characterSetArray[csi.idx-1], true
}
