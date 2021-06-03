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

package sql

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/regex"
)

// CharacterSet represents the character set of a string.
type CharacterSet string

type insensitiveMatcher struct {
	regex.DisposableMatcher
}

func (im *insensitiveMatcher) Match(matchStr string) bool {
	lower := strings.ToLower(matchStr)
	return im.DisposableMatcher.Match(lower)
}

func insensitiveLikeMatcher(likeStr string) (regex.DisposableMatcher, error) {
	lower := strings.ToLower(likeStr)
	dm, err := regex.NewDisposableMatcher("go", lower)

	if err != nil {
		return nil, err
	}

	return &insensitiveMatcher{dm}, nil
}

func sensitiveLikeMatcher(likeStr string) (regex.DisposableMatcher, error) {
	return regex.NewDisposableMatcher("go", likeStr)
}

func insensitiveCompare(a, b string) int {
	lowerA := strings.ToLower(a)
	lowerB := strings.ToLower(b)
	return strings.Compare(lowerA, lowerB)
}

// Collation represents the collation of a string.
type Collation struct {
	Name        string
	CharSet     CharacterSet
	Compare     func(as, bs string) int
	LikeMatcher func(likeStr string) (regex.DisposableMatcher, error)
}

var Collations = map[string]Collation{}

func newCollation(name string, cs CharacterSet) Collation {
	c := Collation{Name: name, CharSet: cs, Compare: insensitiveCompare, LikeMatcher: insensitiveLikeMatcher}
	Collations[name] = c
	return c
}

func newCSCollation(name string, cs CharacterSet) Collation {
	c := Collation{Name: name, CharSet: cs, Compare: strings.Compare, LikeMatcher: sensitiveLikeMatcher}
	Collations[name] = c
	return c
}

// Character sets and collations were obtained from a fresh install of MySQL 8.0.17.
// The character sets were obtained by running `SHOW CHARACTER SET;`.
// The collations were obtained by running `SHOW COLLATION;`.
// utf8mb3 is not listed from the above commands, and was obtained from: https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html
const (
	CharacterSet_armscii8 CharacterSet = "armscii8"
	CharacterSet_ascii    CharacterSet = "ascii"
	CharacterSet_big5     CharacterSet = "big5"
	CharacterSet_binary   CharacterSet = "binary"
	CharacterSet_cp1250   CharacterSet = "cp1250"
	CharacterSet_cp1251   CharacterSet = "cp1251"
	CharacterSet_cp1256   CharacterSet = "cp1256"
	CharacterSet_cp1257   CharacterSet = "cp1257"
	CharacterSet_cp850    CharacterSet = "cp850"
	CharacterSet_cp852    CharacterSet = "cp852"
	CharacterSet_cp866    CharacterSet = "cp866"
	CharacterSet_cp932    CharacterSet = "cp932"
	CharacterSet_dec8     CharacterSet = "dec8"
	CharacterSet_eucjpms  CharacterSet = "eucjpms"
	CharacterSet_euckr    CharacterSet = "euckr"
	CharacterSet_gb18030  CharacterSet = "gb18030"
	CharacterSet_gb2312   CharacterSet = "gb2312"
	CharacterSet_gbk      CharacterSet = "gbk"
	CharacterSet_geostd8  CharacterSet = "geostd8"
	CharacterSet_greek    CharacterSet = "greek"
	CharacterSet_hebrew   CharacterSet = "hebrew"
	CharacterSet_hp8      CharacterSet = "hp8"
	CharacterSet_keybcs2  CharacterSet = "keybcs2"
	CharacterSet_koi8r    CharacterSet = "koi8r"
	CharacterSet_koi8u    CharacterSet = "koi8u"
	CharacterSet_latin1   CharacterSet = "latin1"
	CharacterSet_latin2   CharacterSet = "latin2"
	CharacterSet_latin5   CharacterSet = "latin5"
	CharacterSet_latin7   CharacterSet = "latin7"
	CharacterSet_macce    CharacterSet = "macce"
	CharacterSet_macroman CharacterSet = "macroman"
	CharacterSet_sjis     CharacterSet = "sjis"
	CharacterSet_swe7     CharacterSet = "swe7"
	CharacterSet_tis620   CharacterSet = "tis620"
	CharacterSet_ucs2     CharacterSet = "ucs2"
	CharacterSet_ujis     CharacterSet = "ujis"
	CharacterSet_utf16    CharacterSet = "utf16"
	CharacterSet_utf16le  CharacterSet = "utf16le"
	CharacterSet_utf32    CharacterSet = "utf32"
	CharacterSet_utf8     CharacterSet = "utf8"
	CharacterSet_utf8mb3  CharacterSet = "utf8mb3"
	CharacterSet_utf8mb4  CharacterSet = "utf8mb4"
)

var (
	// case sensitive colations
	Collation_binary = newCSCollation("binary", CharacterSet_binary)

	// case insensitive collations
	Collation_armscii8_general_ci         = newCollation("armscii8_general_ci", CharacterSet_armscii8)
	Collation_armscii8_bin                = newCollation("armscii8_bin", CharacterSet_armscii8)
	Collation_ascii_general_ci            = newCollation("ascii_general_ci", CharacterSet_ascii)
	Collation_ascii_bin                   = newCollation("ascii_bin", CharacterSet_ascii)
	Collation_big5_chinese_ci             = newCollation("big5_chinese_ci", CharacterSet_big5)
	Collation_big5_bin                    = newCollation("big5_bin", CharacterSet_big5)
	Collation_cp1250_general_ci           = newCollation("cp1250_general_ci", CharacterSet_cp1250)
	Collation_cp1250_czech_cs             = newCollation("cp1250_czech_cs", CharacterSet_cp1250)
	Collation_cp1250_croatian_ci          = newCollation("cp1250_croatian_ci", CharacterSet_cp1250)
	Collation_cp1250_bin                  = newCollation("cp1250_bin", CharacterSet_cp1250)
	Collation_cp1250_polish_ci            = newCollation("cp1250_polish_ci", CharacterSet_cp1250)
	Collation_cp1251_bulgarian_ci         = newCollation("cp1251_bulgarian_ci", CharacterSet_cp1251)
	Collation_cp1251_ukrainian_ci         = newCollation("cp1251_ukrainian_ci", CharacterSet_cp1251)
	Collation_cp1251_bin                  = newCollation("cp1251_bin", CharacterSet_cp1251)
	Collation_cp1251_general_ci           = newCollation("cp1251_general_ci", CharacterSet_cp1251)
	Collation_cp1251_general_cs           = newCollation("cp1251_general_cs", CharacterSet_cp1251)
	Collation_cp1256_general_ci           = newCollation("cp1256_general_ci", CharacterSet_cp1256)
	Collation_cp1256_bin                  = newCollation("cp1256_bin", CharacterSet_cp1256)
	Collation_cp1257_lithuanian_ci        = newCollation("cp1257_lithuanian_ci", CharacterSet_cp1257)
	Collation_cp1257_bin                  = newCollation("cp1257_bin", CharacterSet_cp1257)
	Collation_cp1257_general_ci           = newCollation("cp1257_general_ci", CharacterSet_cp1257)
	Collation_cp850_general_ci            = newCollation("cp850_general_ci", CharacterSet_cp850)
	Collation_cp850_bin                   = newCollation("cp850_bin", CharacterSet_cp850)
	Collation_cp852_general_ci            = newCollation("cp852_general_ci", CharacterSet_cp852)
	Collation_cp852_bin                   = newCollation("cp852_bin", CharacterSet_cp852)
	Collation_cp866_general_ci            = newCollation("cp866_general_ci", CharacterSet_cp866)
	Collation_cp866_bin                   = newCollation("cp866_bin", CharacterSet_cp866)
	Collation_cp932_japanese_ci           = newCollation("cp932_japanese_ci", CharacterSet_cp932)
	Collation_cp932_bin                   = newCollation("cp932_bin", CharacterSet_cp932)
	Collation_dec8_swedish_ci             = newCollation("dec8_swedish_ci", CharacterSet_dec8)
	Collation_dec8_bin                    = newCollation("dec8_bin", CharacterSet_dec8)
	Collation_eucjpms_japanese_ci         = newCollation("eucjpms_japanese_ci", CharacterSet_eucjpms)
	Collation_eucjpms_bin                 = newCollation("eucjpms_bin", CharacterSet_eucjpms)
	Collation_euckr_korean_ci             = newCollation("euckr_korean_ci", CharacterSet_euckr)
	Collation_euckr_bin                   = newCollation("euckr_bin", CharacterSet_euckr)
	Collation_gb18030_chinese_ci          = newCollation("gb18030_chinese_ci", CharacterSet_gb18030)
	Collation_gb18030_bin                 = newCollation("gb18030_bin", CharacterSet_gb18030)
	Collation_gb18030_unicode_520_ci      = newCollation("gb18030_unicode_520_ci", CharacterSet_gb18030)
	Collation_gb2312_chinese_ci           = newCollation("gb2312_chinese_ci", CharacterSet_gb2312)
	Collation_gb2312_bin                  = newCollation("gb2312_bin", CharacterSet_gb2312)
	Collation_gbk_chinese_ci              = newCollation("gbk_chinese_ci", CharacterSet_gbk)
	Collation_gbk_bin                     = newCollation("gbk_bin", CharacterSet_gbk)
	Collation_geostd8_general_ci          = newCollation("geostd8_general_ci", CharacterSet_geostd8)
	Collation_geostd8_bin                 = newCollation("geostd8_bin", CharacterSet_geostd8)
	Collation_greek_general_ci            = newCollation("greek_general_ci", CharacterSet_greek)
	Collation_greek_bin                   = newCollation("greek_bin", CharacterSet_greek)
	Collation_hebrew_general_ci           = newCollation("hebrew_general_ci", CharacterSet_hebrew)
	Collation_hebrew_bin                  = newCollation("hebrew_bin", CharacterSet_hebrew)
	Collation_hp8_english_ci              = newCollation("hp8_english_ci", CharacterSet_hp8)
	Collation_hp8_bin                     = newCollation("hp8_bin", CharacterSet_hp8)
	Collation_keybcs2_general_ci          = newCollation("keybcs2_general_ci", CharacterSet_keybcs2)
	Collation_keybcs2_bin                 = newCollation("keybcs2_bin", CharacterSet_keybcs2)
	Collation_koi8r_general_ci            = newCollation("koi8r_general_ci", CharacterSet_koi8r)
	Collation_koi8r_bin                   = newCollation("koi8r_bin", CharacterSet_koi8r)
	Collation_koi8u_general_ci            = newCollation("koi8u_general_ci", CharacterSet_koi8u)
	Collation_koi8u_bin                   = newCollation("koi8u_bin", CharacterSet_koi8u)
	Collation_latin1_german1_ci           = newCollation("latin1_german1_ci", CharacterSet_latin1)
	Collation_latin1_swedish_ci           = newCollation("latin1_swedish_ci", CharacterSet_latin1)
	Collation_latin1_danish_ci            = newCollation("latin1_danish_ci", CharacterSet_latin1)
	Collation_latin1_german2_ci           = newCollation("latin1_german2_ci", CharacterSet_latin1)
	Collation_latin1_bin                  = newCollation("latin1_bin", CharacterSet_latin1)
	Collation_latin1_general_ci           = newCollation("latin1_general_ci", CharacterSet_latin1)
	Collation_latin1_general_cs           = newCollation("latin1_general_cs", CharacterSet_latin1)
	Collation_latin1_spanish_ci           = newCollation("latin1_spanish_ci", CharacterSet_latin1)
	Collation_latin2_czech_cs             = newCollation("latin2_czech_cs", CharacterSet_latin2)
	Collation_latin2_general_ci           = newCollation("latin2_general_ci", CharacterSet_latin2)
	Collation_latin2_hungarian_ci         = newCollation("latin2_hungarian_ci", CharacterSet_latin2)
	Collation_latin2_croatian_ci          = newCollation("latin2_croatian_ci", CharacterSet_latin2)
	Collation_latin2_bin                  = newCollation("latin2_bin", CharacterSet_latin2)
	Collation_latin5_turkish_ci           = newCollation("latin5_turkish_ci", CharacterSet_latin5)
	Collation_latin5_bin                  = newCollation("latin5_bin", CharacterSet_latin5)
	Collation_latin7_estonian_cs          = newCollation("latin7_estonian_cs", CharacterSet_latin7)
	Collation_latin7_general_ci           = newCollation("latin7_general_ci", CharacterSet_latin7)
	Collation_latin7_general_cs           = newCollation("latin7_general_cs", CharacterSet_latin7)
	Collation_latin7_bin                  = newCollation("latin7_bin", CharacterSet_latin7)
	Collation_macce_general_ci            = newCollation("macce_general_ci", CharacterSet_macce)
	Collation_macce_bin                   = newCollation("macce_bin", CharacterSet_macce)
	Collation_macroman_general_ci         = newCollation("macroman_general_ci", CharacterSet_macroman)
	Collation_macroman_bin                = newCollation("macroman_bin", CharacterSet_macroman)
	Collation_sjis_japanese_ci            = newCollation("sjis_japanese_ci", CharacterSet_sjis)
	Collation_sjis_bin                    = newCollation("sjis_bin", CharacterSet_sjis)
	Collation_swe7_swedish_ci             = newCollation("swe7_swedish_ci", CharacterSet_swe7)
	Collation_swe7_bin                    = newCollation("swe7_bin", CharacterSet_swe7)
	Collation_tis620_thai_ci              = newCollation("tis620_thai_ci", CharacterSet_tis620)
	Collation_tis620_bin                  = newCollation("tis620_bin", CharacterSet_tis620)
	Collation_ucs2_general_ci             = newCollation("ucs2_general_ci", CharacterSet_ucs2)
	Collation_ucs2_bin                    = newCollation("ucs2_bin", CharacterSet_ucs2)
	Collation_ucs2_unicode_ci             = newCollation("ucs2_unicode_ci", CharacterSet_ucs2)
	Collation_ucs2_icelandic_ci           = newCollation("ucs2_icelandic_ci", CharacterSet_ucs2)
	Collation_ucs2_latvian_ci             = newCollation("ucs2_latvian_ci", CharacterSet_ucs2)
	Collation_ucs2_romanian_ci            = newCollation("ucs2_romanian_ci", CharacterSet_ucs2)
	Collation_ucs2_slovenian_ci           = newCollation("ucs2_slovenian_ci", CharacterSet_ucs2)
	Collation_ucs2_polish_ci              = newCollation("ucs2_polish_ci", CharacterSet_ucs2)
	Collation_ucs2_estonian_ci            = newCollation("ucs2_estonian_ci", CharacterSet_ucs2)
	Collation_ucs2_spanish_ci             = newCollation("ucs2_spanish_ci", CharacterSet_ucs2)
	Collation_ucs2_swedish_ci             = newCollation("ucs2_swedish_ci", CharacterSet_ucs2)
	Collation_ucs2_turkish_ci             = newCollation("ucs2_turkish_ci", CharacterSet_ucs2)
	Collation_ucs2_czech_ci               = newCollation("ucs2_czech_ci", CharacterSet_ucs2)
	Collation_ucs2_danish_ci              = newCollation("ucs2_danish_ci", CharacterSet_ucs2)
	Collation_ucs2_lithuanian_ci          = newCollation("ucs2_lithuanian_ci", CharacterSet_ucs2)
	Collation_ucs2_slovak_ci              = newCollation("ucs2_slovak_ci", CharacterSet_ucs2)
	Collation_ucs2_spanish2_ci            = newCollation("ucs2_spanish2_ci", CharacterSet_ucs2)
	Collation_ucs2_roman_ci               = newCollation("ucs2_roman_ci", CharacterSet_ucs2)
	Collation_ucs2_persian_ci             = newCollation("ucs2_persian_ci", CharacterSet_ucs2)
	Collation_ucs2_esperanto_ci           = newCollation("ucs2_esperanto_ci", CharacterSet_ucs2)
	Collation_ucs2_hungarian_ci           = newCollation("ucs2_hungarian_ci", CharacterSet_ucs2)
	Collation_ucs2_sinhala_ci             = newCollation("ucs2_sinhala_ci", CharacterSet_ucs2)
	Collation_ucs2_german2_ci             = newCollation("ucs2_german2_ci", CharacterSet_ucs2)
	Collation_ucs2_croatian_ci            = newCollation("ucs2_croatian_ci", CharacterSet_ucs2)
	Collation_ucs2_unicode_520_ci         = newCollation("ucs2_unicode_520_ci", CharacterSet_ucs2)
	Collation_ucs2_vietnamese_ci          = newCollation("ucs2_vietnamese_ci", CharacterSet_ucs2)
	Collation_ucs2_general_mysql500_ci    = newCollation("ucs2_general_mysql500_ci", CharacterSet_ucs2)
	Collation_ujis_japanese_ci            = newCollation("ujis_japanese_ci", CharacterSet_ujis)
	Collation_ujis_bin                    = newCollation("ujis_bin", CharacterSet_ujis)
	Collation_utf16_general_ci            = newCollation("utf16_general_ci", CharacterSet_utf16)
	Collation_utf16_bin                   = newCollation("utf16_bin", CharacterSet_utf16)
	Collation_utf16_unicode_ci            = newCollation("utf16_unicode_ci", CharacterSet_utf16)
	Collation_utf16_icelandic_ci          = newCollation("utf16_icelandic_ci", CharacterSet_utf16)
	Collation_utf16_latvian_ci            = newCollation("utf16_latvian_ci", CharacterSet_utf16)
	Collation_utf16_romanian_ci           = newCollation("utf16_romanian_ci", CharacterSet_utf16)
	Collation_utf16_slovenian_ci          = newCollation("utf16_slovenian_ci", CharacterSet_utf16)
	Collation_utf16_polish_ci             = newCollation("utf16_polish_ci", CharacterSet_utf16)
	Collation_utf16_estonian_ci           = newCollation("utf16_estonian_ci", CharacterSet_utf16)
	Collation_utf16_spanish_ci            = newCollation("utf16_spanish_ci", CharacterSet_utf16)
	Collation_utf16_swedish_ci            = newCollation("utf16_swedish_ci", CharacterSet_utf16)
	Collation_utf16_turkish_ci            = newCollation("utf16_turkish_ci", CharacterSet_utf16)
	Collation_utf16_czech_ci              = newCollation("utf16_czech_ci", CharacterSet_utf16)
	Collation_utf16_danish_ci             = newCollation("utf16_danish_ci", CharacterSet_utf16)
	Collation_utf16_lithuanian_ci         = newCollation("utf16_lithuanian_ci", CharacterSet_utf16)
	Collation_utf16_slovak_ci             = newCollation("utf16_slovak_ci", CharacterSet_utf16)
	Collation_utf16_spanish2_ci           = newCollation("utf16_spanish2_ci", CharacterSet_utf16)
	Collation_utf16_roman_ci              = newCollation("utf16_roman_ci", CharacterSet_utf16)
	Collation_utf16_persian_ci            = newCollation("utf16_persian_ci", CharacterSet_utf16)
	Collation_utf16_esperanto_ci          = newCollation("utf16_esperanto_ci", CharacterSet_utf16)
	Collation_utf16_hungarian_ci          = newCollation("utf16_hungarian_ci", CharacterSet_utf16)
	Collation_utf16_sinhala_ci            = newCollation("utf16_sinhala_ci", CharacterSet_utf16)
	Collation_utf16_german2_ci            = newCollation("utf16_german2_ci", CharacterSet_utf16)
	Collation_utf16_croatian_ci           = newCollation("utf16_croatian_ci", CharacterSet_utf16)
	Collation_utf16_unicode_520_ci        = newCollation("utf16_unicode_520_ci", CharacterSet_utf16)
	Collation_utf16_vietnamese_ci         = newCollation("utf16_vietnamese_ci", CharacterSet_utf16)
	Collation_utf16le_general_ci          = newCollation("utf16le_general_ci", CharacterSet_utf16le)
	Collation_utf16le_bin                 = newCollation("utf16le_bin", CharacterSet_utf16le)
	Collation_utf32_general_ci            = newCollation("utf32_general_ci", CharacterSet_utf32)
	Collation_utf32_bin                   = newCollation("utf32_bin", CharacterSet_utf32)
	Collation_utf32_unicode_ci            = newCollation("utf32_unicode_ci", CharacterSet_utf32)
	Collation_utf32_icelandic_ci          = newCollation("utf32_icelandic_ci", CharacterSet_utf32)
	Collation_utf32_latvian_ci            = newCollation("utf32_latvian_ci", CharacterSet_utf32)
	Collation_utf32_romanian_ci           = newCollation("utf32_romanian_ci", CharacterSet_utf32)
	Collation_utf32_slovenian_ci          = newCollation("utf32_slovenian_ci", CharacterSet_utf32)
	Collation_utf32_polish_ci             = newCollation("utf32_polish_ci", CharacterSet_utf32)
	Collation_utf32_estonian_ci           = newCollation("utf32_estonian_ci", CharacterSet_utf32)
	Collation_utf32_spanish_ci            = newCollation("utf32_spanish_ci", CharacterSet_utf32)
	Collation_utf32_swedish_ci            = newCollation("utf32_swedish_ci", CharacterSet_utf32)
	Collation_utf32_turkish_ci            = newCollation("utf32_turkish_ci", CharacterSet_utf32)
	Collation_utf32_czech_ci              = newCollation("utf32_czech_ci", CharacterSet_utf32)
	Collation_utf32_danish_ci             = newCollation("utf32_danish_ci", CharacterSet_utf32)
	Collation_utf32_lithuanian_ci         = newCollation("utf32_lithuanian_ci", CharacterSet_utf32)
	Collation_utf32_slovak_ci             = newCollation("utf32_slovak_ci", CharacterSet_utf32)
	Collation_utf32_spanish2_ci           = newCollation("utf32_spanish2_ci", CharacterSet_utf32)
	Collation_utf32_roman_ci              = newCollation("utf32_roman_ci", CharacterSet_utf32)
	Collation_utf32_persian_ci            = newCollation("utf32_persian_ci", CharacterSet_utf32)
	Collation_utf32_esperanto_ci          = newCollation("utf32_esperanto_ci", CharacterSet_utf32)
	Collation_utf32_hungarian_ci          = newCollation("utf32_hungarian_ci", CharacterSet_utf32)
	Collation_utf32_sinhala_ci            = newCollation("utf32_sinhala_ci", CharacterSet_utf32)
	Collation_utf32_german2_ci            = newCollation("utf32_german2_ci", CharacterSet_utf32)
	Collation_utf32_croatian_ci           = newCollation("utf32_croatian_ci", CharacterSet_utf32)
	Collation_utf32_unicode_520_ci        = newCollation("utf32_unicode_520_ci", CharacterSet_utf32)
	Collation_utf32_vietnamese_ci         = newCollation("utf32_vietnamese_ci", CharacterSet_utf32)
	Collation_utf8mb3_general_ci          = newCollation("utf8mb3_general_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_tolower_ci          = newCollation("utf8mb3_tolower_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_bin                 = newCollation("utf8mb3_bin", CharacterSet_utf8mb3)
	Collation_utf8mb3_unicode_ci          = newCollation("utf8mb3_unicode_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_icelandic_ci        = newCollation("utf8mb3_icelandic_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_latvian_ci          = newCollation("utf8mb3_latvian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_romanian_ci         = newCollation("utf8mb3_romanian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_slovenian_ci        = newCollation("utf8mb3_slovenian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_polish_ci           = newCollation("utf8mb3_polish_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_estonian_ci         = newCollation("utf8mb3_estonian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_spanish_ci          = newCollation("utf8mb3_spanish_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_swedish_ci          = newCollation("utf8mb3_swedish_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_turkish_ci          = newCollation("utf8mb3_turkish_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_czech_ci            = newCollation("utf8mb3_czech_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_danish_ci           = newCollation("utf8mb3_danish_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_lithuanian_ci       = newCollation("utf8mb3_lithuanian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_slovak_ci           = newCollation("utf8mb3_slovak_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_spanish2_ci         = newCollation("utf8mb3_spanish2_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_roman_ci            = newCollation("utf8mb3_roman_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_persian_ci          = newCollation("utf8mb3_persian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_esperanto_ci        = newCollation("utf8mb3_esperanto_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_hungarian_ci        = newCollation("utf8mb3_hungarian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_sinhala_ci          = newCollation("utf8mb3_sinhala_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_german2_ci          = newCollation("utf8mb3_german2_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_croatian_ci         = newCollation("utf8mb3_croatian_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_unicode_520_ci      = newCollation("utf8mb3_unicode_520_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_vietnamese_ci       = newCollation("utf8mb3_vietnamese_ci", CharacterSet_utf8mb3)
	Collation_utf8mb3_general_mysql500_ci = newCollation("utf8mb3_general_mysql500_ci", CharacterSet_utf8mb3)
	Collation_utf8mb4_general_ci          = newCollation("utf8mb4_general_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_bin                 = newCollation("utf8mb4_bin", CharacterSet_utf8mb4)
	Collation_utf8mb4_unicode_ci          = newCollation("utf8mb4_unicode_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_icelandic_ci        = newCollation("utf8mb4_icelandic_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_latvian_ci          = newCollation("utf8mb4_latvian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_romanian_ci         = newCollation("utf8mb4_romanian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_slovenian_ci        = newCollation("utf8mb4_slovenian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_polish_ci           = newCollation("utf8mb4_polish_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_estonian_ci         = newCollation("utf8mb4_estonian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_spanish_ci          = newCollation("utf8mb4_spanish_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_swedish_ci          = newCollation("utf8mb4_swedish_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_turkish_ci          = newCollation("utf8mb4_turkish_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_czech_ci            = newCollation("utf8mb4_czech_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_danish_ci           = newCollation("utf8mb4_danish_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_lithuanian_ci       = newCollation("utf8mb4_lithuanian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_slovak_ci           = newCollation("utf8mb4_slovak_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_spanish2_ci         = newCollation("utf8mb4_spanish2_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_roman_ci            = newCollation("utf8mb4_roman_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_persian_ci          = newCollation("utf8mb4_persian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_esperanto_ci        = newCollation("utf8mb4_esperanto_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_hungarian_ci        = newCollation("utf8mb4_hungarian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_sinhala_ci          = newCollation("utf8mb4_sinhala_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_german2_ci          = newCollation("utf8mb4_german2_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_croatian_ci         = newCollation("utf8mb4_croatian_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_unicode_520_ci      = newCollation("utf8mb4_unicode_520_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_vietnamese_ci       = newCollation("utf8mb4_vietnamese_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_0900_ai_ci          = newCollation("utf8mb4_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_de_pb_0900_ai_ci    = newCollation("utf8mb4_de_pb_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_is_0900_ai_ci       = newCollation("utf8mb4_is_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_lv_0900_ai_ci       = newCollation("utf8mb4_lv_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_ro_0900_ai_ci       = newCollation("utf8mb4_ro_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_sl_0900_ai_ci       = newCollation("utf8mb4_sl_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_pl_0900_ai_ci       = newCollation("utf8mb4_pl_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_et_0900_ai_ci       = newCollation("utf8mb4_et_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_es_0900_ai_ci       = newCollation("utf8mb4_es_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_sv_0900_ai_ci       = newCollation("utf8mb4_sv_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_tr_0900_ai_ci       = newCollation("utf8mb4_tr_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_cs_0900_ai_ci       = newCollation("utf8mb4_cs_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_da_0900_ai_ci       = newCollation("utf8mb4_da_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_lt_0900_ai_ci       = newCollation("utf8mb4_lt_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_sk_0900_ai_ci       = newCollation("utf8mb4_sk_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_es_trad_0900_ai_ci  = newCollation("utf8mb4_es_trad_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_la_0900_ai_ci       = newCollation("utf8mb4_la_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_eo_0900_ai_ci       = newCollation("utf8mb4_eo_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_hu_0900_ai_ci       = newCollation("utf8mb4_hu_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_hr_0900_ai_ci       = newCollation("utf8mb4_hr_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_vi_0900_ai_ci       = newCollation("utf8mb4_vi_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_0900_as_cs          = newCollation("utf8mb4_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_de_pb_0900_as_cs    = newCollation("utf8mb4_de_pb_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_is_0900_as_cs       = newCollation("utf8mb4_is_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_lv_0900_as_cs       = newCollation("utf8mb4_lv_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_ro_0900_as_cs       = newCollation("utf8mb4_ro_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_sl_0900_as_cs       = newCollation("utf8mb4_sl_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_pl_0900_as_cs       = newCollation("utf8mb4_pl_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_et_0900_as_cs       = newCollation("utf8mb4_et_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_es_0900_as_cs       = newCollation("utf8mb4_es_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_sv_0900_as_cs       = newCollation("utf8mb4_sv_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_tr_0900_as_cs       = newCollation("utf8mb4_tr_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_cs_0900_as_cs       = newCollation("utf8mb4_cs_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_da_0900_as_cs       = newCollation("utf8mb4_da_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_lt_0900_as_cs       = newCollation("utf8mb4_lt_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_sk_0900_as_cs       = newCollation("utf8mb4_sk_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_es_trad_0900_as_cs  = newCollation("utf8mb4_es_trad_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_la_0900_as_cs       = newCollation("utf8mb4_la_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_eo_0900_as_cs       = newCollation("utf8mb4_eo_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_hu_0900_as_cs       = newCollation("utf8mb4_hu_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_hr_0900_as_cs       = newCollation("utf8mb4_hr_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_vi_0900_as_cs       = newCollation("utf8mb4_vi_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_ja_0900_as_cs       = newCollation("utf8mb4_ja_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_ja_0900_as_cs_ks    = newCollation("utf8mb4_ja_0900_as_cs_ks", CharacterSet_utf8mb4)
	Collation_utf8mb4_0900_as_ci          = newCollation("utf8mb4_0900_as_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_ru_0900_ai_ci       = newCollation("utf8mb4_ru_0900_ai_ci", CharacterSet_utf8mb4)
	Collation_utf8mb4_ru_0900_as_cs       = newCollation("utf8mb4_ru_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_zh_0900_as_cs       = newCollation("utf8mb4_zh_0900_as_cs", CharacterSet_utf8mb4)
	Collation_utf8mb4_0900_bin            = newCollation("utf8mb4_0900_bin", CharacterSet_utf8mb4)
	Collation_utf8_general_ci             = newCollation("utf8_general_ci", CharacterSet_utf8)

	Collation_utf8_tolower_ci          = Collation_utf8mb3_tolower_ci
	Collation_utf8_bin                 = Collation_utf8mb3_bin
	Collation_utf8_unicode_ci          = Collation_utf8mb3_unicode_ci
	Collation_utf8_icelandic_ci        = Collation_utf8mb3_icelandic_ci
	Collation_utf8_latvian_ci          = Collation_utf8mb3_latvian_ci
	Collation_utf8_romanian_ci         = Collation_utf8mb3_romanian_ci
	Collation_utf8_slovenian_ci        = Collation_utf8mb3_slovenian_ci
	Collation_utf8_polish_ci           = Collation_utf8mb3_polish_ci
	Collation_utf8_estonian_ci         = Collation_utf8mb3_estonian_ci
	Collation_utf8_spanish_ci          = Collation_utf8mb3_spanish_ci
	Collation_utf8_swedish_ci          = Collation_utf8mb3_swedish_ci
	Collation_utf8_turkish_ci          = Collation_utf8mb3_turkish_ci
	Collation_utf8_czech_ci            = Collation_utf8mb3_czech_ci
	Collation_utf8_danish_ci           = Collation_utf8mb3_danish_ci
	Collation_utf8_lithuanian_ci       = Collation_utf8mb3_lithuanian_ci
	Collation_utf8_slovak_ci           = Collation_utf8mb3_slovak_ci
	Collation_utf8_spanish2_ci         = Collation_utf8mb3_spanish2_ci
	Collation_utf8_roman_ci            = Collation_utf8mb3_roman_ci
	Collation_utf8_persian_ci          = Collation_utf8mb3_persian_ci
	Collation_utf8_esperanto_ci        = Collation_utf8mb3_esperanto_ci
	Collation_utf8_hungarian_ci        = Collation_utf8mb3_hungarian_ci
	Collation_utf8_sinhala_ci          = Collation_utf8mb3_sinhala_ci
	Collation_utf8_german2_ci          = Collation_utf8mb3_german2_ci
	Collation_utf8_croatian_ci         = Collation_utf8mb3_croatian_ci
	Collation_utf8_unicode_520_ci      = Collation_utf8mb3_unicode_520_ci
	Collation_utf8_vietnamese_ci       = Collation_utf8mb3_vietnamese_ci
	Collation_utf8_general_mysql500_ci = Collation_utf8mb3_general_mysql500_ci

	Collation_Default = Collation_utf8mb4_0900_ai_ci
)

var characterSets = map[string]CharacterSet{
	string(CharacterSet_armscii8): CharacterSet_armscii8,
	string(CharacterSet_ascii):    CharacterSet_ascii,
	string(CharacterSet_big5):     CharacterSet_big5,
	string(CharacterSet_binary):   CharacterSet_binary,
	string(CharacterSet_cp1250):   CharacterSet_cp1250,
	string(CharacterSet_cp1251):   CharacterSet_cp1251,
	string(CharacterSet_cp1256):   CharacterSet_cp1256,
	string(CharacterSet_cp1257):   CharacterSet_cp1257,
	string(CharacterSet_cp850):    CharacterSet_cp850,
	string(CharacterSet_cp852):    CharacterSet_cp852,
	string(CharacterSet_cp866):    CharacterSet_cp866,
	string(CharacterSet_cp932):    CharacterSet_cp932,
	string(CharacterSet_dec8):     CharacterSet_dec8,
	string(CharacterSet_eucjpms):  CharacterSet_eucjpms,
	string(CharacterSet_euckr):    CharacterSet_euckr,
	string(CharacterSet_gb18030):  CharacterSet_gb18030,
	string(CharacterSet_gb2312):   CharacterSet_gb2312,
	string(CharacterSet_gbk):      CharacterSet_gbk,
	string(CharacterSet_geostd8):  CharacterSet_geostd8,
	string(CharacterSet_greek):    CharacterSet_greek,
	string(CharacterSet_hebrew):   CharacterSet_hebrew,
	string(CharacterSet_hp8):      CharacterSet_hp8,
	string(CharacterSet_keybcs2):  CharacterSet_keybcs2,
	string(CharacterSet_koi8r):    CharacterSet_koi8r,
	string(CharacterSet_koi8u):    CharacterSet_koi8u,
	string(CharacterSet_latin1):   CharacterSet_latin1,
	string(CharacterSet_latin2):   CharacterSet_latin2,
	string(CharacterSet_latin5):   CharacterSet_latin5,
	string(CharacterSet_latin7):   CharacterSet_latin7,
	string(CharacterSet_macce):    CharacterSet_macce,
	string(CharacterSet_macroman): CharacterSet_macroman,
	string(CharacterSet_sjis):     CharacterSet_sjis,
	string(CharacterSet_swe7):     CharacterSet_swe7,
	string(CharacterSet_tis620):   CharacterSet_tis620,
	string(CharacterSet_ucs2):     CharacterSet_ucs2,
	string(CharacterSet_ujis):     CharacterSet_ujis,
	string(CharacterSet_utf16):    CharacterSet_utf16,
	string(CharacterSet_utf16le):  CharacterSet_utf16le,
	string(CharacterSet_utf32):    CharacterSet_utf32,
	"utf8":                        CharacterSet_utf8mb3,
	string(CharacterSet_utf8mb3):  CharacterSet_utf8mb3,
	string(CharacterSet_utf8mb4):  CharacterSet_utf8mb4,
}

func init() {
	Collations["utf8_tolower_ci"] = Collation_utf8mb3_tolower_ci
	Collations["utf8_bin"] = Collation_utf8mb3_bin
	Collations["utf8_unicode_ci"] = Collation_utf8mb3_unicode_ci
	Collations["utf8_icelandic_ci"] = Collation_utf8mb3_icelandic_ci
	Collations["utf8_latvian_ci"] = Collation_utf8mb3_latvian_ci
	Collations["utf8_romanian_ci"] = Collation_utf8mb3_romanian_ci
	Collations["utf8_slovenian_ci"] = Collation_utf8mb3_slovenian_ci
	Collations["utf8_polish_ci"] = Collation_utf8mb3_polish_ci
	Collations["utf8_estonian_ci"] = Collation_utf8mb3_estonian_ci
	Collations["utf8_spanish_ci"] = Collation_utf8mb3_spanish_ci
	Collations["utf8_swedish_ci"] = Collation_utf8mb3_swedish_ci
	Collations["utf8_turkish_ci"] = Collation_utf8mb3_turkish_ci
	Collations["utf8_czech_ci"] = Collation_utf8mb3_czech_ci
	Collations["utf8_danish_ci"] = Collation_utf8mb3_danish_ci
	Collations["utf8_lithuanian_ci"] = Collation_utf8mb3_lithuanian_ci
	Collations["utf8_slovak_ci"] = Collation_utf8mb3_slovak_ci
	Collations["utf8_spanish2_ci"] = Collation_utf8mb3_spanish2_ci
	Collations["utf8_roman_ci"] = Collation_utf8mb3_roman_ci
	Collations["utf8_persian_ci"] = Collation_utf8mb3_persian_ci
	Collations["utf8_esperanto_ci"] = Collation_utf8mb3_esperanto_ci
	Collations["utf8_hungarian_ci"] = Collation_utf8mb3_hungarian_ci
	Collations["utf8_sinhala_ci"] = Collation_utf8mb3_sinhala_ci
	Collations["utf8_german2_ci"] = Collation_utf8mb3_german2_ci
	Collations["utf8_croatian_ci"] = Collation_utf8mb3_croatian_ci
	Collations["utf8_unicode_520_ci"] = Collation_utf8mb3_unicode_520_ci
	Collations["utf8_vietnamese_ci"] = Collation_utf8mb3_vietnamese_ci
	Collations["utf8_general_mysql500_ci"] = Collation_utf8mb3_general_mysql500_ci
}

var characterSetDefaults = map[CharacterSet]Collation{
	CharacterSet_armscii8: Collation_armscii8_general_ci,
	CharacterSet_ascii:    Collation_ascii_general_ci,
	CharacterSet_big5:     Collation_big5_chinese_ci,
	CharacterSet_binary:   Collation_binary,
	CharacterSet_cp1250:   Collation_cp1250_general_ci,
	CharacterSet_cp1251:   Collation_cp1251_general_ci,
	CharacterSet_cp1256:   Collation_cp1256_general_ci,
	CharacterSet_cp1257:   Collation_cp1257_general_ci,
	CharacterSet_cp850:    Collation_cp850_general_ci,
	CharacterSet_cp852:    Collation_cp852_general_ci,
	CharacterSet_cp866:    Collation_cp866_general_ci,
	CharacterSet_cp932:    Collation_cp932_japanese_ci,
	CharacterSet_dec8:     Collation_dec8_swedish_ci,
	CharacterSet_eucjpms:  Collation_eucjpms_japanese_ci,
	CharacterSet_euckr:    Collation_euckr_korean_ci,
	CharacterSet_gb18030:  Collation_gb18030_chinese_ci,
	CharacterSet_gb2312:   Collation_gb2312_chinese_ci,
	CharacterSet_gbk:      Collation_gbk_chinese_ci,
	CharacterSet_geostd8:  Collation_geostd8_general_ci,
	CharacterSet_greek:    Collation_greek_general_ci,
	CharacterSet_hebrew:   Collation_hebrew_general_ci,
	CharacterSet_hp8:      Collation_hp8_english_ci,
	CharacterSet_keybcs2:  Collation_keybcs2_general_ci,
	CharacterSet_koi8r:    Collation_koi8r_general_ci,
	CharacterSet_koi8u:    Collation_koi8u_general_ci,
	CharacterSet_latin1:   Collation_latin1_swedish_ci,
	CharacterSet_latin2:   Collation_latin2_general_ci,
	CharacterSet_latin5:   Collation_latin5_turkish_ci,
	CharacterSet_latin7:   Collation_latin7_general_ci,
	CharacterSet_macce:    Collation_macce_general_ci,
	CharacterSet_macroman: Collation_macroman_general_ci,
	CharacterSet_sjis:     Collation_sjis_japanese_ci,
	CharacterSet_swe7:     Collation_swe7_swedish_ci,
	CharacterSet_tis620:   Collation_tis620_thai_ci,
	CharacterSet_ucs2:     Collation_ucs2_general_ci,
	CharacterSet_ujis:     Collation_ujis_japanese_ci,
	CharacterSet_utf16:    Collation_utf16_general_ci,
	CharacterSet_utf16le:  Collation_utf16le_general_ci,
	CharacterSet_utf32:    Collation_utf32_general_ci,
	CharacterSet_utf8mb3:  Collation_utf8mb3_general_ci,
	CharacterSet_utf8mb4:  Collation_utf8mb4_0900_ai_ci,
	CharacterSet_utf8:     Collation_utf8_general_ci,
}

var characterSetDefaultBinaryColl = map[CharacterSet]Collation{
	CharacterSet_armscii8: Collation_armscii8_bin,
	CharacterSet_ascii:    Collation_ascii_bin,
	CharacterSet_big5:     Collation_big5_bin,
	CharacterSet_binary:   Collation_binary,
	CharacterSet_cp1250:   Collation_cp1250_bin,
	CharacterSet_cp1251:   Collation_cp1251_bin,
	CharacterSet_cp1256:   Collation_cp1256_bin,
	CharacterSet_cp1257:   Collation_cp1257_bin,
	CharacterSet_cp850:    Collation_cp850_bin,
	CharacterSet_cp852:    Collation_cp852_bin,
	CharacterSet_cp866:    Collation_cp866_bin,
	CharacterSet_cp932:    Collation_cp932_bin,
	CharacterSet_dec8:     Collation_dec8_bin,
	CharacterSet_eucjpms:  Collation_eucjpms_bin,
	CharacterSet_euckr:    Collation_euckr_bin,
	CharacterSet_gb18030:  Collation_gb18030_bin,
	CharacterSet_gb2312:   Collation_gb2312_bin,
	CharacterSet_gbk:      Collation_gbk_bin,
	CharacterSet_geostd8:  Collation_geostd8_bin,
	CharacterSet_greek:    Collation_greek_bin,
	CharacterSet_hebrew:   Collation_hebrew_bin,
	CharacterSet_hp8:      Collation_hp8_bin,
	CharacterSet_keybcs2:  Collation_keybcs2_bin,
	CharacterSet_koi8r:    Collation_koi8r_bin,
	CharacterSet_koi8u:    Collation_koi8u_bin,
	CharacterSet_latin1:   Collation_latin1_bin,
	CharacterSet_latin2:   Collation_latin2_bin,
	CharacterSet_latin5:   Collation_latin5_bin,
	CharacterSet_latin7:   Collation_latin7_bin,
	CharacterSet_macce:    Collation_macce_bin,
	CharacterSet_macroman: Collation_macroman_bin,
	CharacterSet_sjis:     Collation_sjis_bin,
	CharacterSet_swe7:     Collation_swe7_bin,
	CharacterSet_tis620:   Collation_tis620_bin,
	CharacterSet_ucs2:     Collation_ucs2_bin,
	CharacterSet_ujis:     Collation_ujis_bin,
	CharacterSet_utf16:    Collation_utf16_bin,
	CharacterSet_utf16le:  Collation_utf16le_bin,
	CharacterSet_utf32:    Collation_utf32_bin,
	CharacterSet_utf8mb3:  Collation_utf8mb3_bin,
	CharacterSet_utf8mb4:  Collation_utf8mb4_bin,
}

var characterSetDescriptions = map[CharacterSet]string{
	CharacterSet_armscii8: "ARMSCII-8 Armenian",
	CharacterSet_ascii:    "US ASCII",
	CharacterSet_big5:     "Big5 Traditional Chinese",
	CharacterSet_binary:   "Binary pseudo charset",
	CharacterSet_cp1250:   "Windows Central European",
	CharacterSet_cp1251:   "Windows Cyrillic",
	CharacterSet_cp1256:   "Windows Arabic",
	CharacterSet_cp1257:   "Windows Baltic",
	CharacterSet_cp850:    "DOS West European",
	CharacterSet_cp852:    "DOS Central European",
	CharacterSet_cp866:    "DOS Russian",
	CharacterSet_cp932:    "SJIS for Windows Japanese",
	CharacterSet_dec8:     "DEC West European",
	CharacterSet_eucjpms:  "UJIS for Windows Japanese",
	CharacterSet_euckr:    "EUC-KR Korean",
	CharacterSet_gb18030:  "China National Standard GB18030",
	CharacterSet_gb2312:   "GB2312 Simplified Chinese",
	CharacterSet_gbk:      "GBK Simplified Chinese",
	CharacterSet_geostd8:  "GEOSTD8 Georgian",
	CharacterSet_greek:    "ISO 8859-7 Greek",
	CharacterSet_hebrew:   "ISO 8859-8 Hebrew",
	CharacterSet_hp8:      "HP West European",
	CharacterSet_keybcs2:  "DOS Kamenicky Czech-Slovak",
	CharacterSet_koi8r:    "KOI8-R Relcom Russian",
	CharacterSet_koi8u:    "KOI8-U Ukrainian",
	CharacterSet_latin1:   "cp1252 West European",
	CharacterSet_latin2:   "ISO 8859-2 Central European",
	CharacterSet_latin5:   "ISO 8859-9 Turkish",
	CharacterSet_latin7:   "ISO 8859-13 Baltic",
	CharacterSet_macce:    "Mac Central European",
	CharacterSet_macroman: "Mac West European",
	CharacterSet_sjis:     "Shift-JIS Japanese",
	CharacterSet_swe7:     "7bit Swedish",
	CharacterSet_tis620:   "TIS620 Thai",
	CharacterSet_ucs2:     "UCS-2 Unicode",
	CharacterSet_ujis:     "EUC-JP Japanese",
	CharacterSet_utf16:    "UTF-16 Unicode",
	CharacterSet_utf16le:  "UTF-16LE Unicode",
	CharacterSet_utf32:    "UTF-32 Unicode",
	CharacterSet_utf8:     "UTF-8 Unicode",
	CharacterSet_utf8mb3:  "UTF-8 Unicode",
	CharacterSet_utf8mb4:  "UTF-8 Unicode",
}

var characterSetMaxLengths = map[CharacterSet]int64{
	CharacterSet_armscii8: 1,
	CharacterSet_ascii:    1,
	CharacterSet_big5:     2,
	CharacterSet_binary:   1,
	CharacterSet_cp1250:   1,
	CharacterSet_cp1251:   1,
	CharacterSet_cp1256:   1,
	CharacterSet_cp1257:   1,
	CharacterSet_cp850:    1,
	CharacterSet_cp852:    1,
	CharacterSet_cp866:    1,
	CharacterSet_cp932:    2,
	CharacterSet_dec8:     1,
	CharacterSet_eucjpms:  3,
	CharacterSet_euckr:    2,
	CharacterSet_gb18030:  4,
	CharacterSet_gb2312:   2,
	CharacterSet_gbk:      2,
	CharacterSet_geostd8:  1,
	CharacterSet_greek:    1,
	CharacterSet_hebrew:   1,
	CharacterSet_hp8:      1,
	CharacterSet_keybcs2:  1,
	CharacterSet_koi8r:    1,
	CharacterSet_koi8u:    1,
	CharacterSet_latin1:   1,
	CharacterSet_latin2:   1,
	CharacterSet_latin5:   1,
	CharacterSet_latin7:   1,
	CharacterSet_macce:    1,
	CharacterSet_macroman: 1,
	CharacterSet_sjis:     2,
	CharacterSet_swe7:     1,
	CharacterSet_tis620:   1,
	CharacterSet_ucs2:     2,
	CharacterSet_ujis:     3,
	CharacterSet_utf16:    4,
	CharacterSet_utf16le:  4,
	CharacterSet_utf32:    4,
	CharacterSet_utf8mb3:  3,
	CharacterSet_utf8mb4:  4,
	CharacterSet_utf8:     3,
}

var ErrCharacterSetNotSupported = errors.NewKind("Unknown character set: %v")
var ErrCollationNotSupported = errors.NewKind("Unknown collation: %v")

const (
	Y        = "Yes"
	N        = "No"
	NoPad    = "NO PAD"
	PadSpace = "PAD SPACE"
)

type mysqlCollationRow struct {
	ID         int64
	IsDefault  string
	IsCompiled string
	SortLen    int64
	PadSpace   string
}

var CollationToMySQLVals = map[string]mysqlCollationRow{
	Collation_binary.Name:             {63, Y, Y, 0, NoPad},
	Collation_utf8_general_ci.Name:    {33, Y, Y, 1, PadSpace},
	Collation_utf8mb4_0900_ai_ci.Name: {255, Y, Y, 0, NoPad},
}

var SupportedCharsets = []CharacterSet{
	CharacterSet_utf8mb4,
}

// ParseCharacterSet takes in a string representing a CharacterSet and
// returns the result if a match is found, or an error if not.
func ParseCharacterSet(str string) (CharacterSet, error) {
	if cs, ok := characterSets[str]; ok {
		return cs, nil
	}
	return Collation_Default.CharacterSet(), ErrCharacterSetNotSupported.New(str)
}

// ParseCollation takes in an optional character set and collation, along with the binary attribute if present,
// and returns a valid collation or error. A nil character set and collation will return the default collation.
func ParseCollation(characterSetStr *string, collationStr *string, binaryAttribute bool) (Collation, error) {
	if characterSetStr == nil || len(*characterSetStr) == 0 {
		if collationStr == nil || len(*collationStr) == 0 {
			if binaryAttribute {
				return Collation_Default.CharacterSet().BinaryCollation(), nil
			}
			return Collation_Default, nil
		}
		if collation, ok := Collations[*collationStr]; ok {
			if binaryAttribute {
				return collation.CharacterSet().BinaryCollation(), nil
			}
			return collation, nil
		}
		return Collation_Default, ErrCollationNotSupported.New(*collationStr)
	} else {
		characterSet, err := ParseCharacterSet(*characterSetStr)
		if err != nil {
			return Collation_Default, err
		}
		if collationStr == nil || len(*collationStr) == 0 {
			if binaryAttribute {
				return characterSet.BinaryCollation(), nil
			}
			return characterSet.DefaultCollation(), nil
		}
		collation, exists := Collations[*collationStr]
		if !exists {
			return Collation_Default, ErrCollationNotSupported.New(*collationStr)
		}
		if !collation.WorksWithCharacterSet(characterSet) {
			return Collation_Default, fmt.Errorf("%v is not a valid character set for %v", characterSet, collation)
		}
		return collation, nil
	}
}

// DefaultCollation returns the default Collation for this CharacterSet.
func (cs CharacterSet) DefaultCollation() Collation {
	collation, ok := characterSetDefaults[cs]
	if !ok {
		panic(fmt.Sprintf("%v does not have a default collation set", cs))
	}
	return collation
}

func (cs CharacterSet) BinaryCollation() Collation {
	collation, ok := characterSetDefaultBinaryColl[cs]
	if !ok {
		panic(fmt.Sprintf("%v does not have a default binary collation set", cs))
	}
	return collation
}

// Description returns the plain-English description for the CharacterSet.
func (cs CharacterSet) Description() string {
	str, ok := characterSetDescriptions[cs]
	if !ok {
		panic(fmt.Sprintf("%v does not have a description set", cs))
	}
	return str
}

// MaxLength returns the maximum size of a single character in the CharacterSet.
func (cs CharacterSet) MaxLength() int64 {
	length, ok := characterSetMaxLengths[cs]
	if !ok {
		panic(fmt.Sprintf("%v does not have a maximum length set", cs))
	}
	return length
}

// String returns the string representation of the CharacterSet.
func (cs CharacterSet) String() string {
	return string(cs)
}

// CharacterSet returns the CharacterSet belonging to this Collation.
func (c Collation) CharacterSet() CharacterSet {
	return c.CharSet
}

// WorksWithCharacterSet returns whether the Collation is valid for the given CharacterSet.
func (c Collation) WorksWithCharacterSet(cs CharacterSet) bool {
	return c.CharacterSet() == cs
}

// String returns the string representation of the Collation.
func (c Collation) String() string {
	return c.Name
}

// ID returns the id of the Collation.
func (c Collation) ID() int64 {
	s, ok := CollationToMySQLVals[c.Name]
	if !ok {
		s := CollationToMySQLVals[Collation_Default.Name]
		return s.ID
	}
	return s.ID
}

// IsDefault returns string specifying id collation is default.
func (c Collation) IsDefault() string {
	s, ok := CollationToMySQLVals[c.Name]
	if !ok {
		return Y
	}
	return s.IsDefault
}

// IsCompiled returns string specifying id collation is compiled.
func (c Collation) IsCompiled() string {
	s, ok := CollationToMySQLVals[c.Name]
	if !ok {
		return Y
	}
	return s.IsCompiled
}

// SortLen returns sort len of the collation.
func (c Collation) SortLen() int64 {
	s, ok := CollationToMySQLVals[c.Name]
	if !ok {
		return 1
	}
	return s.SortLen
}

// PadSpace returns pad space of the collation.
func (c Collation) PadSpace() string {
	s, ok := CollationToMySQLVals[c.Name]
	if !ok {
		return PadSpace
	}
	return s.PadSpace
}

// Equals returns true if two collations are equal, false otherwise
func (c Collation) Equals(other Collation) bool {
	return c.Name == other.Name
}
