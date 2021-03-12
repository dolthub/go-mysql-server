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

	"gopkg.in/src-d/go-errors.v1"
)

// CharacterSet represents the character set of a string.
type CharacterSet string

// Collation represents the collation of a string.
type Collation string

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

	Collation_Default                               = Collation_utf8mb4_0900_ai_ci
	Collation_armscii8_general_ci         Collation = "armscii8_general_ci"
	Collation_armscii8_bin                Collation = "armscii8_bin"
	Collation_ascii_general_ci            Collation = "ascii_general_ci"
	Collation_ascii_bin                   Collation = "ascii_bin"
	Collation_big5_chinese_ci             Collation = "big5_chinese_ci"
	Collation_big5_bin                    Collation = "big5_bin"
	Collation_binary                      Collation = "binary"
	Collation_cp1250_general_ci           Collation = "cp1250_general_ci"
	Collation_cp1250_czech_cs             Collation = "cp1250_czech_cs"
	Collation_cp1250_croatian_ci          Collation = "cp1250_croatian_ci"
	Collation_cp1250_bin                  Collation = "cp1250_bin"
	Collation_cp1250_polish_ci            Collation = "cp1250_polish_ci"
	Collation_cp1251_bulgarian_ci         Collation = "cp1251_bulgarian_ci"
	Collation_cp1251_ukrainian_ci         Collation = "cp1251_ukrainian_ci"
	Collation_cp1251_bin                  Collation = "cp1251_bin"
	Collation_cp1251_general_ci           Collation = "cp1251_general_ci"
	Collation_cp1251_general_cs           Collation = "cp1251_general_cs"
	Collation_cp1256_general_ci           Collation = "cp1256_general_ci"
	Collation_cp1256_bin                  Collation = "cp1256_bin"
	Collation_cp1257_lithuanian_ci        Collation = "cp1257_lithuanian_ci"
	Collation_cp1257_bin                  Collation = "cp1257_bin"
	Collation_cp1257_general_ci           Collation = "cp1257_general_ci"
	Collation_cp850_general_ci            Collation = "cp850_general_ci"
	Collation_cp850_bin                   Collation = "cp850_bin"
	Collation_cp852_general_ci            Collation = "cp852_general_ci"
	Collation_cp852_bin                   Collation = "cp852_bin"
	Collation_cp866_general_ci            Collation = "cp866_general_ci"
	Collation_cp866_bin                   Collation = "cp866_bin"
	Collation_cp932_japanese_ci           Collation = "cp932_japanese_ci"
	Collation_cp932_bin                   Collation = "cp932_bin"
	Collation_dec8_swedish_ci             Collation = "dec8_swedish_ci"
	Collation_dec8_bin                    Collation = "dec8_bin"
	Collation_eucjpms_japanese_ci         Collation = "eucjpms_japanese_ci"
	Collation_eucjpms_bin                 Collation = "eucjpms_bin"
	Collation_euckr_korean_ci             Collation = "euckr_korean_ci"
	Collation_euckr_bin                   Collation = "euckr_bin"
	Collation_gb18030_chinese_ci          Collation = "gb18030_chinese_ci"
	Collation_gb18030_bin                 Collation = "gb18030_bin"
	Collation_gb18030_unicode_520_ci      Collation = "gb18030_unicode_520_ci"
	Collation_gb2312_chinese_ci           Collation = "gb2312_chinese_ci"
	Collation_gb2312_bin                  Collation = "gb2312_bin"
	Collation_gbk_chinese_ci              Collation = "gbk_chinese_ci"
	Collation_gbk_bin                     Collation = "gbk_bin"
	Collation_geostd8_general_ci          Collation = "geostd8_general_ci"
	Collation_geostd8_bin                 Collation = "geostd8_bin"
	Collation_greek_general_ci            Collation = "greek_general_ci"
	Collation_greek_bin                   Collation = "greek_bin"
	Collation_hebrew_general_ci           Collation = "hebrew_general_ci"
	Collation_hebrew_bin                  Collation = "hebrew_bin"
	Collation_hp8_english_ci              Collation = "hp8_english_ci"
	Collation_hp8_bin                     Collation = "hp8_bin"
	Collation_keybcs2_general_ci          Collation = "keybcs2_general_ci"
	Collation_keybcs2_bin                 Collation = "keybcs2_bin"
	Collation_koi8r_general_ci            Collation = "koi8r_general_ci"
	Collation_koi8r_bin                   Collation = "koi8r_bin"
	Collation_koi8u_general_ci            Collation = "koi8u_general_ci"
	Collation_koi8u_bin                   Collation = "koi8u_bin"
	Collation_latin1_german1_ci           Collation = "latin1_german1_ci"
	Collation_latin1_swedish_ci           Collation = "latin1_swedish_ci"
	Collation_latin1_danish_ci            Collation = "latin1_danish_ci"
	Collation_latin1_german2_ci           Collation = "latin1_german2_ci"
	Collation_latin1_bin                  Collation = "latin1_bin"
	Collation_latin1_general_ci           Collation = "latin1_general_ci"
	Collation_latin1_general_cs           Collation = "latin1_general_cs"
	Collation_latin1_spanish_ci           Collation = "latin1_spanish_ci"
	Collation_latin2_czech_cs             Collation = "latin2_czech_cs"
	Collation_latin2_general_ci           Collation = "latin2_general_ci"
	Collation_latin2_hungarian_ci         Collation = "latin2_hungarian_ci"
	Collation_latin2_croatian_ci          Collation = "latin2_croatian_ci"
	Collation_latin2_bin                  Collation = "latin2_bin"
	Collation_latin5_turkish_ci           Collation = "latin5_turkish_ci"
	Collation_latin5_bin                  Collation = "latin5_bin"
	Collation_latin7_estonian_cs          Collation = "latin7_estonian_cs"
	Collation_latin7_general_ci           Collation = "latin7_general_ci"
	Collation_latin7_general_cs           Collation = "latin7_general_cs"
	Collation_latin7_bin                  Collation = "latin7_bin"
	Collation_macce_general_ci            Collation = "macce_general_ci"
	Collation_macce_bin                   Collation = "macce_bin"
	Collation_macroman_general_ci         Collation = "macroman_general_ci"
	Collation_macroman_bin                Collation = "macroman_bin"
	Collation_sjis_japanese_ci            Collation = "sjis_japanese_ci"
	Collation_sjis_bin                    Collation = "sjis_bin"
	Collation_swe7_swedish_ci             Collation = "swe7_swedish_ci"
	Collation_swe7_bin                    Collation = "swe7_bin"
	Collation_tis620_thai_ci              Collation = "tis620_thai_ci"
	Collation_tis620_bin                  Collation = "tis620_bin"
	Collation_ucs2_general_ci             Collation = "ucs2_general_ci"
	Collation_ucs2_bin                    Collation = "ucs2_bin"
	Collation_ucs2_unicode_ci             Collation = "ucs2_unicode_ci"
	Collation_ucs2_icelandic_ci           Collation = "ucs2_icelandic_ci"
	Collation_ucs2_latvian_ci             Collation = "ucs2_latvian_ci"
	Collation_ucs2_romanian_ci            Collation = "ucs2_romanian_ci"
	Collation_ucs2_slovenian_ci           Collation = "ucs2_slovenian_ci"
	Collation_ucs2_polish_ci              Collation = "ucs2_polish_ci"
	Collation_ucs2_estonian_ci            Collation = "ucs2_estonian_ci"
	Collation_ucs2_spanish_ci             Collation = "ucs2_spanish_ci"
	Collation_ucs2_swedish_ci             Collation = "ucs2_swedish_ci"
	Collation_ucs2_turkish_ci             Collation = "ucs2_turkish_ci"
	Collation_ucs2_czech_ci               Collation = "ucs2_czech_ci"
	Collation_ucs2_danish_ci              Collation = "ucs2_danish_ci"
	Collation_ucs2_lithuanian_ci          Collation = "ucs2_lithuanian_ci"
	Collation_ucs2_slovak_ci              Collation = "ucs2_slovak_ci"
	Collation_ucs2_spanish2_ci            Collation = "ucs2_spanish2_ci"
	Collation_ucs2_roman_ci               Collation = "ucs2_roman_ci"
	Collation_ucs2_persian_ci             Collation = "ucs2_persian_ci"
	Collation_ucs2_esperanto_ci           Collation = "ucs2_esperanto_ci"
	Collation_ucs2_hungarian_ci           Collation = "ucs2_hungarian_ci"
	Collation_ucs2_sinhala_ci             Collation = "ucs2_sinhala_ci"
	Collation_ucs2_german2_ci             Collation = "ucs2_german2_ci"
	Collation_ucs2_croatian_ci            Collation = "ucs2_croatian_ci"
	Collation_ucs2_unicode_520_ci         Collation = "ucs2_unicode_520_ci"
	Collation_ucs2_vietnamese_ci          Collation = "ucs2_vietnamese_ci"
	Collation_ucs2_general_mysql500_ci    Collation = "ucs2_general_mysql500_ci"
	Collation_ujis_japanese_ci            Collation = "ujis_japanese_ci"
	Collation_ujis_bin                    Collation = "ujis_bin"
	Collation_utf16_general_ci            Collation = "utf16_general_ci"
	Collation_utf16_bin                   Collation = "utf16_bin"
	Collation_utf16_unicode_ci            Collation = "utf16_unicode_ci"
	Collation_utf16_icelandic_ci          Collation = "utf16_icelandic_ci"
	Collation_utf16_latvian_ci            Collation = "utf16_latvian_ci"
	Collation_utf16_romanian_ci           Collation = "utf16_romanian_ci"
	Collation_utf16_slovenian_ci          Collation = "utf16_slovenian_ci"
	Collation_utf16_polish_ci             Collation = "utf16_polish_ci"
	Collation_utf16_estonian_ci           Collation = "utf16_estonian_ci"
	Collation_utf16_spanish_ci            Collation = "utf16_spanish_ci"
	Collation_utf16_swedish_ci            Collation = "utf16_swedish_ci"
	Collation_utf16_turkish_ci            Collation = "utf16_turkish_ci"
	Collation_utf16_czech_ci              Collation = "utf16_czech_ci"
	Collation_utf16_danish_ci             Collation = "utf16_danish_ci"
	Collation_utf16_lithuanian_ci         Collation = "utf16_lithuanian_ci"
	Collation_utf16_slovak_ci             Collation = "utf16_slovak_ci"
	Collation_utf16_spanish2_ci           Collation = "utf16_spanish2_ci"
	Collation_utf16_roman_ci              Collation = "utf16_roman_ci"
	Collation_utf16_persian_ci            Collation = "utf16_persian_ci"
	Collation_utf16_esperanto_ci          Collation = "utf16_esperanto_ci"
	Collation_utf16_hungarian_ci          Collation = "utf16_hungarian_ci"
	Collation_utf16_sinhala_ci            Collation = "utf16_sinhala_ci"
	Collation_utf16_german2_ci            Collation = "utf16_german2_ci"
	Collation_utf16_croatian_ci           Collation = "utf16_croatian_ci"
	Collation_utf16_unicode_520_ci        Collation = "utf16_unicode_520_ci"
	Collation_utf16_vietnamese_ci         Collation = "utf16_vietnamese_ci"
	Collation_utf16le_general_ci          Collation = "utf16le_general_ci"
	Collation_utf16le_bin                 Collation = "utf16le_bin"
	Collation_utf32_general_ci            Collation = "utf32_general_ci"
	Collation_utf32_bin                   Collation = "utf32_bin"
	Collation_utf32_unicode_ci            Collation = "utf32_unicode_ci"
	Collation_utf32_icelandic_ci          Collation = "utf32_icelandic_ci"
	Collation_utf32_latvian_ci            Collation = "utf32_latvian_ci"
	Collation_utf32_romanian_ci           Collation = "utf32_romanian_ci"
	Collation_utf32_slovenian_ci          Collation = "utf32_slovenian_ci"
	Collation_utf32_polish_ci             Collation = "utf32_polish_ci"
	Collation_utf32_estonian_ci           Collation = "utf32_estonian_ci"
	Collation_utf32_spanish_ci            Collation = "utf32_spanish_ci"
	Collation_utf32_swedish_ci            Collation = "utf32_swedish_ci"
	Collation_utf32_turkish_ci            Collation = "utf32_turkish_ci"
	Collation_utf32_czech_ci              Collation = "utf32_czech_ci"
	Collation_utf32_danish_ci             Collation = "utf32_danish_ci"
	Collation_utf32_lithuanian_ci         Collation = "utf32_lithuanian_ci"
	Collation_utf32_slovak_ci             Collation = "utf32_slovak_ci"
	Collation_utf32_spanish2_ci           Collation = "utf32_spanish2_ci"
	Collation_utf32_roman_ci              Collation = "utf32_roman_ci"
	Collation_utf32_persian_ci            Collation = "utf32_persian_ci"
	Collation_utf32_esperanto_ci          Collation = "utf32_esperanto_ci"
	Collation_utf32_hungarian_ci          Collation = "utf32_hungarian_ci"
	Collation_utf32_sinhala_ci            Collation = "utf32_sinhala_ci"
	Collation_utf32_german2_ci            Collation = "utf32_german2_ci"
	Collation_utf32_croatian_ci           Collation = "utf32_croatian_ci"
	Collation_utf32_unicode_520_ci        Collation = "utf32_unicode_520_ci"
	Collation_utf32_vietnamese_ci         Collation = "utf32_vietnamese_ci"
	Collation_utf8_general_ci             Collation = "utf8_general_ci"
	Collation_utf8_tolower_ci                       = Collation_utf8mb3_tolower_ci
	Collation_utf8_bin                              = Collation_utf8mb3_bin
	Collation_utf8_unicode_ci                       = Collation_utf8mb3_unicode_ci
	Collation_utf8_icelandic_ci                     = Collation_utf8mb3_icelandic_ci
	Collation_utf8_latvian_ci                       = Collation_utf8mb3_latvian_ci
	Collation_utf8_romanian_ci                      = Collation_utf8mb3_romanian_ci
	Collation_utf8_slovenian_ci                     = Collation_utf8mb3_slovenian_ci
	Collation_utf8_polish_ci                        = Collation_utf8mb3_polish_ci
	Collation_utf8_estonian_ci                      = Collation_utf8mb3_estonian_ci
	Collation_utf8_spanish_ci                       = Collation_utf8mb3_spanish_ci
	Collation_utf8_swedish_ci                       = Collation_utf8mb3_swedish_ci
	Collation_utf8_turkish_ci                       = Collation_utf8mb3_turkish_ci
	Collation_utf8_czech_ci                         = Collation_utf8mb3_czech_ci
	Collation_utf8_danish_ci                        = Collation_utf8mb3_danish_ci
	Collation_utf8_lithuanian_ci                    = Collation_utf8mb3_lithuanian_ci
	Collation_utf8_slovak_ci                        = Collation_utf8mb3_slovak_ci
	Collation_utf8_spanish2_ci                      = Collation_utf8mb3_spanish2_ci
	Collation_utf8_roman_ci                         = Collation_utf8mb3_roman_ci
	Collation_utf8_persian_ci                       = Collation_utf8mb3_persian_ci
	Collation_utf8_esperanto_ci                     = Collation_utf8mb3_esperanto_ci
	Collation_utf8_hungarian_ci                     = Collation_utf8mb3_hungarian_ci
	Collation_utf8_sinhala_ci                       = Collation_utf8mb3_sinhala_ci
	Collation_utf8_german2_ci                       = Collation_utf8mb3_german2_ci
	Collation_utf8_croatian_ci                      = Collation_utf8mb3_croatian_ci
	Collation_utf8_unicode_520_ci                   = Collation_utf8mb3_unicode_520_ci
	Collation_utf8_vietnamese_ci                    = Collation_utf8mb3_vietnamese_ci
	Collation_utf8_general_mysql500_ci              = Collation_utf8mb3_general_mysql500_ci
	Collation_utf8mb3_general_ci          Collation = "utf8mb3_general_ci"
	Collation_utf8mb3_tolower_ci          Collation = "utf8mb3_tolower_ci"
	Collation_utf8mb3_bin                 Collation = "utf8mb3_bin"
	Collation_utf8mb3_unicode_ci          Collation = "utf8mb3_unicode_ci"
	Collation_utf8mb3_icelandic_ci        Collation = "utf8mb3_icelandic_ci"
	Collation_utf8mb3_latvian_ci          Collation = "utf8mb3_latvian_ci"
	Collation_utf8mb3_romanian_ci         Collation = "utf8mb3_romanian_ci"
	Collation_utf8mb3_slovenian_ci        Collation = "utf8mb3_slovenian_ci"
	Collation_utf8mb3_polish_ci           Collation = "utf8mb3_polish_ci"
	Collation_utf8mb3_estonian_ci         Collation = "utf8mb3_estonian_ci"
	Collation_utf8mb3_spanish_ci          Collation = "utf8mb3_spanish_ci"
	Collation_utf8mb3_swedish_ci          Collation = "utf8mb3_swedish_ci"
	Collation_utf8mb3_turkish_ci          Collation = "utf8mb3_turkish_ci"
	Collation_utf8mb3_czech_ci            Collation = "utf8mb3_czech_ci"
	Collation_utf8mb3_danish_ci           Collation = "utf8mb3_danish_ci"
	Collation_utf8mb3_lithuanian_ci       Collation = "utf8mb3_lithuanian_ci"
	Collation_utf8mb3_slovak_ci           Collation = "utf8mb3_slovak_ci"
	Collation_utf8mb3_spanish2_ci         Collation = "utf8mb3_spanish2_ci"
	Collation_utf8mb3_roman_ci            Collation = "utf8mb3_roman_ci"
	Collation_utf8mb3_persian_ci          Collation = "utf8mb3_persian_ci"
	Collation_utf8mb3_esperanto_ci        Collation = "utf8mb3_esperanto_ci"
	Collation_utf8mb3_hungarian_ci        Collation = "utf8mb3_hungarian_ci"
	Collation_utf8mb3_sinhala_ci          Collation = "utf8mb3_sinhala_ci"
	Collation_utf8mb3_german2_ci          Collation = "utf8mb3_german2_ci"
	Collation_utf8mb3_croatian_ci         Collation = "utf8mb3_croatian_ci"
	Collation_utf8mb3_unicode_520_ci      Collation = "utf8mb3_unicode_520_ci"
	Collation_utf8mb3_vietnamese_ci       Collation = "utf8mb3_vietnamese_ci"
	Collation_utf8mb3_general_mysql500_ci Collation = "utf8mb3_general_mysql500_ci"
	Collation_utf8mb4_general_ci          Collation = "utf8mb4_general_ci"
	Collation_utf8mb4_bin                 Collation = "utf8mb4_bin"
	Collation_utf8mb4_unicode_ci          Collation = "utf8mb4_unicode_ci"
	Collation_utf8mb4_icelandic_ci        Collation = "utf8mb4_icelandic_ci"
	Collation_utf8mb4_latvian_ci          Collation = "utf8mb4_latvian_ci"
	Collation_utf8mb4_romanian_ci         Collation = "utf8mb4_romanian_ci"
	Collation_utf8mb4_slovenian_ci        Collation = "utf8mb4_slovenian_ci"
	Collation_utf8mb4_polish_ci           Collation = "utf8mb4_polish_ci"
	Collation_utf8mb4_estonian_ci         Collation = "utf8mb4_estonian_ci"
	Collation_utf8mb4_spanish_ci          Collation = "utf8mb4_spanish_ci"
	Collation_utf8mb4_swedish_ci          Collation = "utf8mb4_swedish_ci"
	Collation_utf8mb4_turkish_ci          Collation = "utf8mb4_turkish_ci"
	Collation_utf8mb4_czech_ci            Collation = "utf8mb4_czech_ci"
	Collation_utf8mb4_danish_ci           Collation = "utf8mb4_danish_ci"
	Collation_utf8mb4_lithuanian_ci       Collation = "utf8mb4_lithuanian_ci"
	Collation_utf8mb4_slovak_ci           Collation = "utf8mb4_slovak_ci"
	Collation_utf8mb4_spanish2_ci         Collation = "utf8mb4_spanish2_ci"
	Collation_utf8mb4_roman_ci            Collation = "utf8mb4_roman_ci"
	Collation_utf8mb4_persian_ci          Collation = "utf8mb4_persian_ci"
	Collation_utf8mb4_esperanto_ci        Collation = "utf8mb4_esperanto_ci"
	Collation_utf8mb4_hungarian_ci        Collation = "utf8mb4_hungarian_ci"
	Collation_utf8mb4_sinhala_ci          Collation = "utf8mb4_sinhala_ci"
	Collation_utf8mb4_german2_ci          Collation = "utf8mb4_german2_ci"
	Collation_utf8mb4_croatian_ci         Collation = "utf8mb4_croatian_ci"
	Collation_utf8mb4_unicode_520_ci      Collation = "utf8mb4_unicode_520_ci"
	Collation_utf8mb4_vietnamese_ci       Collation = "utf8mb4_vietnamese_ci"
	Collation_utf8mb4_0900_ai_ci          Collation = "utf8mb4_0900_ai_ci"
	Collation_utf8mb4_de_pb_0900_ai_ci    Collation = "utf8mb4_de_pb_0900_ai_ci"
	Collation_utf8mb4_is_0900_ai_ci       Collation = "utf8mb4_is_0900_ai_ci"
	Collation_utf8mb4_lv_0900_ai_ci       Collation = "utf8mb4_lv_0900_ai_ci"
	Collation_utf8mb4_ro_0900_ai_ci       Collation = "utf8mb4_ro_0900_ai_ci"
	Collation_utf8mb4_sl_0900_ai_ci       Collation = "utf8mb4_sl_0900_ai_ci"
	Collation_utf8mb4_pl_0900_ai_ci       Collation = "utf8mb4_pl_0900_ai_ci"
	Collation_utf8mb4_et_0900_ai_ci       Collation = "utf8mb4_et_0900_ai_ci"
	Collation_utf8mb4_es_0900_ai_ci       Collation = "utf8mb4_es_0900_ai_ci"
	Collation_utf8mb4_sv_0900_ai_ci       Collation = "utf8mb4_sv_0900_ai_ci"
	Collation_utf8mb4_tr_0900_ai_ci       Collation = "utf8mb4_tr_0900_ai_ci"
	Collation_utf8mb4_cs_0900_ai_ci       Collation = "utf8mb4_cs_0900_ai_ci"
	Collation_utf8mb4_da_0900_ai_ci       Collation = "utf8mb4_da_0900_ai_ci"
	Collation_utf8mb4_lt_0900_ai_ci       Collation = "utf8mb4_lt_0900_ai_ci"
	Collation_utf8mb4_sk_0900_ai_ci       Collation = "utf8mb4_sk_0900_ai_ci"
	Collation_utf8mb4_es_trad_0900_ai_ci  Collation = "utf8mb4_es_trad_0900_ai_ci"
	Collation_utf8mb4_la_0900_ai_ci       Collation = "utf8mb4_la_0900_ai_ci"
	Collation_utf8mb4_eo_0900_ai_ci       Collation = "utf8mb4_eo_0900_ai_ci"
	Collation_utf8mb4_hu_0900_ai_ci       Collation = "utf8mb4_hu_0900_ai_ci"
	Collation_utf8mb4_hr_0900_ai_ci       Collation = "utf8mb4_hr_0900_ai_ci"
	Collation_utf8mb4_vi_0900_ai_ci       Collation = "utf8mb4_vi_0900_ai_ci"
	Collation_utf8mb4_0900_as_cs          Collation = "utf8mb4_0900_as_cs"
	Collation_utf8mb4_de_pb_0900_as_cs    Collation = "utf8mb4_de_pb_0900_as_cs"
	Collation_utf8mb4_is_0900_as_cs       Collation = "utf8mb4_is_0900_as_cs"
	Collation_utf8mb4_lv_0900_as_cs       Collation = "utf8mb4_lv_0900_as_cs"
	Collation_utf8mb4_ro_0900_as_cs       Collation = "utf8mb4_ro_0900_as_cs"
	Collation_utf8mb4_sl_0900_as_cs       Collation = "utf8mb4_sl_0900_as_cs"
	Collation_utf8mb4_pl_0900_as_cs       Collation = "utf8mb4_pl_0900_as_cs"
	Collation_utf8mb4_et_0900_as_cs       Collation = "utf8mb4_et_0900_as_cs"
	Collation_utf8mb4_es_0900_as_cs       Collation = "utf8mb4_es_0900_as_cs"
	Collation_utf8mb4_sv_0900_as_cs       Collation = "utf8mb4_sv_0900_as_cs"
	Collation_utf8mb4_tr_0900_as_cs       Collation = "utf8mb4_tr_0900_as_cs"
	Collation_utf8mb4_cs_0900_as_cs       Collation = "utf8mb4_cs_0900_as_cs"
	Collation_utf8mb4_da_0900_as_cs       Collation = "utf8mb4_da_0900_as_cs"
	Collation_utf8mb4_lt_0900_as_cs       Collation = "utf8mb4_lt_0900_as_cs"
	Collation_utf8mb4_sk_0900_as_cs       Collation = "utf8mb4_sk_0900_as_cs"
	Collation_utf8mb4_es_trad_0900_as_cs  Collation = "utf8mb4_es_trad_0900_as_cs"
	Collation_utf8mb4_la_0900_as_cs       Collation = "utf8mb4_la_0900_as_cs"
	Collation_utf8mb4_eo_0900_as_cs       Collation = "utf8mb4_eo_0900_as_cs"
	Collation_utf8mb4_hu_0900_as_cs       Collation = "utf8mb4_hu_0900_as_cs"
	Collation_utf8mb4_hr_0900_as_cs       Collation = "utf8mb4_hr_0900_as_cs"
	Collation_utf8mb4_vi_0900_as_cs       Collation = "utf8mb4_vi_0900_as_cs"
	Collation_utf8mb4_ja_0900_as_cs       Collation = "utf8mb4_ja_0900_as_cs"
	Collation_utf8mb4_ja_0900_as_cs_ks    Collation = "utf8mb4_ja_0900_as_cs_ks"
	Collation_utf8mb4_0900_as_ci          Collation = "utf8mb4_0900_as_ci"
	Collation_utf8mb4_ru_0900_ai_ci       Collation = "utf8mb4_ru_0900_ai_ci"
	Collation_utf8mb4_ru_0900_as_cs       Collation = "utf8mb4_ru_0900_as_cs"
	Collation_utf8mb4_zh_0900_as_cs       Collation = "utf8mb4_zh_0900_as_cs"
	Collation_utf8mb4_0900_bin            Collation = "utf8mb4_0900_bin"
)

var (
	characterSets = map[string]CharacterSet{
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

	collations = map[string]Collation{
		string(Collation_armscii8_general_ci):         Collation_armscii8_general_ci,
		string(Collation_armscii8_bin):                Collation_armscii8_bin,
		string(Collation_ascii_general_ci):            Collation_ascii_general_ci,
		string(Collation_ascii_bin):                   Collation_ascii_bin,
		string(Collation_big5_chinese_ci):             Collation_big5_chinese_ci,
		string(Collation_big5_bin):                    Collation_big5_bin,
		string(Collation_binary):                      Collation_binary,
		string(Collation_cp1250_general_ci):           Collation_cp1250_general_ci,
		string(Collation_cp1250_czech_cs):             Collation_cp1250_czech_cs,
		string(Collation_cp1250_croatian_ci):          Collation_cp1250_croatian_ci,
		string(Collation_cp1250_bin):                  Collation_cp1250_bin,
		string(Collation_cp1250_polish_ci):            Collation_cp1250_polish_ci,
		string(Collation_cp1251_bulgarian_ci):         Collation_cp1251_bulgarian_ci,
		string(Collation_cp1251_ukrainian_ci):         Collation_cp1251_ukrainian_ci,
		string(Collation_cp1251_bin):                  Collation_cp1251_bin,
		string(Collation_cp1251_general_ci):           Collation_cp1251_general_ci,
		string(Collation_cp1251_general_cs):           Collation_cp1251_general_cs,
		string(Collation_cp1256_general_ci):           Collation_cp1256_general_ci,
		string(Collation_cp1256_bin):                  Collation_cp1256_bin,
		string(Collation_cp1257_lithuanian_ci):        Collation_cp1257_lithuanian_ci,
		string(Collation_cp1257_bin):                  Collation_cp1257_bin,
		string(Collation_cp1257_general_ci):           Collation_cp1257_general_ci,
		string(Collation_cp850_general_ci):            Collation_cp850_general_ci,
		string(Collation_cp850_bin):                   Collation_cp850_bin,
		string(Collation_cp852_general_ci):            Collation_cp852_general_ci,
		string(Collation_cp852_bin):                   Collation_cp852_bin,
		string(Collation_cp866_general_ci):            Collation_cp866_general_ci,
		string(Collation_cp866_bin):                   Collation_cp866_bin,
		string(Collation_cp932_japanese_ci):           Collation_cp932_japanese_ci,
		string(Collation_cp932_bin):                   Collation_cp932_bin,
		string(Collation_dec8_swedish_ci):             Collation_dec8_swedish_ci,
		string(Collation_dec8_bin):                    Collation_dec8_bin,
		string(Collation_eucjpms_japanese_ci):         Collation_eucjpms_japanese_ci,
		string(Collation_eucjpms_bin):                 Collation_eucjpms_bin,
		string(Collation_euckr_korean_ci):             Collation_euckr_korean_ci,
		string(Collation_euckr_bin):                   Collation_euckr_bin,
		string(Collation_gb18030_chinese_ci):          Collation_gb18030_chinese_ci,
		string(Collation_gb18030_bin):                 Collation_gb18030_bin,
		string(Collation_gb18030_unicode_520_ci):      Collation_gb18030_unicode_520_ci,
		string(Collation_gb2312_chinese_ci):           Collation_gb2312_chinese_ci,
		string(Collation_gb2312_bin):                  Collation_gb2312_bin,
		string(Collation_gbk_chinese_ci):              Collation_gbk_chinese_ci,
		string(Collation_gbk_bin):                     Collation_gbk_bin,
		string(Collation_geostd8_general_ci):          Collation_geostd8_general_ci,
		string(Collation_geostd8_bin):                 Collation_geostd8_bin,
		string(Collation_greek_general_ci):            Collation_greek_general_ci,
		string(Collation_greek_bin):                   Collation_greek_bin,
		string(Collation_hebrew_general_ci):           Collation_hebrew_general_ci,
		string(Collation_hebrew_bin):                  Collation_hebrew_bin,
		string(Collation_hp8_english_ci):              Collation_hp8_english_ci,
		string(Collation_hp8_bin):                     Collation_hp8_bin,
		string(Collation_keybcs2_general_ci):          Collation_keybcs2_general_ci,
		string(Collation_keybcs2_bin):                 Collation_keybcs2_bin,
		string(Collation_koi8r_general_ci):            Collation_koi8r_general_ci,
		string(Collation_koi8r_bin):                   Collation_koi8r_bin,
		string(Collation_koi8u_general_ci):            Collation_koi8u_general_ci,
		string(Collation_koi8u_bin):                   Collation_koi8u_bin,
		string(Collation_latin1_german1_ci):           Collation_latin1_german1_ci,
		string(Collation_latin1_swedish_ci):           Collation_latin1_swedish_ci,
		string(Collation_latin1_danish_ci):            Collation_latin1_danish_ci,
		string(Collation_latin1_german2_ci):           Collation_latin1_german2_ci,
		string(Collation_latin1_bin):                  Collation_latin1_bin,
		string(Collation_latin1_general_ci):           Collation_latin1_general_ci,
		string(Collation_latin1_general_cs):           Collation_latin1_general_cs,
		string(Collation_latin1_spanish_ci):           Collation_latin1_spanish_ci,
		string(Collation_latin2_czech_cs):             Collation_latin2_czech_cs,
		string(Collation_latin2_general_ci):           Collation_latin2_general_ci,
		string(Collation_latin2_hungarian_ci):         Collation_latin2_hungarian_ci,
		string(Collation_latin2_croatian_ci):          Collation_latin2_croatian_ci,
		string(Collation_latin2_bin):                  Collation_latin2_bin,
		string(Collation_latin5_turkish_ci):           Collation_latin5_turkish_ci,
		string(Collation_latin5_bin):                  Collation_latin5_bin,
		string(Collation_latin7_estonian_cs):          Collation_latin7_estonian_cs,
		string(Collation_latin7_general_ci):           Collation_latin7_general_ci,
		string(Collation_latin7_general_cs):           Collation_latin7_general_cs,
		string(Collation_latin7_bin):                  Collation_latin7_bin,
		string(Collation_macce_general_ci):            Collation_macce_general_ci,
		string(Collation_macce_bin):                   Collation_macce_bin,
		string(Collation_macroman_general_ci):         Collation_macroman_general_ci,
		string(Collation_macroman_bin):                Collation_macroman_bin,
		string(Collation_sjis_japanese_ci):            Collation_sjis_japanese_ci,
		string(Collation_sjis_bin):                    Collation_sjis_bin,
		string(Collation_swe7_swedish_ci):             Collation_swe7_swedish_ci,
		string(Collation_swe7_bin):                    Collation_swe7_bin,
		string(Collation_tis620_thai_ci):              Collation_tis620_thai_ci,
		string(Collation_tis620_bin):                  Collation_tis620_bin,
		string(Collation_ucs2_general_ci):             Collation_ucs2_general_ci,
		string(Collation_ucs2_bin):                    Collation_ucs2_bin,
		string(Collation_ucs2_unicode_ci):             Collation_ucs2_unicode_ci,
		string(Collation_ucs2_icelandic_ci):           Collation_ucs2_icelandic_ci,
		string(Collation_ucs2_latvian_ci):             Collation_ucs2_latvian_ci,
		string(Collation_ucs2_romanian_ci):            Collation_ucs2_romanian_ci,
		string(Collation_ucs2_slovenian_ci):           Collation_ucs2_slovenian_ci,
		string(Collation_ucs2_polish_ci):              Collation_ucs2_polish_ci,
		string(Collation_ucs2_estonian_ci):            Collation_ucs2_estonian_ci,
		string(Collation_ucs2_spanish_ci):             Collation_ucs2_spanish_ci,
		string(Collation_ucs2_swedish_ci):             Collation_ucs2_swedish_ci,
		string(Collation_ucs2_turkish_ci):             Collation_ucs2_turkish_ci,
		string(Collation_ucs2_czech_ci):               Collation_ucs2_czech_ci,
		string(Collation_ucs2_danish_ci):              Collation_ucs2_danish_ci,
		string(Collation_ucs2_lithuanian_ci):          Collation_ucs2_lithuanian_ci,
		string(Collation_ucs2_slovak_ci):              Collation_ucs2_slovak_ci,
		string(Collation_ucs2_spanish2_ci):            Collation_ucs2_spanish2_ci,
		string(Collation_ucs2_roman_ci):               Collation_ucs2_roman_ci,
		string(Collation_ucs2_persian_ci):             Collation_ucs2_persian_ci,
		string(Collation_ucs2_esperanto_ci):           Collation_ucs2_esperanto_ci,
		string(Collation_ucs2_hungarian_ci):           Collation_ucs2_hungarian_ci,
		string(Collation_ucs2_sinhala_ci):             Collation_ucs2_sinhala_ci,
		string(Collation_ucs2_german2_ci):             Collation_ucs2_german2_ci,
		string(Collation_ucs2_croatian_ci):            Collation_ucs2_croatian_ci,
		string(Collation_ucs2_unicode_520_ci):         Collation_ucs2_unicode_520_ci,
		string(Collation_ucs2_vietnamese_ci):          Collation_ucs2_vietnamese_ci,
		string(Collation_ucs2_general_mysql500_ci):    Collation_ucs2_general_mysql500_ci,
		string(Collation_ujis_japanese_ci):            Collation_ujis_japanese_ci,
		string(Collation_ujis_bin):                    Collation_ujis_bin,
		string(Collation_utf16_general_ci):            Collation_utf16_general_ci,
		string(Collation_utf16_bin):                   Collation_utf16_bin,
		string(Collation_utf16_unicode_ci):            Collation_utf16_unicode_ci,
		string(Collation_utf16_icelandic_ci):          Collation_utf16_icelandic_ci,
		string(Collation_utf16_latvian_ci):            Collation_utf16_latvian_ci,
		string(Collation_utf16_romanian_ci):           Collation_utf16_romanian_ci,
		string(Collation_utf16_slovenian_ci):          Collation_utf16_slovenian_ci,
		string(Collation_utf16_polish_ci):             Collation_utf16_polish_ci,
		string(Collation_utf16_estonian_ci):           Collation_utf16_estonian_ci,
		string(Collation_utf16_spanish_ci):            Collation_utf16_spanish_ci,
		string(Collation_utf16_swedish_ci):            Collation_utf16_swedish_ci,
		string(Collation_utf16_turkish_ci):            Collation_utf16_turkish_ci,
		string(Collation_utf16_czech_ci):              Collation_utf16_czech_ci,
		string(Collation_utf16_danish_ci):             Collation_utf16_danish_ci,
		string(Collation_utf16_lithuanian_ci):         Collation_utf16_lithuanian_ci,
		string(Collation_utf16_slovak_ci):             Collation_utf16_slovak_ci,
		string(Collation_utf16_spanish2_ci):           Collation_utf16_spanish2_ci,
		string(Collation_utf16_roman_ci):              Collation_utf16_roman_ci,
		string(Collation_utf16_persian_ci):            Collation_utf16_persian_ci,
		string(Collation_utf16_esperanto_ci):          Collation_utf16_esperanto_ci,
		string(Collation_utf16_hungarian_ci):          Collation_utf16_hungarian_ci,
		string(Collation_utf16_sinhala_ci):            Collation_utf16_sinhala_ci,
		string(Collation_utf16_german2_ci):            Collation_utf16_german2_ci,
		string(Collation_utf16_croatian_ci):           Collation_utf16_croatian_ci,
		string(Collation_utf16_unicode_520_ci):        Collation_utf16_unicode_520_ci,
		string(Collation_utf16_vietnamese_ci):         Collation_utf16_vietnamese_ci,
		string(Collation_utf16le_general_ci):          Collation_utf16le_general_ci,
		string(Collation_utf16le_bin):                 Collation_utf16le_bin,
		string(Collation_utf32_general_ci):            Collation_utf32_general_ci,
		string(Collation_utf32_bin):                   Collation_utf32_bin,
		string(Collation_utf32_unicode_ci):            Collation_utf32_unicode_ci,
		string(Collation_utf32_icelandic_ci):          Collation_utf32_icelandic_ci,
		string(Collation_utf32_latvian_ci):            Collation_utf32_latvian_ci,
		string(Collation_utf32_romanian_ci):           Collation_utf32_romanian_ci,
		string(Collation_utf32_slovenian_ci):          Collation_utf32_slovenian_ci,
		string(Collation_utf32_polish_ci):             Collation_utf32_polish_ci,
		string(Collation_utf32_estonian_ci):           Collation_utf32_estonian_ci,
		string(Collation_utf32_spanish_ci):            Collation_utf32_spanish_ci,
		string(Collation_utf32_swedish_ci):            Collation_utf32_swedish_ci,
		string(Collation_utf32_turkish_ci):            Collation_utf32_turkish_ci,
		string(Collation_utf32_czech_ci):              Collation_utf32_czech_ci,
		string(Collation_utf32_danish_ci):             Collation_utf32_danish_ci,
		string(Collation_utf32_lithuanian_ci):         Collation_utf32_lithuanian_ci,
		string(Collation_utf32_slovak_ci):             Collation_utf32_slovak_ci,
		string(Collation_utf32_spanish2_ci):           Collation_utf32_spanish2_ci,
		string(Collation_utf32_roman_ci):              Collation_utf32_roman_ci,
		string(Collation_utf32_persian_ci):            Collation_utf32_persian_ci,
		string(Collation_utf32_esperanto_ci):          Collation_utf32_esperanto_ci,
		string(Collation_utf32_hungarian_ci):          Collation_utf32_hungarian_ci,
		string(Collation_utf32_sinhala_ci):            Collation_utf32_sinhala_ci,
		string(Collation_utf32_german2_ci):            Collation_utf32_german2_ci,
		string(Collation_utf32_croatian_ci):           Collation_utf32_croatian_ci,
		string(Collation_utf32_unicode_520_ci):        Collation_utf32_unicode_520_ci,
		string(Collation_utf32_vietnamese_ci):         Collation_utf32_vietnamese_ci,
		"utf8_general_ci":                             Collation_utf8mb3_general_ci,
		"utf8_tolower_ci":                             Collation_utf8mb3_tolower_ci,
		"utf8_bin":                                    Collation_utf8mb3_bin,
		"utf8_unicode_ci":                             Collation_utf8mb3_unicode_ci,
		"utf8_icelandic_ci":                           Collation_utf8mb3_icelandic_ci,
		"utf8_latvian_ci":                             Collation_utf8mb3_latvian_ci,
		"utf8_romanian_ci":                            Collation_utf8mb3_romanian_ci,
		"utf8_slovenian_ci":                           Collation_utf8mb3_slovenian_ci,
		"utf8_polish_ci":                              Collation_utf8mb3_polish_ci,
		"utf8_estonian_ci":                            Collation_utf8mb3_estonian_ci,
		"utf8_spanish_ci":                             Collation_utf8mb3_spanish_ci,
		"utf8_swedish_ci":                             Collation_utf8mb3_swedish_ci,
		"utf8_turkish_ci":                             Collation_utf8mb3_turkish_ci,
		"utf8_czech_ci":                               Collation_utf8mb3_czech_ci,
		"utf8_danish_ci":                              Collation_utf8mb3_danish_ci,
		"utf8_lithuanian_ci":                          Collation_utf8mb3_lithuanian_ci,
		"utf8_slovak_ci":                              Collation_utf8mb3_slovak_ci,
		"utf8_spanish2_ci":                            Collation_utf8mb3_spanish2_ci,
		"utf8_roman_ci":                               Collation_utf8mb3_roman_ci,
		"utf8_persian_ci":                             Collation_utf8mb3_persian_ci,
		"utf8_esperanto_ci":                           Collation_utf8mb3_esperanto_ci,
		"utf8_hungarian_ci":                           Collation_utf8mb3_hungarian_ci,
		"utf8_sinhala_ci":                             Collation_utf8mb3_sinhala_ci,
		"utf8_german2_ci":                             Collation_utf8mb3_german2_ci,
		"utf8_croatian_ci":                            Collation_utf8mb3_croatian_ci,
		"utf8_unicode_520_ci":                         Collation_utf8mb3_unicode_520_ci,
		"utf8_vietnamese_ci":                          Collation_utf8mb3_vietnamese_ci,
		"utf8_general_mysql500_ci":                    Collation_utf8mb3_general_mysql500_ci,
		string(Collation_utf8mb3_general_ci):          Collation_utf8mb3_general_ci,
		string(Collation_utf8mb3_tolower_ci):          Collation_utf8mb3_tolower_ci,
		string(Collation_utf8mb3_bin):                 Collation_utf8mb3_bin,
		string(Collation_utf8mb3_unicode_ci):          Collation_utf8mb3_unicode_ci,
		string(Collation_utf8mb3_icelandic_ci):        Collation_utf8mb3_icelandic_ci,
		string(Collation_utf8mb3_latvian_ci):          Collation_utf8mb3_latvian_ci,
		string(Collation_utf8mb3_romanian_ci):         Collation_utf8mb3_romanian_ci,
		string(Collation_utf8mb3_slovenian_ci):        Collation_utf8mb3_slovenian_ci,
		string(Collation_utf8mb3_polish_ci):           Collation_utf8mb3_polish_ci,
		string(Collation_utf8mb3_estonian_ci):         Collation_utf8mb3_estonian_ci,
		string(Collation_utf8mb3_spanish_ci):          Collation_utf8mb3_spanish_ci,
		string(Collation_utf8mb3_swedish_ci):          Collation_utf8mb3_swedish_ci,
		string(Collation_utf8mb3_turkish_ci):          Collation_utf8mb3_turkish_ci,
		string(Collation_utf8mb3_czech_ci):            Collation_utf8mb3_czech_ci,
		string(Collation_utf8mb3_danish_ci):           Collation_utf8mb3_danish_ci,
		string(Collation_utf8mb3_lithuanian_ci):       Collation_utf8mb3_lithuanian_ci,
		string(Collation_utf8mb3_slovak_ci):           Collation_utf8mb3_slovak_ci,
		string(Collation_utf8mb3_spanish2_ci):         Collation_utf8mb3_spanish2_ci,
		string(Collation_utf8mb3_roman_ci):            Collation_utf8mb3_roman_ci,
		string(Collation_utf8mb3_persian_ci):          Collation_utf8mb3_persian_ci,
		string(Collation_utf8mb3_esperanto_ci):        Collation_utf8mb3_esperanto_ci,
		string(Collation_utf8mb3_hungarian_ci):        Collation_utf8mb3_hungarian_ci,
		string(Collation_utf8mb3_sinhala_ci):          Collation_utf8mb3_sinhala_ci,
		string(Collation_utf8mb3_german2_ci):          Collation_utf8mb3_german2_ci,
		string(Collation_utf8mb3_croatian_ci):         Collation_utf8mb3_croatian_ci,
		string(Collation_utf8mb3_unicode_520_ci):      Collation_utf8mb3_unicode_520_ci,
		string(Collation_utf8mb3_vietnamese_ci):       Collation_utf8mb3_vietnamese_ci,
		string(Collation_utf8mb3_general_mysql500_ci): Collation_utf8mb3_general_mysql500_ci,
		string(Collation_utf8mb4_general_ci):          Collation_utf8mb4_general_ci,
		string(Collation_utf8mb4_bin):                 Collation_utf8mb4_bin,
		string(Collation_utf8mb4_unicode_ci):          Collation_utf8mb4_unicode_ci,
		string(Collation_utf8mb4_icelandic_ci):        Collation_utf8mb4_icelandic_ci,
		string(Collation_utf8mb4_latvian_ci):          Collation_utf8mb4_latvian_ci,
		string(Collation_utf8mb4_romanian_ci):         Collation_utf8mb4_romanian_ci,
		string(Collation_utf8mb4_slovenian_ci):        Collation_utf8mb4_slovenian_ci,
		string(Collation_utf8mb4_polish_ci):           Collation_utf8mb4_polish_ci,
		string(Collation_utf8mb4_estonian_ci):         Collation_utf8mb4_estonian_ci,
		string(Collation_utf8mb4_spanish_ci):          Collation_utf8mb4_spanish_ci,
		string(Collation_utf8mb4_swedish_ci):          Collation_utf8mb4_swedish_ci,
		string(Collation_utf8mb4_turkish_ci):          Collation_utf8mb4_turkish_ci,
		string(Collation_utf8mb4_czech_ci):            Collation_utf8mb4_czech_ci,
		string(Collation_utf8mb4_danish_ci):           Collation_utf8mb4_danish_ci,
		string(Collation_utf8mb4_lithuanian_ci):       Collation_utf8mb4_lithuanian_ci,
		string(Collation_utf8mb4_slovak_ci):           Collation_utf8mb4_slovak_ci,
		string(Collation_utf8mb4_spanish2_ci):         Collation_utf8mb4_spanish2_ci,
		string(Collation_utf8mb4_roman_ci):            Collation_utf8mb4_roman_ci,
		string(Collation_utf8mb4_persian_ci):          Collation_utf8mb4_persian_ci,
		string(Collation_utf8mb4_esperanto_ci):        Collation_utf8mb4_esperanto_ci,
		string(Collation_utf8mb4_hungarian_ci):        Collation_utf8mb4_hungarian_ci,
		string(Collation_utf8mb4_sinhala_ci):          Collation_utf8mb4_sinhala_ci,
		string(Collation_utf8mb4_german2_ci):          Collation_utf8mb4_german2_ci,
		string(Collation_utf8mb4_croatian_ci):         Collation_utf8mb4_croatian_ci,
		string(Collation_utf8mb4_unicode_520_ci):      Collation_utf8mb4_unicode_520_ci,
		string(Collation_utf8mb4_vietnamese_ci):       Collation_utf8mb4_vietnamese_ci,
		string(Collation_utf8mb4_0900_ai_ci):          Collation_utf8mb4_0900_ai_ci,
		string(Collation_utf8mb4_de_pb_0900_ai_ci):    Collation_utf8mb4_de_pb_0900_ai_ci,
		string(Collation_utf8mb4_is_0900_ai_ci):       Collation_utf8mb4_is_0900_ai_ci,
		string(Collation_utf8mb4_lv_0900_ai_ci):       Collation_utf8mb4_lv_0900_ai_ci,
		string(Collation_utf8mb4_ro_0900_ai_ci):       Collation_utf8mb4_ro_0900_ai_ci,
		string(Collation_utf8mb4_sl_0900_ai_ci):       Collation_utf8mb4_sl_0900_ai_ci,
		string(Collation_utf8mb4_pl_0900_ai_ci):       Collation_utf8mb4_pl_0900_ai_ci,
		string(Collation_utf8mb4_et_0900_ai_ci):       Collation_utf8mb4_et_0900_ai_ci,
		string(Collation_utf8mb4_es_0900_ai_ci):       Collation_utf8mb4_es_0900_ai_ci,
		string(Collation_utf8mb4_sv_0900_ai_ci):       Collation_utf8mb4_sv_0900_ai_ci,
		string(Collation_utf8mb4_tr_0900_ai_ci):       Collation_utf8mb4_tr_0900_ai_ci,
		string(Collation_utf8mb4_cs_0900_ai_ci):       Collation_utf8mb4_cs_0900_ai_ci,
		string(Collation_utf8mb4_da_0900_ai_ci):       Collation_utf8mb4_da_0900_ai_ci,
		string(Collation_utf8mb4_lt_0900_ai_ci):       Collation_utf8mb4_lt_0900_ai_ci,
		string(Collation_utf8mb4_sk_0900_ai_ci):       Collation_utf8mb4_sk_0900_ai_ci,
		string(Collation_utf8mb4_es_trad_0900_ai_ci):  Collation_utf8mb4_es_trad_0900_ai_ci,
		string(Collation_utf8mb4_la_0900_ai_ci):       Collation_utf8mb4_la_0900_ai_ci,
		string(Collation_utf8mb4_eo_0900_ai_ci):       Collation_utf8mb4_eo_0900_ai_ci,
		string(Collation_utf8mb4_hu_0900_ai_ci):       Collation_utf8mb4_hu_0900_ai_ci,
		string(Collation_utf8mb4_hr_0900_ai_ci):       Collation_utf8mb4_hr_0900_ai_ci,
		string(Collation_utf8mb4_vi_0900_ai_ci):       Collation_utf8mb4_vi_0900_ai_ci,
		string(Collation_utf8mb4_0900_as_cs):          Collation_utf8mb4_0900_as_cs,
		string(Collation_utf8mb4_de_pb_0900_as_cs):    Collation_utf8mb4_de_pb_0900_as_cs,
		string(Collation_utf8mb4_is_0900_as_cs):       Collation_utf8mb4_is_0900_as_cs,
		string(Collation_utf8mb4_lv_0900_as_cs):       Collation_utf8mb4_lv_0900_as_cs,
		string(Collation_utf8mb4_ro_0900_as_cs):       Collation_utf8mb4_ro_0900_as_cs,
		string(Collation_utf8mb4_sl_0900_as_cs):       Collation_utf8mb4_sl_0900_as_cs,
		string(Collation_utf8mb4_pl_0900_as_cs):       Collation_utf8mb4_pl_0900_as_cs,
		string(Collation_utf8mb4_et_0900_as_cs):       Collation_utf8mb4_et_0900_as_cs,
		string(Collation_utf8mb4_es_0900_as_cs):       Collation_utf8mb4_es_0900_as_cs,
		string(Collation_utf8mb4_sv_0900_as_cs):       Collation_utf8mb4_sv_0900_as_cs,
		string(Collation_utf8mb4_tr_0900_as_cs):       Collation_utf8mb4_tr_0900_as_cs,
		string(Collation_utf8mb4_cs_0900_as_cs):       Collation_utf8mb4_cs_0900_as_cs,
		string(Collation_utf8mb4_da_0900_as_cs):       Collation_utf8mb4_da_0900_as_cs,
		string(Collation_utf8mb4_lt_0900_as_cs):       Collation_utf8mb4_lt_0900_as_cs,
		string(Collation_utf8mb4_sk_0900_as_cs):       Collation_utf8mb4_sk_0900_as_cs,
		string(Collation_utf8mb4_es_trad_0900_as_cs):  Collation_utf8mb4_es_trad_0900_as_cs,
		string(Collation_utf8mb4_la_0900_as_cs):       Collation_utf8mb4_la_0900_as_cs,
		string(Collation_utf8mb4_eo_0900_as_cs):       Collation_utf8mb4_eo_0900_as_cs,
		string(Collation_utf8mb4_hu_0900_as_cs):       Collation_utf8mb4_hu_0900_as_cs,
		string(Collation_utf8mb4_hr_0900_as_cs):       Collation_utf8mb4_hr_0900_as_cs,
		string(Collation_utf8mb4_vi_0900_as_cs):       Collation_utf8mb4_vi_0900_as_cs,
		string(Collation_utf8mb4_ja_0900_as_cs):       Collation_utf8mb4_ja_0900_as_cs,
		string(Collation_utf8mb4_ja_0900_as_cs_ks):    Collation_utf8mb4_ja_0900_as_cs_ks,
		string(Collation_utf8mb4_0900_as_ci):          Collation_utf8mb4_0900_as_ci,
		string(Collation_utf8mb4_ru_0900_ai_ci):       Collation_utf8mb4_ru_0900_ai_ci,
		string(Collation_utf8mb4_ru_0900_as_cs):       Collation_utf8mb4_ru_0900_as_cs,
		string(Collation_utf8mb4_zh_0900_as_cs):       Collation_utf8mb4_zh_0900_as_cs,
		string(Collation_utf8mb4_0900_bin):            Collation_utf8mb4_0900_bin,
	}

	characterSetDefaults = map[CharacterSet]Collation{
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

	characterSetDefaultBinaryColl = map[CharacterSet]Collation{
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

	collationToCharacterSet = map[Collation]CharacterSet{
		Collation_armscii8_general_ci:         CharacterSet_armscii8,
		Collation_armscii8_bin:                CharacterSet_armscii8,
		Collation_ascii_general_ci:            CharacterSet_ascii,
		Collation_ascii_bin:                   CharacterSet_ascii,
		Collation_big5_chinese_ci:             CharacterSet_big5,
		Collation_big5_bin:                    CharacterSet_big5,
		Collation_binary:                      CharacterSet_binary,
		Collation_cp1250_general_ci:           CharacterSet_cp1250,
		Collation_cp1250_czech_cs:             CharacterSet_cp1250,
		Collation_cp1250_croatian_ci:          CharacterSet_cp1250,
		Collation_cp1250_bin:                  CharacterSet_cp1250,
		Collation_cp1250_polish_ci:            CharacterSet_cp1250,
		Collation_cp1251_bulgarian_ci:         CharacterSet_cp1251,
		Collation_cp1251_ukrainian_ci:         CharacterSet_cp1251,
		Collation_cp1251_bin:                  CharacterSet_cp1251,
		Collation_cp1251_general_ci:           CharacterSet_cp1251,
		Collation_cp1251_general_cs:           CharacterSet_cp1251,
		Collation_cp1256_general_ci:           CharacterSet_cp1256,
		Collation_cp1256_bin:                  CharacterSet_cp1256,
		Collation_cp1257_lithuanian_ci:        CharacterSet_cp1257,
		Collation_cp1257_bin:                  CharacterSet_cp1257,
		Collation_cp1257_general_ci:           CharacterSet_cp1257,
		Collation_cp850_general_ci:            CharacterSet_cp850,
		Collation_cp850_bin:                   CharacterSet_cp850,
		Collation_cp852_general_ci:            CharacterSet_cp852,
		Collation_cp852_bin:                   CharacterSet_cp852,
		Collation_cp866_general_ci:            CharacterSet_cp866,
		Collation_cp866_bin:                   CharacterSet_cp866,
		Collation_cp932_japanese_ci:           CharacterSet_cp932,
		Collation_cp932_bin:                   CharacterSet_cp932,
		Collation_dec8_swedish_ci:             CharacterSet_dec8,
		Collation_dec8_bin:                    CharacterSet_dec8,
		Collation_eucjpms_japanese_ci:         CharacterSet_eucjpms,
		Collation_eucjpms_bin:                 CharacterSet_eucjpms,
		Collation_euckr_korean_ci:             CharacterSet_euckr,
		Collation_euckr_bin:                   CharacterSet_euckr,
		Collation_gb18030_chinese_ci:          CharacterSet_gb18030,
		Collation_gb18030_bin:                 CharacterSet_gb18030,
		Collation_gb18030_unicode_520_ci:      CharacterSet_gb18030,
		Collation_gb2312_chinese_ci:           CharacterSet_gb2312,
		Collation_gb2312_bin:                  CharacterSet_gb2312,
		Collation_gbk_chinese_ci:              CharacterSet_gbk,
		Collation_gbk_bin:                     CharacterSet_gbk,
		Collation_geostd8_general_ci:          CharacterSet_geostd8,
		Collation_geostd8_bin:                 CharacterSet_geostd8,
		Collation_greek_general_ci:            CharacterSet_greek,
		Collation_greek_bin:                   CharacterSet_greek,
		Collation_hebrew_general_ci:           CharacterSet_hebrew,
		Collation_hebrew_bin:                  CharacterSet_hebrew,
		Collation_hp8_english_ci:              CharacterSet_hp8,
		Collation_hp8_bin:                     CharacterSet_hp8,
		Collation_keybcs2_general_ci:          CharacterSet_keybcs2,
		Collation_keybcs2_bin:                 CharacterSet_keybcs2,
		Collation_koi8r_general_ci:            CharacterSet_koi8r,
		Collation_koi8r_bin:                   CharacterSet_koi8r,
		Collation_koi8u_general_ci:            CharacterSet_koi8u,
		Collation_koi8u_bin:                   CharacterSet_koi8u,
		Collation_latin1_german1_ci:           CharacterSet_latin1,
		Collation_latin1_swedish_ci:           CharacterSet_latin1,
		Collation_latin1_danish_ci:            CharacterSet_latin1,
		Collation_latin1_german2_ci:           CharacterSet_latin1,
		Collation_latin1_bin:                  CharacterSet_latin1,
		Collation_latin1_general_ci:           CharacterSet_latin1,
		Collation_latin1_general_cs:           CharacterSet_latin1,
		Collation_latin1_spanish_ci:           CharacterSet_latin1,
		Collation_latin2_czech_cs:             CharacterSet_latin2,
		Collation_latin2_general_ci:           CharacterSet_latin2,
		Collation_latin2_hungarian_ci:         CharacterSet_latin2,
		Collation_latin2_croatian_ci:          CharacterSet_latin2,
		Collation_latin2_bin:                  CharacterSet_latin2,
		Collation_latin5_turkish_ci:           CharacterSet_latin5,
		Collation_latin5_bin:                  CharacterSet_latin5,
		Collation_latin7_estonian_cs:          CharacterSet_latin7,
		Collation_latin7_general_ci:           CharacterSet_latin7,
		Collation_latin7_general_cs:           CharacterSet_latin7,
		Collation_latin7_bin:                  CharacterSet_latin7,
		Collation_macce_general_ci:            CharacterSet_macce,
		Collation_macce_bin:                   CharacterSet_macce,
		Collation_macroman_general_ci:         CharacterSet_macroman,
		Collation_macroman_bin:                CharacterSet_macroman,
		Collation_sjis_japanese_ci:            CharacterSet_sjis,
		Collation_sjis_bin:                    CharacterSet_sjis,
		Collation_swe7_swedish_ci:             CharacterSet_swe7,
		Collation_swe7_bin:                    CharacterSet_swe7,
		Collation_tis620_thai_ci:              CharacterSet_tis620,
		Collation_tis620_bin:                  CharacterSet_tis620,
		Collation_ucs2_general_ci:             CharacterSet_ucs2,
		Collation_ucs2_bin:                    CharacterSet_ucs2,
		Collation_ucs2_unicode_ci:             CharacterSet_ucs2,
		Collation_ucs2_icelandic_ci:           CharacterSet_ucs2,
		Collation_ucs2_latvian_ci:             CharacterSet_ucs2,
		Collation_ucs2_romanian_ci:            CharacterSet_ucs2,
		Collation_ucs2_slovenian_ci:           CharacterSet_ucs2,
		Collation_ucs2_polish_ci:              CharacterSet_ucs2,
		Collation_ucs2_estonian_ci:            CharacterSet_ucs2,
		Collation_ucs2_spanish_ci:             CharacterSet_ucs2,
		Collation_ucs2_swedish_ci:             CharacterSet_ucs2,
		Collation_ucs2_turkish_ci:             CharacterSet_ucs2,
		Collation_ucs2_czech_ci:               CharacterSet_ucs2,
		Collation_ucs2_danish_ci:              CharacterSet_ucs2,
		Collation_ucs2_lithuanian_ci:          CharacterSet_ucs2,
		Collation_ucs2_slovak_ci:              CharacterSet_ucs2,
		Collation_ucs2_spanish2_ci:            CharacterSet_ucs2,
		Collation_ucs2_roman_ci:               CharacterSet_ucs2,
		Collation_ucs2_persian_ci:             CharacterSet_ucs2,
		Collation_ucs2_esperanto_ci:           CharacterSet_ucs2,
		Collation_ucs2_hungarian_ci:           CharacterSet_ucs2,
		Collation_ucs2_sinhala_ci:             CharacterSet_ucs2,
		Collation_ucs2_german2_ci:             CharacterSet_ucs2,
		Collation_ucs2_croatian_ci:            CharacterSet_ucs2,
		Collation_ucs2_unicode_520_ci:         CharacterSet_ucs2,
		Collation_ucs2_vietnamese_ci:          CharacterSet_ucs2,
		Collation_ucs2_general_mysql500_ci:    CharacterSet_ucs2,
		Collation_ujis_japanese_ci:            CharacterSet_ujis,
		Collation_ujis_bin:                    CharacterSet_ujis,
		Collation_utf16_general_ci:            CharacterSet_utf16,
		Collation_utf16_bin:                   CharacterSet_utf16,
		Collation_utf16_unicode_ci:            CharacterSet_utf16,
		Collation_utf16_icelandic_ci:          CharacterSet_utf16,
		Collation_utf16_latvian_ci:            CharacterSet_utf16,
		Collation_utf16_romanian_ci:           CharacterSet_utf16,
		Collation_utf16_slovenian_ci:          CharacterSet_utf16,
		Collation_utf16_polish_ci:             CharacterSet_utf16,
		Collation_utf16_estonian_ci:           CharacterSet_utf16,
		Collation_utf16_spanish_ci:            CharacterSet_utf16,
		Collation_utf16_swedish_ci:            CharacterSet_utf16,
		Collation_utf16_turkish_ci:            CharacterSet_utf16,
		Collation_utf16_czech_ci:              CharacterSet_utf16,
		Collation_utf16_danish_ci:             CharacterSet_utf16,
		Collation_utf16_lithuanian_ci:         CharacterSet_utf16,
		Collation_utf16_slovak_ci:             CharacterSet_utf16,
		Collation_utf16_spanish2_ci:           CharacterSet_utf16,
		Collation_utf16_roman_ci:              CharacterSet_utf16,
		Collation_utf16_persian_ci:            CharacterSet_utf16,
		Collation_utf16_esperanto_ci:          CharacterSet_utf16,
		Collation_utf16_hungarian_ci:          CharacterSet_utf16,
		Collation_utf16_sinhala_ci:            CharacterSet_utf16,
		Collation_utf16_german2_ci:            CharacterSet_utf16,
		Collation_utf16_croatian_ci:           CharacterSet_utf16,
		Collation_utf16_unicode_520_ci:        CharacterSet_utf16,
		Collation_utf16_vietnamese_ci:         CharacterSet_utf16,
		Collation_utf16le_general_ci:          CharacterSet_utf16le,
		Collation_utf16le_bin:                 CharacterSet_utf16le,
		Collation_utf32_general_ci:            CharacterSet_utf32,
		Collation_utf32_bin:                   CharacterSet_utf32,
		Collation_utf32_unicode_ci:            CharacterSet_utf32,
		Collation_utf32_icelandic_ci:          CharacterSet_utf32,
		Collation_utf32_latvian_ci:            CharacterSet_utf32,
		Collation_utf32_romanian_ci:           CharacterSet_utf32,
		Collation_utf32_slovenian_ci:          CharacterSet_utf32,
		Collation_utf32_polish_ci:             CharacterSet_utf32,
		Collation_utf32_estonian_ci:           CharacterSet_utf32,
		Collation_utf32_spanish_ci:            CharacterSet_utf32,
		Collation_utf32_swedish_ci:            CharacterSet_utf32,
		Collation_utf32_turkish_ci:            CharacterSet_utf32,
		Collation_utf32_czech_ci:              CharacterSet_utf32,
		Collation_utf32_danish_ci:             CharacterSet_utf32,
		Collation_utf32_lithuanian_ci:         CharacterSet_utf32,
		Collation_utf32_slovak_ci:             CharacterSet_utf32,
		Collation_utf32_spanish2_ci:           CharacterSet_utf32,
		Collation_utf32_roman_ci:              CharacterSet_utf32,
		Collation_utf32_persian_ci:            CharacterSet_utf32,
		Collation_utf32_esperanto_ci:          CharacterSet_utf32,
		Collation_utf32_hungarian_ci:          CharacterSet_utf32,
		Collation_utf32_sinhala_ci:            CharacterSet_utf32,
		Collation_utf32_german2_ci:            CharacterSet_utf32,
		Collation_utf32_croatian_ci:           CharacterSet_utf32,
		Collation_utf32_unicode_520_ci:        CharacterSet_utf32,
		Collation_utf32_vietnamese_ci:         CharacterSet_utf32,
		Collation_utf8mb3_general_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_tolower_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_bin:                 CharacterSet_utf8mb3,
		Collation_utf8mb3_unicode_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_icelandic_ci:        CharacterSet_utf8mb3,
		Collation_utf8mb3_latvian_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_romanian_ci:         CharacterSet_utf8mb3,
		Collation_utf8mb3_slovenian_ci:        CharacterSet_utf8mb3,
		Collation_utf8mb3_polish_ci:           CharacterSet_utf8mb3,
		Collation_utf8mb3_estonian_ci:         CharacterSet_utf8mb3,
		Collation_utf8mb3_spanish_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_swedish_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_turkish_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_czech_ci:            CharacterSet_utf8mb3,
		Collation_utf8mb3_danish_ci:           CharacterSet_utf8mb3,
		Collation_utf8mb3_lithuanian_ci:       CharacterSet_utf8mb3,
		Collation_utf8mb3_slovak_ci:           CharacterSet_utf8mb3,
		Collation_utf8mb3_spanish2_ci:         CharacterSet_utf8mb3,
		Collation_utf8mb3_roman_ci:            CharacterSet_utf8mb3,
		Collation_utf8mb3_persian_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_esperanto_ci:        CharacterSet_utf8mb3,
		Collation_utf8mb3_hungarian_ci:        CharacterSet_utf8mb3,
		Collation_utf8mb3_sinhala_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_german2_ci:          CharacterSet_utf8mb3,
		Collation_utf8mb3_croatian_ci:         CharacterSet_utf8mb3,
		Collation_utf8mb3_unicode_520_ci:      CharacterSet_utf8mb3,
		Collation_utf8mb3_vietnamese_ci:       CharacterSet_utf8mb3,
		Collation_utf8mb3_general_mysql500_ci: CharacterSet_utf8mb3,
		Collation_utf8mb4_general_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_bin:                 CharacterSet_utf8mb4,
		Collation_utf8mb4_unicode_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_icelandic_ci:        CharacterSet_utf8mb4,
		Collation_utf8mb4_latvian_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_romanian_ci:         CharacterSet_utf8mb4,
		Collation_utf8mb4_slovenian_ci:        CharacterSet_utf8mb4,
		Collation_utf8mb4_polish_ci:           CharacterSet_utf8mb4,
		Collation_utf8mb4_estonian_ci:         CharacterSet_utf8mb4,
		Collation_utf8mb4_spanish_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_swedish_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_turkish_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_czech_ci:            CharacterSet_utf8mb4,
		Collation_utf8mb4_danish_ci:           CharacterSet_utf8mb4,
		Collation_utf8mb4_lithuanian_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_slovak_ci:           CharacterSet_utf8mb4,
		Collation_utf8mb4_spanish2_ci:         CharacterSet_utf8mb4,
		Collation_utf8mb4_roman_ci:            CharacterSet_utf8mb4,
		Collation_utf8mb4_persian_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_esperanto_ci:        CharacterSet_utf8mb4,
		Collation_utf8mb4_hungarian_ci:        CharacterSet_utf8mb4,
		Collation_utf8mb4_sinhala_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_german2_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_croatian_ci:         CharacterSet_utf8mb4,
		Collation_utf8mb4_unicode_520_ci:      CharacterSet_utf8mb4,
		Collation_utf8mb4_vietnamese_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_0900_ai_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_de_pb_0900_ai_ci:    CharacterSet_utf8mb4,
		Collation_utf8mb4_is_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_lv_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_ro_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sl_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_pl_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_et_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_es_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sv_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_tr_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_cs_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_da_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_lt_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sk_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_es_trad_0900_ai_ci:  CharacterSet_utf8mb4,
		Collation_utf8mb4_la_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_eo_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_hu_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_hr_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_vi_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_0900_as_cs:          CharacterSet_utf8mb4,
		Collation_utf8mb4_de_pb_0900_as_cs:    CharacterSet_utf8mb4,
		Collation_utf8mb4_is_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_lv_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_ro_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sl_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_pl_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_et_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_es_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sv_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_tr_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_cs_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_da_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_lt_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_sk_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_es_trad_0900_as_cs:  CharacterSet_utf8mb4,
		Collation_utf8mb4_la_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_eo_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_hu_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_hr_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_vi_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_ja_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_ja_0900_as_cs_ks:    CharacterSet_utf8mb4,
		Collation_utf8mb4_0900_as_ci:          CharacterSet_utf8mb4,
		Collation_utf8mb4_ru_0900_ai_ci:       CharacterSet_utf8mb4,
		Collation_utf8mb4_ru_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_zh_0900_as_cs:       CharacterSet_utf8mb4,
		Collation_utf8mb4_0900_bin:            CharacterSet_utf8mb4,
		Collation_utf8_general_ci:             CharacterSet_utf8,
	}

	characterSetDescriptions = map[CharacterSet]string{
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

	characterSetMaxLengths = map[CharacterSet]int64{
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

	ErrCharacterSetNotSupported = errors.NewKind("Unknown character set: %v")
	ErrCollationNotSupported    = errors.NewKind("Unknown collation: %v")
)

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

var CollationToMySQLVals = map[Collation]mysqlCollationRow{
	Collation_binary:             {63, Y, Y, 0, NoPad},
	Collation_utf8_general_ci:    {33, Y, Y, 1, PadSpace},
	Collation_utf8mb4_0900_ai_ci: {255, Y, Y, 0, NoPad},
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
		if collation, ok := collations[*collationStr]; ok {
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
		collation, exists := collations[*collationStr]
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
	cs, ok := collationToCharacterSet[c]
	if !ok {
		panic(fmt.Sprintf("%v does not have a character set defined", c))
	}
	return cs
}

// WorksWithCharacterSet returns whether the Collation is valid for the given CharacterSet.
func (c Collation) WorksWithCharacterSet(cs CharacterSet) bool {
	return c.CharacterSet() == cs
}

// String returns the string representation of the Collation.
func (c Collation) String() string {
	return string(c)
}

// ID returns the id of the Collation.
func (c Collation) ID() int64 {
	s, ok := CollationToMySQLVals[c]
	if !ok {
		s := CollationToMySQLVals[Collation_Default]
		return s.ID
	}
	return s.ID
}

// IsDefault returns string specifying id collation is default.
func (c Collation) IsDefault() string {
	s, ok := CollationToMySQLVals[c]
	if !ok {
		return Y
	}
	return s.IsDefault
}

// IsCompiled returns string specifying id collation is compiled.
func (c Collation) IsCompiled() string {
	s, ok := CollationToMySQLVals[c]
	if !ok {
		return Y
	}
	return s.IsCompiled
}

// SortLen returns sort len of the collation.
func (c Collation) SortLen() int64 {
	s, ok := CollationToMySQLVals[c]
	if !ok {
		return 1
	}
	return s.SortLen
}

// PadSpace returns pad space of the collation.
func (c Collation) PadSpace() string {
	s, ok := CollationToMySQLVals[c]
	if !ok {
		return PadSpace
	}
	return s.PadSpace
}
