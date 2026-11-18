// Copyright 2023 Dolthub, Inc.
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

// THIS FILE IS GENERATED. DO NOT EDIT BY HAND.

package encodings

import (
	_ "embed"
	"encoding/binary"
	"sync"
)

// IgnorableWeight is the zero sort weight assigned to ignorable code points in
// Unicode Collation Algorithm collations.
const IgnorableWeight int32 = 0

func loadWeightsMap(m map[rune]int32, bin []byte) {
	for i := 0; i < len(bin); i += 8 {
		m[rune(binary.BigEndian.Uint32(bin[i:]))] = int32(binary.BigEndian.Uint32(bin[i+4:]))
	}
}

//go:embed common_utf8mb4_es_0900_ai_ci_Weights.bin
var common_utf8mb4_es_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf8mb4_es_0900_ai_ci_Weights_map = make(map[rune]int32)
var common_utf8mb4_es_0900_ai_ci_Weights_once sync.Once

func common_utf8mb4_es_0900_ai_ci_Weights() map[rune]int32 {
	common_utf8mb4_es_0900_ai_ci_Weights_once.Do(func() {
		loadWeightsMap(common_utf8mb4_es_0900_ai_ci_Weights_map, common_utf8mb4_es_0900_ai_ci_Weights_bin)
	})
	return common_utf8mb4_es_0900_ai_ci_Weights_map
}

//go:embed common_utf8mb4_es_0900_as_cs_Weights.bin
var common_utf8mb4_es_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var common_utf8mb4_es_0900_as_cs_Weights_map = make(map[rune]int32)
var common_utf8mb4_es_0900_as_cs_Weights_once sync.Once

func common_utf8mb4_es_0900_as_cs_Weights() map[rune]int32 {
	common_utf8mb4_es_0900_as_cs_Weights_once.Do(func() {
		loadWeightsMap(common_utf8mb4_es_0900_as_cs_Weights_map, common_utf8mb4_es_0900_as_cs_Weights_bin)
	})
	return common_utf8mb4_es_0900_as_cs_Weights_map
}

//go:embed common_utf_croatian_ci_Weights.bin
var common_utf_croatian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_croatian_ci_Weights_map = make(map[rune]int32)
var common_utf_croatian_ci_Weights_once sync.Once

func common_utf_croatian_ci_Weights() map[rune]int32 {
	common_utf_croatian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_croatian_ci_Weights_map, common_utf_croatian_ci_Weights_bin) })
	return common_utf_croatian_ci_Weights_map
}

//go:embed common_utf_czech_ci_Weights.bin
var common_utf_czech_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_czech_ci_Weights_map = make(map[rune]int32)
var common_utf_czech_ci_Weights_once sync.Once

func common_utf_czech_ci_Weights() map[rune]int32 {
	common_utf_czech_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_czech_ci_Weights_map, common_utf_czech_ci_Weights_bin) })
	return common_utf_czech_ci_Weights_map
}

//go:embed common_utf_danish_ci_Weights.bin
var common_utf_danish_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_danish_ci_Weights_map = make(map[rune]int32)
var common_utf_danish_ci_Weights_once sync.Once

func common_utf_danish_ci_Weights() map[rune]int32 {
	common_utf_danish_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_danish_ci_Weights_map, common_utf_danish_ci_Weights_bin) })
	return common_utf_danish_ci_Weights_map
}

//go:embed common_utf_esperanto_ci_Weights.bin
var common_utf_esperanto_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_esperanto_ci_Weights_map = make(map[rune]int32)
var common_utf_esperanto_ci_Weights_once sync.Once

func common_utf_esperanto_ci_Weights() map[rune]int32 {
	common_utf_esperanto_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_esperanto_ci_Weights_map, common_utf_esperanto_ci_Weights_bin) })
	return common_utf_esperanto_ci_Weights_map
}

//go:embed common_utf_estonian_ci_Weights.bin
var common_utf_estonian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_estonian_ci_Weights_map = make(map[rune]int32)
var common_utf_estonian_ci_Weights_once sync.Once

func common_utf_estonian_ci_Weights() map[rune]int32 {
	common_utf_estonian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_estonian_ci_Weights_map, common_utf_estonian_ci_Weights_bin) })
	return common_utf_estonian_ci_Weights_map
}

//go:embed common_utf_german2_ci_Weights.bin
var common_utf_german2_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_german2_ci_Weights_map = make(map[rune]int32)
var common_utf_german2_ci_Weights_once sync.Once

func common_utf_german2_ci_Weights() map[rune]int32 {
	common_utf_german2_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_german2_ci_Weights_map, common_utf_german2_ci_Weights_bin) })
	return common_utf_german2_ci_Weights_map
}

//go:embed common_utf_hungarian_ci_Weights.bin
var common_utf_hungarian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_hungarian_ci_Weights_map = make(map[rune]int32)
var common_utf_hungarian_ci_Weights_once sync.Once

func common_utf_hungarian_ci_Weights() map[rune]int32 {
	common_utf_hungarian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_hungarian_ci_Weights_map, common_utf_hungarian_ci_Weights_bin) })
	return common_utf_hungarian_ci_Weights_map
}

//go:embed common_utf_icelandic_ci_Weights.bin
var common_utf_icelandic_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_icelandic_ci_Weights_map = make(map[rune]int32)
var common_utf_icelandic_ci_Weights_once sync.Once

func common_utf_icelandic_ci_Weights() map[rune]int32 {
	common_utf_icelandic_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_icelandic_ci_Weights_map, common_utf_icelandic_ci_Weights_bin) })
	return common_utf_icelandic_ci_Weights_map
}

//go:embed common_utf_latvian_ci_Weights.bin
var common_utf_latvian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_latvian_ci_Weights_map = make(map[rune]int32)
var common_utf_latvian_ci_Weights_once sync.Once

func common_utf_latvian_ci_Weights() map[rune]int32 {
	common_utf_latvian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_latvian_ci_Weights_map, common_utf_latvian_ci_Weights_bin) })
	return common_utf_latvian_ci_Weights_map
}

//go:embed common_utf_lithuanian_ci_Weights.bin
var common_utf_lithuanian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_lithuanian_ci_Weights_map = make(map[rune]int32)
var common_utf_lithuanian_ci_Weights_once sync.Once

func common_utf_lithuanian_ci_Weights() map[rune]int32 {
	common_utf_lithuanian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_lithuanian_ci_Weights_map, common_utf_lithuanian_ci_Weights_bin) })
	return common_utf_lithuanian_ci_Weights_map
}

//go:embed common_utf_persian_ci_Weights.bin
var common_utf_persian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_persian_ci_Weights_map = make(map[rune]int32)
var common_utf_persian_ci_Weights_once sync.Once

func common_utf_persian_ci_Weights() map[rune]int32 {
	common_utf_persian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_persian_ci_Weights_map, common_utf_persian_ci_Weights_bin) })
	return common_utf_persian_ci_Weights_map
}

//go:embed common_utf_polish_ci_Weights.bin
var common_utf_polish_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_polish_ci_Weights_map = make(map[rune]int32)
var common_utf_polish_ci_Weights_once sync.Once

func common_utf_polish_ci_Weights() map[rune]int32 {
	common_utf_polish_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_polish_ci_Weights_map, common_utf_polish_ci_Weights_bin) })
	return common_utf_polish_ci_Weights_map
}

//go:embed common_utf_roman_ci_Weights.bin
var common_utf_roman_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_roman_ci_Weights_map = make(map[rune]int32)
var common_utf_roman_ci_Weights_once sync.Once

func common_utf_roman_ci_Weights() map[rune]int32 {
	common_utf_roman_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_roman_ci_Weights_map, common_utf_roman_ci_Weights_bin) })
	return common_utf_roman_ci_Weights_map
}

//go:embed common_utf_romanian_ci_Weights.bin
var common_utf_romanian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_romanian_ci_Weights_map = make(map[rune]int32)
var common_utf_romanian_ci_Weights_once sync.Once

func common_utf_romanian_ci_Weights() map[rune]int32 {
	common_utf_romanian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_romanian_ci_Weights_map, common_utf_romanian_ci_Weights_bin) })
	return common_utf_romanian_ci_Weights_map
}

//go:embed common_utf_sinhala_ci_Weights.bin
var common_utf_sinhala_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_sinhala_ci_Weights_map = make(map[rune]int32)
var common_utf_sinhala_ci_Weights_once sync.Once

func common_utf_sinhala_ci_Weights() map[rune]int32 {
	common_utf_sinhala_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_sinhala_ci_Weights_map, common_utf_sinhala_ci_Weights_bin) })
	return common_utf_sinhala_ci_Weights_map
}

//go:embed common_utf_slovak_ci_Weights.bin
var common_utf_slovak_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_slovak_ci_Weights_map = make(map[rune]int32)
var common_utf_slovak_ci_Weights_once sync.Once

func common_utf_slovak_ci_Weights() map[rune]int32 {
	common_utf_slovak_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_slovak_ci_Weights_map, common_utf_slovak_ci_Weights_bin) })
	return common_utf_slovak_ci_Weights_map
}

//go:embed common_utf_slovenian_ci_Weights.bin
var common_utf_slovenian_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_slovenian_ci_Weights_map = make(map[rune]int32)
var common_utf_slovenian_ci_Weights_once sync.Once

func common_utf_slovenian_ci_Weights() map[rune]int32 {
	common_utf_slovenian_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_slovenian_ci_Weights_map, common_utf_slovenian_ci_Weights_bin) })
	return common_utf_slovenian_ci_Weights_map
}

//go:embed common_utf_spanish2_ci_Weights.bin
var common_utf_spanish2_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_spanish2_ci_Weights_map = make(map[rune]int32)
var common_utf_spanish2_ci_Weights_once sync.Once

func common_utf_spanish2_ci_Weights() map[rune]int32 {
	common_utf_spanish2_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_spanish2_ci_Weights_map, common_utf_spanish2_ci_Weights_bin) })
	return common_utf_spanish2_ci_Weights_map
}

//go:embed common_utf_swedish_ci_Weights.bin
var common_utf_swedish_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_swedish_ci_Weights_map = make(map[rune]int32)
var common_utf_swedish_ci_Weights_once sync.Once

func common_utf_swedish_ci_Weights() map[rune]int32 {
	common_utf_swedish_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_swedish_ci_Weights_map, common_utf_swedish_ci_Weights_bin) })
	return common_utf_swedish_ci_Weights_map
}

//go:embed common_utf_turkish_ci_Weights.bin
var common_utf_turkish_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_turkish_ci_Weights_map = make(map[rune]int32)
var common_utf_turkish_ci_Weights_once sync.Once

func common_utf_turkish_ci_Weights() map[rune]int32 {
	common_utf_turkish_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_turkish_ci_Weights_map, common_utf_turkish_ci_Weights_bin) })
	return common_utf_turkish_ci_Weights_map
}

//go:embed common_utf_unicode_520_ci_Weights.bin
var common_utf_unicode_520_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_unicode_520_ci_Weights_map = make(map[rune]int32)
var common_utf_unicode_520_ci_Weights_once sync.Once

func common_utf_unicode_520_ci_Weights() map[rune]int32 {
	common_utf_unicode_520_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_unicode_520_ci_Weights_map, common_utf_unicode_520_ci_Weights_bin) })
	return common_utf_unicode_520_ci_Weights_map
}

//go:embed common_utf_unicode_ci_Weights.bin
var common_utf_unicode_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_unicode_ci_Weights_map = make(map[rune]int32)
var common_utf_unicode_ci_Weights_once sync.Once

func common_utf_unicode_ci_Weights() map[rune]int32 {
	common_utf_unicode_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_unicode_ci_Weights_map, common_utf_unicode_ci_Weights_bin) })
	return common_utf_unicode_ci_Weights_map
}

//go:embed common_utf_vietnamese_ci_Weights.bin
var common_utf_vietnamese_ci_Weights_bin []byte // This is generated using the ./generate package.
var common_utf_vietnamese_ci_Weights_map = make(map[rune]int32)
var common_utf_vietnamese_ci_Weights_once sync.Once

func common_utf_vietnamese_ci_Weights() map[rune]int32 {
	common_utf_vietnamese_ci_Weights_once.Do(func() { loadWeightsMap(common_utf_vietnamese_ci_Weights_map, common_utf_vietnamese_ci_Weights_bin) })
	return common_utf_vietnamese_ci_Weights_map
}

//go:embed utf8mb3_unicode_520_ci_Weights.bin
var utf8mb3_unicode_520_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb3_unicode_520_ci_Weights_map = make(map[rune]int32)
var utf8mb3_unicode_520_ci_Weights_once sync.Once

func utf8mb3_unicode_520_ci_Weights() map[rune]int32 {
	utf8mb3_unicode_520_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb3_unicode_520_ci_Weights_map, utf8mb3_unicode_520_ci_Weights_bin) })
	return utf8mb3_unicode_520_ci_Weights_map
}

//go:embed utf8mb4_0900_ai_ci_Weights.bin
var utf8mb4_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_0900_ai_ci_Weights_once sync.Once

func utf8mb4_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_0900_ai_ci_Weights_map, utf8mb4_0900_ai_ci_Weights_bin) })
	return utf8mb4_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_0900_as_ci_Weights.bin
var utf8mb4_0900_as_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_0900_as_ci_Weights_map = make(map[rune]int32)
var utf8mb4_0900_as_ci_Weights_once sync.Once

func utf8mb4_0900_as_ci_Weights() map[rune]int32 {
	utf8mb4_0900_as_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_0900_as_ci_Weights_map, utf8mb4_0900_as_ci_Weights_bin) })
	return utf8mb4_0900_as_ci_Weights_map
}

//go:embed utf8mb4_0900_as_cs_Weights.bin
var utf8mb4_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_0900_as_cs_Weights_once sync.Once

func utf8mb4_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_0900_as_cs_Weights_map, utf8mb4_0900_as_cs_Weights_bin) })
	return utf8mb4_0900_as_cs_Weights_map
}

//go:embed utf8mb4_cs_0900_ai_ci_Weights.bin
var utf8mb4_cs_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_cs_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_cs_0900_ai_ci_Weights_once sync.Once

func utf8mb4_cs_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_cs_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_cs_0900_ai_ci_Weights_map, utf8mb4_cs_0900_ai_ci_Weights_bin) })
	return utf8mb4_cs_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_cs_0900_as_cs_Weights.bin
var utf8mb4_cs_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_cs_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_cs_0900_as_cs_Weights_once sync.Once

func utf8mb4_cs_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_cs_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_cs_0900_as_cs_Weights_map, utf8mb4_cs_0900_as_cs_Weights_bin) })
	return utf8mb4_cs_0900_as_cs_Weights_map
}

//go:embed utf8mb4_da_0900_ai_ci_Weights.bin
var utf8mb4_da_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_da_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_da_0900_ai_ci_Weights_once sync.Once

func utf8mb4_da_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_da_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_da_0900_ai_ci_Weights_map, utf8mb4_da_0900_ai_ci_Weights_bin) })
	return utf8mb4_da_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_da_0900_as_cs_Weights.bin
var utf8mb4_da_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_da_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_da_0900_as_cs_Weights_once sync.Once

func utf8mb4_da_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_da_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_da_0900_as_cs_Weights_map, utf8mb4_da_0900_as_cs_Weights_bin) })
	return utf8mb4_da_0900_as_cs_Weights_map
}

//go:embed utf8mb4_de_pb_0900_ai_ci_Weights.bin
var utf8mb4_de_pb_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_de_pb_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_de_pb_0900_ai_ci_Weights_once sync.Once

func utf8mb4_de_pb_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_de_pb_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_de_pb_0900_ai_ci_Weights_map, utf8mb4_de_pb_0900_ai_ci_Weights_bin) })
	return utf8mb4_de_pb_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_de_pb_0900_as_cs_Weights.bin
var utf8mb4_de_pb_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_de_pb_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_de_pb_0900_as_cs_Weights_once sync.Once

func utf8mb4_de_pb_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_de_pb_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_de_pb_0900_as_cs_Weights_map, utf8mb4_de_pb_0900_as_cs_Weights_bin) })
	return utf8mb4_de_pb_0900_as_cs_Weights_map
}

//go:embed utf8mb4_eo_0900_ai_ci_Weights.bin
var utf8mb4_eo_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_eo_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_eo_0900_ai_ci_Weights_once sync.Once

func utf8mb4_eo_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_eo_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_eo_0900_ai_ci_Weights_map, utf8mb4_eo_0900_ai_ci_Weights_bin) })
	return utf8mb4_eo_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_eo_0900_as_cs_Weights.bin
var utf8mb4_eo_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_eo_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_eo_0900_as_cs_Weights_once sync.Once

func utf8mb4_eo_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_eo_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_eo_0900_as_cs_Weights_map, utf8mb4_eo_0900_as_cs_Weights_bin) })
	return utf8mb4_eo_0900_as_cs_Weights_map
}

//go:embed utf8mb4_et_0900_ai_ci_Weights.bin
var utf8mb4_et_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_et_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_et_0900_ai_ci_Weights_once sync.Once

func utf8mb4_et_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_et_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_et_0900_ai_ci_Weights_map, utf8mb4_et_0900_ai_ci_Weights_bin) })
	return utf8mb4_et_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_et_0900_as_cs_Weights.bin
var utf8mb4_et_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_et_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_et_0900_as_cs_Weights_once sync.Once

func utf8mb4_et_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_et_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_et_0900_as_cs_Weights_map, utf8mb4_et_0900_as_cs_Weights_bin) })
	return utf8mb4_et_0900_as_cs_Weights_map
}

//go:embed utf8mb4_hr_0900_ai_ci_Weights.bin
var utf8mb4_hr_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_hr_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_hr_0900_ai_ci_Weights_once sync.Once

func utf8mb4_hr_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_hr_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_hr_0900_ai_ci_Weights_map, utf8mb4_hr_0900_ai_ci_Weights_bin) })
	return utf8mb4_hr_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_hr_0900_as_cs_Weights.bin
var utf8mb4_hr_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_hr_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_hr_0900_as_cs_Weights_once sync.Once

func utf8mb4_hr_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_hr_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_hr_0900_as_cs_Weights_map, utf8mb4_hr_0900_as_cs_Weights_bin) })
	return utf8mb4_hr_0900_as_cs_Weights_map
}

//go:embed utf8mb4_hu_0900_ai_ci_Weights.bin
var utf8mb4_hu_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_hu_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_hu_0900_ai_ci_Weights_once sync.Once

func utf8mb4_hu_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_hu_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_hu_0900_ai_ci_Weights_map, utf8mb4_hu_0900_ai_ci_Weights_bin) })
	return utf8mb4_hu_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_hu_0900_as_cs_Weights.bin
var utf8mb4_hu_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_hu_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_hu_0900_as_cs_Weights_once sync.Once

func utf8mb4_hu_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_hu_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_hu_0900_as_cs_Weights_map, utf8mb4_hu_0900_as_cs_Weights_bin) })
	return utf8mb4_hu_0900_as_cs_Weights_map
}

//go:embed utf8mb4_is_0900_ai_ci_Weights.bin
var utf8mb4_is_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_is_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_is_0900_ai_ci_Weights_once sync.Once

func utf8mb4_is_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_is_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_is_0900_ai_ci_Weights_map, utf8mb4_is_0900_ai_ci_Weights_bin) })
	return utf8mb4_is_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_is_0900_as_cs_Weights.bin
var utf8mb4_is_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_is_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_is_0900_as_cs_Weights_once sync.Once

func utf8mb4_is_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_is_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_is_0900_as_cs_Weights_map, utf8mb4_is_0900_as_cs_Weights_bin) })
	return utf8mb4_is_0900_as_cs_Weights_map
}

//go:embed utf8mb4_ja_0900_as_cs_Weights.bin
var utf8mb4_ja_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ja_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_ja_0900_as_cs_Weights_once sync.Once

func utf8mb4_ja_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_ja_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ja_0900_as_cs_Weights_map, utf8mb4_ja_0900_as_cs_Weights_bin) })
	return utf8mb4_ja_0900_as_cs_Weights_map
}

//go:embed utf8mb4_ja_0900_as_cs_ks_Weights.bin
var utf8mb4_ja_0900_as_cs_ks_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ja_0900_as_cs_ks_Weights_map = make(map[rune]int32)
var utf8mb4_ja_0900_as_cs_ks_Weights_once sync.Once

func utf8mb4_ja_0900_as_cs_ks_Weights() map[rune]int32 {
	utf8mb4_ja_0900_as_cs_ks_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ja_0900_as_cs_ks_Weights_map, utf8mb4_ja_0900_as_cs_ks_Weights_bin) })
	return utf8mb4_ja_0900_as_cs_ks_Weights_map
}

//go:embed utf8mb4_la_0900_ai_ci_Weights.bin
var utf8mb4_la_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_la_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_la_0900_ai_ci_Weights_once sync.Once

func utf8mb4_la_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_la_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_la_0900_ai_ci_Weights_map, utf8mb4_la_0900_ai_ci_Weights_bin) })
	return utf8mb4_la_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_la_0900_as_cs_Weights.bin
var utf8mb4_la_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_la_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_la_0900_as_cs_Weights_once sync.Once

func utf8mb4_la_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_la_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_la_0900_as_cs_Weights_map, utf8mb4_la_0900_as_cs_Weights_bin) })
	return utf8mb4_la_0900_as_cs_Weights_map
}

//go:embed utf8mb4_lt_0900_ai_ci_Weights.bin
var utf8mb4_lt_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_lt_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_lt_0900_ai_ci_Weights_once sync.Once

func utf8mb4_lt_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_lt_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_lt_0900_ai_ci_Weights_map, utf8mb4_lt_0900_ai_ci_Weights_bin) })
	return utf8mb4_lt_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_lt_0900_as_cs_Weights.bin
var utf8mb4_lt_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_lt_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_lt_0900_as_cs_Weights_once sync.Once

func utf8mb4_lt_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_lt_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_lt_0900_as_cs_Weights_map, utf8mb4_lt_0900_as_cs_Weights_bin) })
	return utf8mb4_lt_0900_as_cs_Weights_map
}

//go:embed utf8mb4_lv_0900_ai_ci_Weights.bin
var utf8mb4_lv_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_lv_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_lv_0900_ai_ci_Weights_once sync.Once

func utf8mb4_lv_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_lv_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_lv_0900_ai_ci_Weights_map, utf8mb4_lv_0900_ai_ci_Weights_bin) })
	return utf8mb4_lv_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_lv_0900_as_cs_Weights.bin
var utf8mb4_lv_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_lv_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_lv_0900_as_cs_Weights_once sync.Once

func utf8mb4_lv_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_lv_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_lv_0900_as_cs_Weights_map, utf8mb4_lv_0900_as_cs_Weights_bin) })
	return utf8mb4_lv_0900_as_cs_Weights_map
}

//go:embed utf8mb4_pl_0900_ai_ci_Weights.bin
var utf8mb4_pl_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_pl_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_pl_0900_ai_ci_Weights_once sync.Once

func utf8mb4_pl_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_pl_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_pl_0900_ai_ci_Weights_map, utf8mb4_pl_0900_ai_ci_Weights_bin) })
	return utf8mb4_pl_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_pl_0900_as_cs_Weights.bin
var utf8mb4_pl_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_pl_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_pl_0900_as_cs_Weights_once sync.Once

func utf8mb4_pl_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_pl_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_pl_0900_as_cs_Weights_map, utf8mb4_pl_0900_as_cs_Weights_bin) })
	return utf8mb4_pl_0900_as_cs_Weights_map
}

//go:embed utf8mb4_ro_0900_ai_ci_Weights.bin
var utf8mb4_ro_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ro_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_ro_0900_ai_ci_Weights_once sync.Once

func utf8mb4_ro_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_ro_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ro_0900_ai_ci_Weights_map, utf8mb4_ro_0900_ai_ci_Weights_bin) })
	return utf8mb4_ro_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_ro_0900_as_cs_Weights.bin
var utf8mb4_ro_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ro_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_ro_0900_as_cs_Weights_once sync.Once

func utf8mb4_ro_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_ro_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ro_0900_as_cs_Weights_map, utf8mb4_ro_0900_as_cs_Weights_bin) })
	return utf8mb4_ro_0900_as_cs_Weights_map
}

//go:embed utf8mb4_ru_0900_ai_ci_Weights.bin
var utf8mb4_ru_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ru_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_ru_0900_ai_ci_Weights_once sync.Once

func utf8mb4_ru_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_ru_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ru_0900_ai_ci_Weights_map, utf8mb4_ru_0900_ai_ci_Weights_bin) })
	return utf8mb4_ru_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_ru_0900_as_cs_Weights.bin
var utf8mb4_ru_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_ru_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_ru_0900_as_cs_Weights_once sync.Once

func utf8mb4_ru_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_ru_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_ru_0900_as_cs_Weights_map, utf8mb4_ru_0900_as_cs_Weights_bin) })
	return utf8mb4_ru_0900_as_cs_Weights_map
}

//go:embed utf8mb4_sk_0900_ai_ci_Weights.bin
var utf8mb4_sk_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sk_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_sk_0900_ai_ci_Weights_once sync.Once

func utf8mb4_sk_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_sk_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sk_0900_ai_ci_Weights_map, utf8mb4_sk_0900_ai_ci_Weights_bin) })
	return utf8mb4_sk_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_sk_0900_as_cs_Weights.bin
var utf8mb4_sk_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sk_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_sk_0900_as_cs_Weights_once sync.Once

func utf8mb4_sk_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_sk_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sk_0900_as_cs_Weights_map, utf8mb4_sk_0900_as_cs_Weights_bin) })
	return utf8mb4_sk_0900_as_cs_Weights_map
}

//go:embed utf8mb4_sl_0900_ai_ci_Weights.bin
var utf8mb4_sl_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sl_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_sl_0900_ai_ci_Weights_once sync.Once

func utf8mb4_sl_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_sl_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sl_0900_ai_ci_Weights_map, utf8mb4_sl_0900_ai_ci_Weights_bin) })
	return utf8mb4_sl_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_sl_0900_as_cs_Weights.bin
var utf8mb4_sl_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sl_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_sl_0900_as_cs_Weights_once sync.Once

func utf8mb4_sl_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_sl_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sl_0900_as_cs_Weights_map, utf8mb4_sl_0900_as_cs_Weights_bin) })
	return utf8mb4_sl_0900_as_cs_Weights_map
}

//go:embed utf8mb4_sv_0900_ai_ci_Weights.bin
var utf8mb4_sv_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sv_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_sv_0900_ai_ci_Weights_once sync.Once

func utf8mb4_sv_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_sv_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sv_0900_ai_ci_Weights_map, utf8mb4_sv_0900_ai_ci_Weights_bin) })
	return utf8mb4_sv_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_sv_0900_as_cs_Weights.bin
var utf8mb4_sv_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_sv_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_sv_0900_as_cs_Weights_once sync.Once

func utf8mb4_sv_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_sv_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_sv_0900_as_cs_Weights_map, utf8mb4_sv_0900_as_cs_Weights_bin) })
	return utf8mb4_sv_0900_as_cs_Weights_map
}

//go:embed utf8mb4_tr_0900_ai_ci_Weights.bin
var utf8mb4_tr_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_tr_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_tr_0900_ai_ci_Weights_once sync.Once

func utf8mb4_tr_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_tr_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_tr_0900_ai_ci_Weights_map, utf8mb4_tr_0900_ai_ci_Weights_bin) })
	return utf8mb4_tr_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_tr_0900_as_cs_Weights.bin
var utf8mb4_tr_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_tr_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_tr_0900_as_cs_Weights_once sync.Once

func utf8mb4_tr_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_tr_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_tr_0900_as_cs_Weights_map, utf8mb4_tr_0900_as_cs_Weights_bin) })
	return utf8mb4_tr_0900_as_cs_Weights_map
}

//go:embed utf8mb4_vi_0900_ai_ci_Weights.bin
var utf8mb4_vi_0900_ai_ci_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_vi_0900_ai_ci_Weights_map = make(map[rune]int32)
var utf8mb4_vi_0900_ai_ci_Weights_once sync.Once

func utf8mb4_vi_0900_ai_ci_Weights() map[rune]int32 {
	utf8mb4_vi_0900_ai_ci_Weights_once.Do(func() { loadWeightsMap(utf8mb4_vi_0900_ai_ci_Weights_map, utf8mb4_vi_0900_ai_ci_Weights_bin) })
	return utf8mb4_vi_0900_ai_ci_Weights_map
}

//go:embed utf8mb4_vi_0900_as_cs_Weights.bin
var utf8mb4_vi_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_vi_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_vi_0900_as_cs_Weights_once sync.Once

func utf8mb4_vi_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_vi_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_vi_0900_as_cs_Weights_map, utf8mb4_vi_0900_as_cs_Weights_bin) })
	return utf8mb4_vi_0900_as_cs_Weights_map
}

//go:embed utf8mb4_zh_0900_as_cs_Weights.bin
var utf8mb4_zh_0900_as_cs_Weights_bin []byte // This is generated using the ./generate package.
var utf8mb4_zh_0900_as_cs_Weights_map = make(map[rune]int32)
var utf8mb4_zh_0900_as_cs_Weights_once sync.Once

func utf8mb4_zh_0900_as_cs_Weights() map[rune]int32 {
	utf8mb4_zh_0900_as_cs_Weights_once.Do(func() { loadWeightsMap(utf8mb4_zh_0900_as_cs_Weights_map, utf8mb4_zh_0900_as_cs_Weights_bin) })
	return utf8mb4_zh_0900_as_cs_Weights_map
}

// IsUCACollation reports whether |name| uses Unicode Collation Algorithm rules.
func IsUCACollation(name string) bool {
	return ucaCollations[name]
}

var ucaCollations = map[string]bool{
	"utf16_croatian_ci":          true,
	"utf16_czech_ci":             true,
	"utf16_danish_ci":            true,
	"utf16_esperanto_ci":         true,
	"utf16_estonian_ci":          true,
	"utf16_german2_ci":           true,
	"utf16_hungarian_ci":         true,
	"utf16_icelandic_ci":         true,
	"utf16_latvian_ci":           true,
	"utf16_lithuanian_ci":        true,
	"utf16_persian_ci":           true,
	"utf16_polish_ci":            true,
	"utf16_roman_ci":             true,
	"utf16_romanian_ci":          true,
	"utf16_sinhala_ci":           true,
	"utf16_slovak_ci":            true,
	"utf16_slovenian_ci":         true,
	"utf16_spanish2_ci":          true,
	"utf16_spanish_ci":           true,
	"utf16_swedish_ci":           true,
	"utf16_turkish_ci":           true,
	"utf16_unicode_520_ci":       true,
	"utf16_unicode_ci":           true,
	"utf16_vietnamese_ci":        true,
	"utf32_croatian_ci":          true,
	"utf32_czech_ci":             true,
	"utf32_danish_ci":            true,
	"utf32_esperanto_ci":         true,
	"utf32_estonian_ci":          true,
	"utf32_german2_ci":           true,
	"utf32_hungarian_ci":         true,
	"utf32_icelandic_ci":         true,
	"utf32_latvian_ci":           true,
	"utf32_lithuanian_ci":        true,
	"utf32_persian_ci":           true,
	"utf32_polish_ci":            true,
	"utf32_roman_ci":             true,
	"utf32_romanian_ci":          true,
	"utf32_sinhala_ci":           true,
	"utf32_slovak_ci":            true,
	"utf32_slovenian_ci":         true,
	"utf32_spanish2_ci":          true,
	"utf32_spanish_ci":           true,
	"utf32_swedish_ci":           true,
	"utf32_turkish_ci":           true,
	"utf32_unicode_520_ci":       true,
	"utf32_unicode_ci":           true,
	"utf32_vietnamese_ci":        true,
	"utf8mb3_croatian_ci":        true,
	"utf8mb3_czech_ci":           true,
	"utf8mb3_danish_ci":          true,
	"utf8mb3_esperanto_ci":       true,
	"utf8mb3_estonian_ci":        true,
	"utf8mb3_german2_ci":         true,
	"utf8mb3_hungarian_ci":       true,
	"utf8mb3_icelandic_ci":       true,
	"utf8mb3_latvian_ci":         true,
	"utf8mb3_lithuanian_ci":      true,
	"utf8mb3_persian_ci":         true,
	"utf8mb3_polish_ci":          true,
	"utf8mb3_roman_ci":           true,
	"utf8mb3_romanian_ci":        true,
	"utf8mb3_sinhala_ci":         true,
	"utf8mb3_slovak_ci":          true,
	"utf8mb3_slovenian_ci":       true,
	"utf8mb3_spanish2_ci":        true,
	"utf8mb3_spanish_ci":         true,
	"utf8mb3_swedish_ci":         true,
	"utf8mb3_turkish_ci":         true,
	"utf8mb3_unicode_520_ci":     true,
	"utf8mb3_unicode_ci":         true,
	"utf8mb3_vietnamese_ci":      true,
	"utf8mb4_0900_ai_ci":         true,
	"utf8mb4_0900_as_ci":         true,
	"utf8mb4_0900_as_cs":         true,
	"utf8mb4_croatian_ci":        true,
	"utf8mb4_cs_0900_ai_ci":      true,
	"utf8mb4_cs_0900_as_cs":      true,
	"utf8mb4_czech_ci":           true,
	"utf8mb4_da_0900_ai_ci":      true,
	"utf8mb4_da_0900_as_cs":      true,
	"utf8mb4_danish_ci":          true,
	"utf8mb4_de_pb_0900_ai_ci":   true,
	"utf8mb4_de_pb_0900_as_cs":   true,
	"utf8mb4_eo_0900_ai_ci":      true,
	"utf8mb4_eo_0900_as_cs":      true,
	"utf8mb4_es_0900_ai_ci":      true,
	"utf8mb4_es_0900_as_cs":      true,
	"utf8mb4_es_trad_0900_ai_ci": true,
	"utf8mb4_es_trad_0900_as_cs": true,
	"utf8mb4_esperanto_ci":       true,
	"utf8mb4_estonian_ci":        true,
	"utf8mb4_et_0900_ai_ci":      true,
	"utf8mb4_et_0900_as_cs":      true,
	"utf8mb4_german2_ci":         true,
	"utf8mb4_hr_0900_ai_ci":      true,
	"utf8mb4_hr_0900_as_cs":      true,
	"utf8mb4_hu_0900_ai_ci":      true,
	"utf8mb4_hu_0900_as_cs":      true,
	"utf8mb4_hungarian_ci":       true,
	"utf8mb4_icelandic_ci":       true,
	"utf8mb4_is_0900_ai_ci":      true,
	"utf8mb4_is_0900_as_cs":      true,
	"utf8mb4_ja_0900_as_cs":      true,
	"utf8mb4_ja_0900_as_cs_ks":   true,
	"utf8mb4_la_0900_ai_ci":      true,
	"utf8mb4_la_0900_as_cs":      true,
	"utf8mb4_latvian_ci":         true,
	"utf8mb4_lithuanian_ci":      true,
	"utf8mb4_lt_0900_ai_ci":      true,
	"utf8mb4_lt_0900_as_cs":      true,
	"utf8mb4_lv_0900_ai_ci":      true,
	"utf8mb4_lv_0900_as_cs":      true,
	"utf8mb4_persian_ci":         true,
	"utf8mb4_pl_0900_ai_ci":      true,
	"utf8mb4_pl_0900_as_cs":      true,
	"utf8mb4_polish_ci":          true,
	"utf8mb4_ro_0900_ai_ci":      true,
	"utf8mb4_ro_0900_as_cs":      true,
	"utf8mb4_roman_ci":           true,
	"utf8mb4_romanian_ci":        true,
	"utf8mb4_ru_0900_ai_ci":      true,
	"utf8mb4_ru_0900_as_cs":      true,
	"utf8mb4_sinhala_ci":         true,
	"utf8mb4_sk_0900_ai_ci":      true,
	"utf8mb4_sk_0900_as_cs":      true,
	"utf8mb4_sl_0900_ai_ci":      true,
	"utf8mb4_sl_0900_as_cs":      true,
	"utf8mb4_slovak_ci":          true,
	"utf8mb4_slovenian_ci":       true,
	"utf8mb4_spanish2_ci":        true,
	"utf8mb4_spanish_ci":         true,
	"utf8mb4_sv_0900_ai_ci":      true,
	"utf8mb4_sv_0900_as_cs":      true,
	"utf8mb4_swedish_ci":         true,
	"utf8mb4_tr_0900_ai_ci":      true,
	"utf8mb4_tr_0900_as_cs":      true,
	"utf8mb4_turkish_ci":         true,
	"utf8mb4_unicode_520_ci":     true,
	"utf8mb4_unicode_ci":         true,
	"utf8mb4_vi_0900_ai_ci":      true,
	"utf8mb4_vi_0900_as_cs":      true,
	"utf8mb4_vietnamese_ci":      true,
	"utf8mb4_zh_0900_as_cs":      true,
}
