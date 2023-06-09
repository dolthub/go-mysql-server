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

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

var Header = `// Copyright 2023 Dolthub, Inc.
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

func loadWeightsMap(m map[rune]int32, bin []byte) {
	for i := 0; i < len(bin); i += 8 {
		m[rune(binary.BigEndian.Uint32(bin[i:]))] = int32(binary.BigEndian.Uint32(bin[i+4:]))
	}
}
`

func main() {
	gofile, err := os.OpenFile("../weightmaps.go", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer gofile.Close()

	fmt.Fprintf(gofile, "%s", Header)

	var weightkeys []string
	for k := range WeightMaps {
		weightkeys = append(weightkeys, k)
	}
	sort.Strings(weightkeys)
	for _, k := range weightkeys {
		v := WeightMaps[k]
		OutputWeights(k, v)
		OutputGoForMap(gofile, k)
	}
}

func OutputWeights(name string, weights map[rune]int32) {
	binfile, err := os.OpenFile("../"+name+".bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer binfile.Close()

	var keys []int
	for k := range weights {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	for _, ki := range keys {
		k := rune(ki)
		v := weights[k]
		err := binary.Write(binfile, binary.BigEndian, k)
		if err != nil {
			panic(err)
		}
		err = binary.Write(binfile, binary.BigEndian, v)
		if err != nil {
			panic(err)
		}
	}
}

func OutputGoForMap(gofile *os.File, name string) {
	fmt.Fprintln(gofile)
	fmt.Fprintln(gofile, "//go:embed "+name+".bin")
	fmt.Fprintln(gofile, "var "+name+"_bin []byte // This is generated using the ./generate package.")
	fmt.Fprintln(gofile, "var "+name+"_map = make(map[rune]int32)")
	fmt.Fprintln(gofile, "var "+name+"_once sync.Once")
	fmt.Fprintln(gofile)
	fmt.Fprintln(gofile, "func "+name+"() map[rune]int32 {")
	fmt.Fprintln(gofile, "\t"+name+"_once.Do(func() { loadWeightsMap("+name+"_map, "+name+"_bin) })")
	fmt.Fprintln(gofile, "\treturn "+name+"_map")
	fmt.Fprintln(gofile, "}")
}

var WeightMaps = map[string]map[rune]int32{
	"utf16_croatian_ci_Weights":          utf16_croatian_ci_Weights,
	"utf16_czech_ci_Weights":             utf16_czech_ci_Weights,
	"utf16_danish_ci_Weights":            utf16_danish_ci_Weights,
	"utf16_esperanto_ci_Weights":         utf16_esperanto_ci_Weights,
	"utf16_estonian_ci_Weights":          utf16_estonian_ci_Weights,
	"utf16_german2_ci_Weights":           utf16_german2_ci_Weights,
	"utf16_hungarian_ci_Weights":         utf16_hungarian_ci_Weights,
	"utf16_icelandic_ci_Weights":         utf16_icelandic_ci_Weights,
	"utf16_latvian_ci_Weights":           utf16_latvian_ci_Weights,
	"utf16_lithuanian_ci_Weights":        utf16_lithuanian_ci_Weights,
	"utf16_persian_ci_Weights":           utf16_persian_ci_Weights,
	"utf16_polish_ci_Weights":            utf16_polish_ci_Weights,
	"utf16_roman_ci_Weights":             utf16_roman_ci_Weights,
	"utf16_romanian_ci_Weights":          utf16_romanian_ci_Weights,
	"utf16_sinhala_ci_Weights":           utf16_sinhala_ci_Weights,
	"utf16_slovak_ci_Weights":            utf16_slovak_ci_Weights,
	"utf16_slovenian_ci_Weights":         utf16_slovenian_ci_Weights,
	"utf16_spanish2_ci_Weights":          utf16_spanish2_ci_Weights,
	"utf16_spanish_ci_Weights":           utf16_spanish_ci_Weights,
	"utf16_swedish_ci_Weights":           utf16_swedish_ci_Weights,
	"utf16_turkish_ci_Weights":           utf16_turkish_ci_Weights,
	"utf16_unicode_520_ci_Weights":       utf16_unicode_520_ci_Weights,
	"utf16_unicode_ci_Weights":           utf16_unicode_ci_Weights,
	"utf16_vietnamese_ci_Weights":        utf16_vietnamese_ci_Weights,
	"utf32_croatian_ci_Weights":          utf32_croatian_ci_Weights,
	"utf32_czech_ci_Weights":             utf32_czech_ci_Weights,
	"utf32_danish_ci_Weights":            utf32_danish_ci_Weights,
	"utf32_esperanto_ci_Weights":         utf32_esperanto_ci_Weights,
	"utf32_estonian_ci_Weights":          utf32_estonian_ci_Weights,
	"utf32_german2_ci_Weights":           utf32_german2_ci_Weights,
	"utf32_hungarian_ci_Weights":         utf32_hungarian_ci_Weights,
	"utf32_icelandic_ci_Weights":         utf32_icelandic_ci_Weights,
	"utf32_latvian_ci_Weights":           utf32_latvian_ci_Weights,
	"utf32_lithuanian_ci_Weights":        utf32_lithuanian_ci_Weights,
	"utf32_persian_ci_Weights":           utf32_persian_ci_Weights,
	"utf32_polish_ci_Weights":            utf32_polish_ci_Weights,
	"utf32_roman_ci_Weights":             utf32_roman_ci_Weights,
	"utf32_romanian_ci_Weights":          utf32_romanian_ci_Weights,
	"utf32_sinhala_ci_Weights":           utf32_sinhala_ci_Weights,
	"utf32_slovak_ci_Weights":            utf32_slovak_ci_Weights,
	"utf32_slovenian_ci_Weights":         utf32_slovenian_ci_Weights,
	"utf32_spanish2_ci_Weights":          utf32_spanish2_ci_Weights,
	"utf32_spanish_ci_Weights":           utf32_spanish_ci_Weights,
	"utf32_swedish_ci_Weights":           utf32_swedish_ci_Weights,
	"utf32_turkish_ci_Weights":           utf32_turkish_ci_Weights,
	"utf32_unicode_520_ci_Weights":       utf32_unicode_520_ci_Weights,
	"utf32_unicode_ci_Weights":           utf32_unicode_ci_Weights,
	"utf32_vietnamese_ci_Weights":        utf32_vietnamese_ci_Weights,
	"utf8mb3_croatian_ci_Weights":        utf8mb3_croatian_ci_Weights,
	"utf8mb3_czech_ci_Weights":           utf8mb3_czech_ci_Weights,
	"utf8mb3_danish_ci_Weights":          utf8mb3_danish_ci_Weights,
	"utf8mb3_esperanto_ci_Weights":       utf8mb3_esperanto_ci_Weights,
	"utf8mb3_estonian_ci_Weights":        utf8mb3_estonian_ci_Weights,
	"utf8mb3_german2_ci_Weights":         utf8mb3_german2_ci_Weights,
	"utf8mb3_hungarian_ci_Weights":       utf8mb3_hungarian_ci_Weights,
	"utf8mb3_icelandic_ci_Weights":       utf8mb3_icelandic_ci_Weights,
	"utf8mb3_latvian_ci_Weights":         utf8mb3_latvian_ci_Weights,
	"utf8mb3_lithuanian_ci_Weights":      utf8mb3_lithuanian_ci_Weights,
	"utf8mb3_persian_ci_Weights":         utf8mb3_persian_ci_Weights,
	"utf8mb3_polish_ci_Weights":          utf8mb3_polish_ci_Weights,
	"utf8mb3_roman_ci_Weights":           utf8mb3_roman_ci_Weights,
	"utf8mb3_romanian_ci_Weights":        utf8mb3_romanian_ci_Weights,
	"utf8mb3_sinhala_ci_Weights":         utf8mb3_sinhala_ci_Weights,
	"utf8mb3_slovak_ci_Weights":          utf8mb3_slovak_ci_Weights,
	"utf8mb3_slovenian_ci_Weights":       utf8mb3_slovenian_ci_Weights,
	"utf8mb3_spanish2_ci_Weights":        utf8mb3_spanish2_ci_Weights,
	"utf8mb3_spanish_ci_Weights":         utf8mb3_spanish_ci_Weights,
	"utf8mb3_swedish_ci_Weights":         utf8mb3_swedish_ci_Weights,
	"utf8mb3_turkish_ci_Weights":         utf8mb3_turkish_ci_Weights,
	"utf8mb3_unicode_520_ci_Weights":     utf8mb3_unicode_520_ci_Weights,
	"utf8mb3_unicode_ci_Weights":         utf8mb3_unicode_ci_Weights,
	"utf8mb3_vietnamese_ci_Weights":      utf8mb3_vietnamese_ci_Weights,
	"utf8mb4_0900_ai_ci_Weights":         utf8mb4_0900_ai_ci_Weights,
	"utf8mb4_0900_as_ci_Weights":         utf8mb4_0900_as_ci_Weights,
	"utf8mb4_0900_as_cs_Weights":         utf8mb4_0900_as_cs_Weights,
	"utf8mb4_croatian_ci_Weights":        utf8mb4_croatian_ci_Weights,
	"utf8mb4_cs_0900_ai_ci_Weights":      utf8mb4_cs_0900_ai_ci_Weights,
	"utf8mb4_cs_0900_as_cs_Weights":      utf8mb4_cs_0900_as_cs_Weights,
	"utf8mb4_czech_ci_Weights":           utf8mb4_czech_ci_Weights,
	"utf8mb4_da_0900_ai_ci_Weights":      utf8mb4_da_0900_ai_ci_Weights,
	"utf8mb4_da_0900_as_cs_Weights":      utf8mb4_da_0900_as_cs_Weights,
	"utf8mb4_danish_ci_Weights":          utf8mb4_danish_ci_Weights,
	"utf8mb4_de_pb_0900_ai_ci_Weights":   utf8mb4_de_pb_0900_ai_ci_Weights,
	"utf8mb4_de_pb_0900_as_cs_Weights":   utf8mb4_de_pb_0900_as_cs_Weights,
	"utf8mb4_eo_0900_ai_ci_Weights":      utf8mb4_eo_0900_ai_ci_Weights,
	"utf8mb4_eo_0900_as_cs_Weights":      utf8mb4_eo_0900_as_cs_Weights,
	"utf8mb4_es_0900_ai_ci_Weights":      utf8mb4_es_0900_ai_ci_Weights,
	"utf8mb4_es_0900_as_cs_Weights":      utf8mb4_es_0900_as_cs_Weights,
	"utf8mb4_es_trad_0900_ai_ci_Weights": utf8mb4_es_trad_0900_ai_ci_Weights,
	"utf8mb4_es_trad_0900_as_cs_Weights": utf8mb4_es_trad_0900_as_cs_Weights,
	"utf8mb4_esperanto_ci_Weights":       utf8mb4_esperanto_ci_Weights,
	"utf8mb4_estonian_ci_Weights":        utf8mb4_estonian_ci_Weights,
	"utf8mb4_et_0900_ai_ci_Weights":      utf8mb4_et_0900_ai_ci_Weights,
	"utf8mb4_et_0900_as_cs_Weights":      utf8mb4_et_0900_as_cs_Weights,
	"utf8mb4_german2_ci_Weights":         utf8mb4_german2_ci_Weights,
	"utf8mb4_hr_0900_ai_ci_Weights":      utf8mb4_hr_0900_ai_ci_Weights,
	"utf8mb4_hr_0900_as_cs_Weights":      utf8mb4_hr_0900_as_cs_Weights,
	"utf8mb4_hu_0900_ai_ci_Weights":      utf8mb4_hu_0900_ai_ci_Weights,
	"utf8mb4_hu_0900_as_cs_Weights":      utf8mb4_hu_0900_as_cs_Weights,
	"utf8mb4_hungarian_ci_Weights":       utf8mb4_hungarian_ci_Weights,
	"utf8mb4_icelandic_ci_Weights":       utf8mb4_icelandic_ci_Weights,
	"utf8mb4_is_0900_ai_ci_Weights":      utf8mb4_is_0900_ai_ci_Weights,
	"utf8mb4_is_0900_as_cs_Weights":      utf8mb4_is_0900_as_cs_Weights,
	"utf8mb4_ja_0900_as_cs_Weights":      utf8mb4_ja_0900_as_cs_Weights,
	"utf8mb4_ja_0900_as_cs_ks_Weights":   utf8mb4_ja_0900_as_cs_ks_Weights,
	"utf8mb4_la_0900_ai_ci_Weights":      utf8mb4_la_0900_ai_ci_Weights,
	"utf8mb4_la_0900_as_cs_Weights":      utf8mb4_la_0900_as_cs_Weights,
	"utf8mb4_latvian_ci_Weights":         utf8mb4_latvian_ci_Weights,
	"utf8mb4_lithuanian_ci_Weights":      utf8mb4_lithuanian_ci_Weights,
	"utf8mb4_lt_0900_ai_ci_Weights":      utf8mb4_lt_0900_ai_ci_Weights,
	"utf8mb4_lt_0900_as_cs_Weights":      utf8mb4_lt_0900_as_cs_Weights,
	"utf8mb4_lv_0900_ai_ci_Weights":      utf8mb4_lv_0900_ai_ci_Weights,
	"utf8mb4_lv_0900_as_cs_Weights":      utf8mb4_lv_0900_as_cs_Weights,
	"utf8mb4_persian_ci_Weights":         utf8mb4_persian_ci_Weights,
	"utf8mb4_pl_0900_ai_ci_Weights":      utf8mb4_pl_0900_ai_ci_Weights,
	"utf8mb4_pl_0900_as_cs_Weights":      utf8mb4_pl_0900_as_cs_Weights,
	"utf8mb4_polish_ci_Weights":          utf8mb4_polish_ci_Weights,
	"utf8mb4_ro_0900_ai_ci_Weights":      utf8mb4_ro_0900_ai_ci_Weights,
	"utf8mb4_ro_0900_as_cs_Weights":      utf8mb4_ro_0900_as_cs_Weights,
	"utf8mb4_roman_ci_Weights":           utf8mb4_roman_ci_Weights,
	"utf8mb4_romanian_ci_Weights":        utf8mb4_romanian_ci_Weights,
	"utf8mb4_ru_0900_ai_ci_Weights":      utf8mb4_ru_0900_ai_ci_Weights,
	"utf8mb4_ru_0900_as_cs_Weights":      utf8mb4_ru_0900_as_cs_Weights,
	"utf8mb4_sinhala_ci_Weights":         utf8mb4_sinhala_ci_Weights,
	"utf8mb4_sk_0900_ai_ci_Weights":      utf8mb4_sk_0900_ai_ci_Weights,
	"utf8mb4_sk_0900_as_cs_Weights":      utf8mb4_sk_0900_as_cs_Weights,
	"utf8mb4_sl_0900_ai_ci_Weights":      utf8mb4_sl_0900_ai_ci_Weights,
	"utf8mb4_sl_0900_as_cs_Weights":      utf8mb4_sl_0900_as_cs_Weights,
	"utf8mb4_slovak_ci_Weights":          utf8mb4_slovak_ci_Weights,
	"utf8mb4_slovenian_ci_Weights":       utf8mb4_slovenian_ci_Weights,
	"utf8mb4_spanish2_ci_Weights":        utf8mb4_spanish2_ci_Weights,
	"utf8mb4_spanish_ci_Weights":         utf8mb4_spanish_ci_Weights,
	"utf8mb4_sv_0900_ai_ci_Weights":      utf8mb4_sv_0900_ai_ci_Weights,
	"utf8mb4_sv_0900_as_cs_Weights":      utf8mb4_sv_0900_as_cs_Weights,
	"utf8mb4_swedish_ci_Weights":         utf8mb4_swedish_ci_Weights,
	"utf8mb4_tr_0900_ai_ci_Weights":      utf8mb4_tr_0900_ai_ci_Weights,
	"utf8mb4_tr_0900_as_cs_Weights":      utf8mb4_tr_0900_as_cs_Weights,
	"utf8mb4_turkish_ci_Weights":         utf8mb4_turkish_ci_Weights,
	"utf8mb4_unicode_520_ci_Weights":     utf8mb4_unicode_520_ci_Weights,
	"utf8mb4_unicode_ci_Weights":         utf8mb4_unicode_ci_Weights,
	"utf8mb4_vi_0900_ai_ci_Weights":      utf8mb4_vi_0900_ai_ci_Weights,
	"utf8mb4_vi_0900_as_cs_Weights":      utf8mb4_vi_0900_as_cs_Weights,
	"utf8mb4_vietnamese_ci_Weights":      utf8mb4_vietnamese_ci_Weights,
	"utf8mb4_zh_0900_as_cs_Weights":      utf8mb4_zh_0900_as_cs_Weights,
}
