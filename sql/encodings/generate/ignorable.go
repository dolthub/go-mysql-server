// Copyright 2026 Dolthub, Inc.
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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"go/format"
	"hash/fnv"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
)

const (
	// weightsKeySuffix is the suffix used on collation keys in WeightMaps.
	weightsKeySuffix = "_Weights"

	// ignorableKeySuffix is the suffix used on generated UCA ignorable tables.
	ignorableKeySuffix = "_IgnorableRanges"

	// ignorableGenFilePerm is the file mode used when writing the generated
	// file.
	ignorableGenFilePerm = 0o644

	// collationMaskBatchSize is the maximum number of collations evaluated per
	// bitmask query pass.
	collationMaskBatchSize = 32

	// bmpMaxRune is the highest Basic Multilingual Plane code point.
	bmpMaxRune = '\uffff'

	// int32ByteSize is the serialized byte width of one int32 value when
	// hashing.
	int32ByteSize = 4

	// decimalBase is base 10 for integer parsing.
	decimalBase = 10

	// rangesPerLine is the number of [2]rune intervals formatted per output
	// line.
	rangesPerLine = 4

	// bitSize64 is the 64-bit width used when parsing bitmask results.
	bitSize64 = 64
)

// generateIgnorable writes UCA ignorable rune interval tables to |outPath|.
//
// For every UCA collation in WeightMaps whose weight map does not already
// assign zero to ignorable code points, it queries |mysqlBin| for empty
// WEIGHT_STRING() values as defined in [Unicode Character Sets] and
// deduplicates identical interval slices.
//
// [Unicode Character Sets]: https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html
func generateIgnorable(mysqlBin, outPath string) {
	weightKeys := SortedMapKeys(WeightMaps)
	allCollations := make([]string, 0, len(weightKeys))
	suffixSet := make(map[string]struct{})
	collationSuffix := make(map[string]string)

	for _, wk := range weightKeys {
		collName := strings.TrimSuffix(wk, weightsKeySuffix)
		allCollations = append(allCollations, collName)
		if WeightMaps[wk][0] == 0 {
			continue
		}
		_, suffix := GetCharacterSet(collName)
		utf8mb4Coll := "utf8mb4" + suffix
		suffixSet[utf8mb4Coll] = struct{}{}
		collationSuffix[collName] = utf8mb4Coll
	}

	utf8mb4Colls := slices.Sorted(func(yield func(string) bool) {
		for k := range suffixSet {
			if !yield(k) {
				return
			}
		}
	})
	rangesByUTF8MB4 := queryIgnorableRanges(mysqlBin, utf8mb4Colls)

	collRanges := make(map[string][][2]rune, len(collationSuffix))
	rangeHashes := make(map[string]uint64, len(collationSuffix))
	for collName, utf8mb4Coll := range collationSuffix {
		rangeKey := collName + ignorableKeySuffix
		ranges := rangesByUTF8MB4[utf8mb4Coll]
		collRanges[rangeKey] = ranges
		rangeHashes[rangeKey] = hashInt32Pairs(ranges)
	}

	rangeKeyToVar, _ := deduplicateMaps(collRanges, rangeHashes)
	collToVarName := make(map[string]string, len(collationSuffix))
	for collName := range collationSuffix {
		collToVarName[collName] = rangeKeyToVar[collName+ignorableKeySuffix]
	}

	src := renderIgnorableGenFile(allCollations, collRanges, collToVarName)
	if err := os.WriteFile(outPath, src, ignorableGenFilePerm); err != nil {
		panic(err)
	}
}

// queryIgnorableRanges queries |mysqlBin| for every Unicode scalar value whose
// WEIGHT_STRING() is empty under each collation in |collations|.
func queryIgnorableRanges(mysqlBin string, collations []string) map[string][][2]rune {
	runesByCollation := make(map[string][]rune, len(collations))
	var sqlBuf strings.Builder
	sqlBuf.WriteString(`
CREATE TEMPORARY TABLE cps (cp INT NOT NULL, s VARCHAR(1) CHARACTER SET utf8mb4 NOT NULL);
INSERT INTO cps
WITH d AS (
  SELECT 0 v UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
  UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
  UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
  UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
),
nums AS (
  SELECT (a.v*1048576 + b.v*65536 + c.v*4096 + d.v*256 + e.v*16 + f.v) AS cp
  FROM d a, d b, d c, d d, d e, d f
  WHERE a.v <= 1
)
SELECT cp, CONVERT(CHAR(cp USING utf32) USING utf8mb4)
FROM nums
WHERE cp <= 1114111 AND cp NOT BETWEEN 55296 AND 57343;
`)

	for batchIdx := 0; batchIdx*collationMaskBatchSize < len(collations); batchIdx++ {
		start := batchIdx * collationMaskBatchSize
		end := min(start+collationMaskBatchSize, len(collations))
		batch := collations[start:end]
		exprs := make([]string, len(batch))
		for i, coll := range batch {
			exprs[i] = fmt.Sprintf("IF(WEIGHT_STRING(s COLLATE %s) = '', %d, 0)", coll, uint64(1)<<i)
		}
		fmt.Fprintf(&sqlBuf, "SELECT %d, cp, (%s) AS mask FROM cps HAVING mask != 0 ORDER BY cp;\n",
			batchIdx, strings.Join(exprs, " | "))
	}

	cmd := exec.Command(mysqlBin, "-N", "-B", "-e", sqlBuf.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("sql query failed: %v\n%s", err, out))
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			panic(fmt.Sprintf("unexpected query output line: %q", scanner.Text()))
		}
		batchIdx, err := strconv.Atoi(fields[0])
		if err != nil {
			panic(err)
		}
		cp, err := strconv.Atoi(fields[1])
		if err != nil {
			panic(err)
		}
		mask, err := strconv.ParseUint(fields[2], decimalBase, bitSize64)
		if err != nil {
			panic(err)
		}
		start := batchIdx * collationMaskBatchSize
		end := min(start+collationMaskBatchSize, len(collations))
		for i, coll := range collations[start:end] {
			if mask&(uint64(1)<<i) != 0 {
				runesByCollation[coll] = append(runesByCollation[coll], rune(cp))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	res := make(map[string][][2]rune, len(collations))
	for _, coll := range collations {
		res[coll] = collapseRuneRanges(runesByCollation[coll])
	}
	return res
}

// collapseRuneRanges merges sorted |runes| into contiguous inclusive [2]rune
// intervals.
func collapseRuneRanges(runes []rune) [][2]rune {
	if len(runes) == 0 {
		return nil
	}
	ranges := [][2]rune{{runes[0], runes[0]}}
	for _, r := range runes[1:] {
		last := &ranges[len(ranges)-1]
		if r == last[1]+1 {
			last[1] = r
		} else {
			ranges = append(ranges, [2]rune{r, r})
		}
	}
	return ranges
}

// hashInt32Pairs returns the FNV-64a digest of |pairs| for content
// deduplication.
func hashInt32Pairs(pairs [][2]int32) uint64 {
	h := fnv.New64a()
	var buf [int32ByteSize * 2]byte
	for _, pair := range pairs {
		binary.LittleEndian.PutUint32(buf[:int32ByteSize], uint32(pair[0]))
		binary.LittleEndian.PutUint32(buf[int32ByteSize:], uint32(pair[1]))
		if _, err := h.Write(buf[:]); err != nil {
			panic(err)
		}
	}
	return h.Sum64()
}

// renderIgnorableGenFile formats the generated Go source declaring the
// deduplicated UCA ignorable rune range tables and ucaIgnorableRanges map.
func renderIgnorableGenFile(allCollations []string, sharedRanges map[string][][2]rune, collToVarName map[string]string) []byte {
	var buf bytes.Buffer
	buf.WriteString("// THIS FILE IS GENERATED. DO NOT EDIT BY HAND.\n\n")
	buf.WriteString("package encodings\n\n")

	for _, varName := range SortedMapKeys(sharedRanges) {
		fmt.Fprintf(&buf, "// %s holds generated UCA\n// ignorable code-point intervals.\n", varName)
		fmt.Fprintf(&buf, "var %s = [][2]rune{", varName)
		for idx, iv := range sharedRanges[varName] {
			if idx%rangesPerLine == 0 {
				buf.WriteString("\n\t")
			} else {
				buf.WriteString(" ")
			}
			fmt.Fprintf(&buf, "{%s, %s},", formatRuneLiteral(iv[0]), formatRuneLiteral(iv[1]))
		}
		buf.WriteString("\n}\n\n")
	}

	buf.WriteString("// ucaIgnorableRanges maps each UCA collation name to its ignorable intervals.\n")
	buf.WriteString("var ucaIgnorableRanges = map[string][][2]rune{\n")
	for _, coll := range allCollations {
		if varName, ok := collToVarName[coll]; ok {
			fmt.Fprintf(&buf, "\t%q: %s,\n", coll, varName)
		} else {
			fmt.Fprintf(&buf, "\t%q: nil,\n", coll)
		}
	}
	buf.WriteString("}\n")

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}
	return formatted
}

// formatRuneLiteral formats |r| as a Go rune escape literal.
func formatRuneLiteral(r rune) string {
	if r <= bmpMaxRune {
		return fmt.Sprintf("'\\u%04x'", r)
	}
	return fmt.Sprintf("'\\U%08x'", r)
}
