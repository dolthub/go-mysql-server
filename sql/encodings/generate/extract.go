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
	"context"
	"database/sql"
	"fmt"
	"slices"

	_ "github.com/go-sql-driver/mysql"
)

const (
	// maxBMPRune is the highest Basic Multilingual Plane scalar value.
	maxBMPRune rune = 0xFFFF
	// maxUnicodeRune is the highest valid Unicode scalar value.
	maxUnicodeRune rune = 0x10FFFF
)

// extractTarget maps a generated binary weight table to the reference collation
// used to extract its weights via [WEIGHT_STRING].
//
// [WEIGHT_STRING]: https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_weight-string
type extractTarget struct {
	// binName is the output binary filename stem written to the parent directory.
	binName string
	// charset is the SQL character set used when converting code points.
	charset string
	// collation is the SQL collation name queried on the reference server.
	collation string
	// maxRune is the highest Unicode scalar value covered by this collation.
	maxRune rune
	// baseKeys provides the set of non-range runes stored in the binary map.
	baseKeys map[rune]int32
}

// ucaExtractTargets lists the 24 deduplicated UCA 4.0.0 and 5.2.0 binary tables
// extracted from a reference server.
var ucaExtractTargets = []extractTarget{
	{binName: "common_utf_croatian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_croatian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_croatian_ci_Weights},
	{binName: "common_utf_czech_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_czech_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_czech_ci_Weights},
	{binName: "common_utf_danish_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_danish_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_danish_ci_Weights},
	{binName: "common_utf_esperanto_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_esperanto_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_esperanto_ci_Weights},
	{binName: "common_utf_estonian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_estonian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_estonian_ci_Weights},
	{binName: "common_utf_german2_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_german2_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_german2_ci_Weights},
	{binName: "common_utf_hungarian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_hungarian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_hungarian_ci_Weights},
	{binName: "common_utf_icelandic_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_icelandic_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_icelandic_ci_Weights},
	{binName: "common_utf_latvian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_latvian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_latvian_ci_Weights},
	{binName: "common_utf_lithuanian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_lithuanian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_lithuanian_ci_Weights},
	{binName: "common_utf_persian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_persian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_persian_ci_Weights},
	{binName: "common_utf_polish_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_polish_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_polish_ci_Weights},
	{binName: "common_utf_roman_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_roman_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_roman_ci_Weights},
	{binName: "common_utf_romanian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_romanian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_romanian_ci_Weights},
	{binName: "common_utf_sinhala_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_sinhala_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_sinhala_ci_Weights},
	{binName: "common_utf_slovak_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_slovak_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_slovak_ci_Weights},
	{binName: "common_utf_slovenian_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_slovenian_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_slovenian_ci_Weights},
	{binName: "common_utf_spanish2_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_spanish2_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_spanish2_ci_Weights},
	{binName: "common_utf_swedish_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_swedish_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_swedish_ci_Weights},
	{binName: "common_utf_turkish_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_turkish_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_turkish_ci_Weights},
	{binName: "common_utf_unicode_520_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_unicode_520_ci", maxRune: maxUnicodeRune, baseKeys: utf8mb4_unicode_520_ci_Weights},
	{binName: "common_utf_unicode_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_unicode_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_unicode_ci_Weights},
	{binName: "common_utf_vietnamese_ci_Weights", charset: "utf8mb4", collation: "utf8mb4_vietnamese_ci", maxRune: maxBMPRune, baseKeys: utf8mb4_vietnamese_ci_Weights},
	{binName: "utf8mb3_unicode_520_ci_Weights", charset: "utf8mb3", collation: "utf8mb3_unicode_520_ci", maxRune: maxBMPRune, baseKeys: utf8mb3_unicode_520_ci_Weights},
}

// extractTargetNames holds the binary stem names owned by the reference
// extractor so offline runs do not overwrite extracted tables with stale maps.
var extractTargetNames = func() map[string]struct{} {
	m := make(map[string]struct{}, len(ucaExtractTargets))
	for _, t := range ucaExtractTargets {
		m[t.binName] = struct{}{}
	}
	return m
}()

// extractUCAMaps connects to |dsn| with |ctx| and regenerates the UCA 4.0.0 and
// 5.2.0 binary weight tables from the reference server.
//
// When |collationFilter| is non-empty, only the matching collation in
// ucaExtractTargets is extracted. Ignorable code points receive weight 0 and
// distinct non-empty weight strings receive sequential positive weights.
func extractUCAMaps(ctx context.Context, dsn, collationFilter string) error {
	targets, err := selectExtractTargets(collationFilter)
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET cte_max_recursion_depth = 70000"); err != nil {
		return fmt.Errorf("set cte_max_recursion_depth: %w", err)
	}

	for _, target := range targets {
		weights, err := extractCollationWeights(ctx, conn, target)
		if err != nil {
			return fmt.Errorf("extract %s: %w", target.collation, err)
		}
		if err := writeWeights(target.binName, weights); err != nil {
			return fmt.Errorf("write %s: %w", target.binName, err)
		}
		fmt.Printf("extracted %s (%d entries)\n", target.binName, len(weights))
	}
	return nil
}

// selectExtractTargets returns the subset of ucaExtractTargets matching
// |collationFilter|, or all targets when |collationFilter| is empty.
func selectExtractTargets(collationFilter string) ([]extractTarget, error) {
	if collationFilter == "" {
		return ucaExtractTargets, nil
	}
	for _, t := range ucaExtractTargets {
		if t.collation == collationFilter {
			return []extractTarget{t}, nil
		}
	}
	return nil, fmt.Errorf("unsupported UCA extraction collation %q", collationFilter)
}

// extractCollationWeights extracts dense sort weights for |target| using |conn|
// and |ctx|.
//
// Every Unicode scalar value up to |target|'s maxRune is queried so distinct
// non-empty weight strings receive sequential positive ranks and ignorable code
// points receive 0. Only keys in |target|'s baseKeys are returned.
func extractCollationWeights(ctx context.Context, conn *sql.Conn, target extractTarget) (map[rune]int32, error) {
	const chunkSize = 65536
	const surrogateLo = 0xD800
	const surrogateHi = 0xDFFF

	// Store raw weight strings keyed by rune and deduplicate non-empty weights
	// so every distinct weight class receives a dense positive rank.
	rawByRune := make(map[rune]string, len(target.baseKeys))
	uniqueWeights := make(map[string]struct{}, chunkSize)

	query := fmt.Sprintf(`
WITH RECURSIVE seq(n) AS (
	SELECT ?
	UNION ALL
	SELECT n + 1 FROM seq WHERE n < ?
)
SELECT n, WEIGHT_STRING(CONVERT(CAST(UNHEX(LPAD(HEX(n), 8, '0')) AS CHAR CHARACTER SET utf32) USING %s) COLLATE %s)
FROM seq
WHERE n < %d OR n > %d`, target.charset, target.collation, surrogateLo, surrogateHi)

	for start := rune(0); start <= target.maxRune; start += chunkSize {
		end := min(start+chunkSize-1, target.maxRune)
		rows, err := conn.QueryContext(ctx, query, int(start), int(end))
		if err != nil {
			return nil, fmt.Errorf("query range [%d, %d]: %w", start, end, err)
		}
		for rows.Next() {
			var cp int32
			var ws []byte
			if err := rows.Scan(&cp, &ws); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan row: %w", err)
			}
			r := rune(cp)
			s := string(ws)
			if len(s) > 0 {
				uniqueWeights[s] = struct{}{}
			}
			if _, ok := target.baseKeys[r]; ok {
				rawByRune[r] = s
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate range [%d, %d]: %w", start, end, err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close rows: %w", err)
		}
	}

	sortedWeights := make([]string, 0, len(uniqueWeights))
	for w := range uniqueWeights {
		sortedWeights = append(sortedWeights, w)
	}
	slices.Sort(sortedWeights)

	rankByWeight := make(map[string]int32, len(sortedWeights))
	for i, w := range sortedWeights {
		rankByWeight[w] = int32(i + 1)
	}

	out := make(map[rune]int32, len(target.baseKeys))
	for r := range target.baseKeys {
		raw, ok := rawByRune[r]
		if !ok {
			return nil, fmt.Errorf("missing extracted weight for rune U+%04X", r)
		}
		if len(raw) == 0 {
			out[r] = 0
		} else {
			out[r] = rankByWeight[raw]
		}
	}
	return out, nil
}
