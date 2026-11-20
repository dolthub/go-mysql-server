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
	"strings"
	"unicode"

	_ "github.com/go-sql-driver/mysql"
)

// extractChunkSize is the number of Unicode code points queried per recursive
// CTE batch (one 65,536-code-point Unicode plane).
//
// Batching bounds cte_max_recursion_depth and per-query memory when scanning
// all Unicode planes up to the maximum Unicode code point ([unicode.MaxRune]).
const extractChunkSize = 1 << 16

// extractUCAWeightTables connects to |dsn| with |ctx| and writes the binary
// weight tables in |tables| from [WEIGHT_STRING] queries.
//
// [WEIGHT_STRING]: https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_weight-string
func extractUCAWeightTables(ctx context.Context, dsn string, tables []string) error {
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

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET cte_max_recursion_depth = %d", extractChunkSize)); err != nil {
		return fmt.Errorf("set cte_max_recursion_depth: %w", err)
	}

	for _, table := range tables {
		c := referenceCollation(table)
		weights, err := extractUCAWeights(ctx, conn, c, WeightMaps[table])
		if err != nil {
			return fmt.Errorf("extract %s: %w", c, err)
		}
		OutputWeights(table, weights)
		fmt.Printf("extracted %s (%d entries)\n", table, len(weights))
	}
	return nil
}

// extractUCAWeights queries |conn| with |ctx| for UCA |collation| and maps
// each rune in |keys| to its dense sort weight.
//
// In UCA collations, ignorable runes have an empty weight string and receive
// weight 0, while distinct non-empty weight strings receive sequential positive
// weights.
func extractUCAWeights(ctx context.Context, conn *sql.Conn, collation string, keys map[rune]int32) (map[rune]int32, error) {
	const maxBMPRune = 0xFFFF
	const surrogateLo = 0xD800
	const surrogateHi = 0xDFFF

	charset, _ := GetCharacterSet(collation)
	maxRune := rune(unicode.MaxRune)
	if sorted := SortedMapKeys(keys); sorted[len(sorted)-1] <= maxBMPRune {
		// Tables that store only BMP runes are ranked from BMP weights alone.
		maxRune = maxBMPRune
	}

	rawByRune := make(map[rune]string, len(keys))
	// Pre-seed "" so empty weight strings (ignorable runes) sort to rank 0.
	weightSet := map[string]struct{}{"": {}}

	query := fmt.Sprintf(`
WITH RECURSIVE seq(n) AS (
	SELECT ?
	UNION ALL
	SELECT n + 1 FROM seq WHERE n < ?
)
SELECT n, WEIGHT_STRING(CONVERT(CAST(UNHEX(LPAD(HEX(n), 8, '0')) AS CHAR CHARACTER SET utf32) USING %s) COLLATE %s)
FROM seq
WHERE n < %d OR n > %d`, charset, collation, surrogateLo, surrogateHi)

	for start := rune(0); start <= maxRune; start += extractChunkSize {
		end := min(start+extractChunkSize-1, maxRune)
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
			weightSet[s] = struct{}{}
			if _, ok := keys[r]; ok {
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

	ranked := SortedMapKeys(weightSet)
	out := make(map[rune]int32, len(keys))
	for r := range keys {
		raw, ok := rawByRune[r]
		if !ok {
			return nil, fmt.Errorf("missing extracted weight for rune U+%04X", r)
		}
		i, found := slices.BinarySearch(ranked, raw)
		if !found {
			return nil, fmt.Errorf("missing ranked weight for rune U+%04X", r)
		}
		out[r] = int32(i)
	}
	return out, nil
}

// referenceCollation returns the reference collation for binary |table|, or ""
// for UCA 9.0.0 tables.
//
// Checked-in UCA 9.0.0 maps already assign weight 0 to ignorable code points,
// so only UCA 4.0.0 and 5.2.0 tables are extracted from a reference server.
func referenceCollation(table string) string {
	if strings.Contains(table, "_0900_") {
		return ""
	}
	return strings.Replace(strings.TrimSuffix(table, "_Weights"), "common_utf_", "utf8mb4_", 1)
}
