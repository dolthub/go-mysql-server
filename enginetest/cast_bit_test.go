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

package enginetest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func runCastBitSetup(t *testing.T, e enginetest.QueryEngine, ctx *sql.Context, q string) {
	t.Helper()
	_, iter, _, err := e.Query(ctx, q)
	require.NoError(t, err, "query: %s", q)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(t, err, "query: %s", q)
}

// TestCastBitToCharAndBinary covers dolthub/dolt#12641: CAST / CONVERT of a BIT
// value to CHAR or BINARY must return the value's bytes, matching MySQL, not
// the decimal text of the value. This applies to BIT columns and b'...'
// literals, with or without a length. It is the same class of bug as #11422
// (GROUP_CONCAT(BIT)), in a different function: the value used to fall through
// TypeAwareConversion to the LongText / LongBlob conversion, which renders the
// decimal digits (uint64(0x4142) became "16706" instead of "AB").
func TestCastBitToCharAndBinary(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()
	ctx := enginetest.NewContext(harness)

	runCastBitSetup(t, e, ctx, "CREATE TABLE m (id INT PRIMARY KEY, bt BIT(16))")
	runCastBitSetup(t, e, ctx, "INSERT INTO m VALUES (1, b'0100000101000010')")

	tests := []queries.QueryTest{
		// The issue's repro queries, asserted via HEX so the expected values
		// are plain strings.
		{Query: "SELECT HEX(CAST(bt AS CHAR)) FROM m", Expected: []sql.Row{{"4142"}}},
		{Query: "SELECT HEX(CAST(bt AS BINARY)) FROM m", Expected: []sql.Row{{"4142"}}},
		{Query: "SELECT HEX(CONVERT(bt, CHAR)) FROM m", Expected: []sql.Row{{"4142"}}},
		{Query: "SELECT HEX(CAST(bt AS CHAR(1))) FROM m", Expected: []sql.Row{{"41"}}},
		{Query: "SELECT HEX(CAST(bt AS BINARY(12))) FROM m", Expected: []sql.Row{{"414200000000000000000000"}}},
		{Query: "SELECT HEX(CAST(b'1000001' AS CHAR))", Expected: []sql.Row{{"41"}}},
		// Direct values, not just their hex.
		{Query: "SELECT CAST(bt AS CHAR) FROM m", Expected: []sql.Row{{"AB"}}},
		{Query: "SELECT CAST(bt AS BINARY) FROM m", Expected: []sql.Row{{[]byte{0x41, 0x42}}}},
		// Bit-width edge cases: BIT(1) is one byte, BIT(9) is two bytes.
		{Query: "SELECT HEX(CAST(b'1' AS CHAR))", Expected: []sql.Row{{"01"}}},
		{Query: "SELECT HEX(CAST(b'100000000' AS CHAR))", Expected: []sql.Row{{"0100"}}},
		// Numeric targets are unaffected: a BIT still converts to its integer value.
		{Query: "SELECT CAST(bt AS SIGNED) FROM m", Expected: []sql.Row{{int64(16706)}}},
	}

	for _, tt := range tests {
		enginetest.TestQuery(t, harness, e, tt)
	}
}
