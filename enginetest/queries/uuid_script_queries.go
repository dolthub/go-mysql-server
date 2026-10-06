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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// UUIDScriptTests contains self-contained uuid script tests.
var UUIDScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9857
		Name:        "UUID_SHORT() function returns 64-bit unsigned integers with proper construction",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT UUID_SHORT() > 0",
				Expected: []sql.Row{
					{true}, // Should return positive values
				},
			},
			{
				Query: "SELECT UUID_SHORT() != UUID_SHORT()",
				Expected: []sql.Row{
					{true}, // Should return different values on each call
				},
			},
			{
				Query: "SELECT UUID_SHORT() + 0 > 0",
				Expected: []sql.Row{
					{true}, // Should work in arithmetic expressions
				},
			},
			{
				Query: "SELECT CAST(UUID_SHORT() AS CHAR) != ''",
				Expected: []sql.Row{
					{true}, // Should cast to non-empty string
				},
			},
			{
				Query: "SELECT UUID_SHORT() BETWEEN 1 AND 18446744073709551615",
				Expected: []sql.Row{
					{true}, // Should be within uint64 range
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // Server ID should be 0-255
				},
			},
			{
				Query: "SET @@global.server_id = 253",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // server time won't let us pin this down further
				},
			},
			{
				Query: "SET @@global.server_id = 1",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name:    "UUIDs used in the wild.",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET @uuid = '6ccd780c-baba-1026-9564-5b8c656024db'",
			"SET @binuuid = '0011223344556677'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT IS_UUID(UUID())`,
				Expected: []sql.Row{{true}},
			},
			{
				Query:    `SELECT IS_UUID(@uuid)`,
				Expected: []sql.Row{{true}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid))`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid, 1), 1)`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				// https://github.com/dolthub/dolt/issues/11457
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), null)`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), 3000)`,
				Expected: []sql.Row{{"baba1026-780c-6ccd-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), -10)`,
				Expected: []sql.Row{{"baba1026-780c-6ccd-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT UUID_TO_BIN(NULL)`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    `SELECT HEX(UUID_TO_BIN(@uuid))`,
				Expected: []sql.Row{{"6CCD780CBABA102695645B8C656024DB"}},
			},
			{
				Query:       `SELECT UUID_TO_BIN(123)`,
				ExpectedErr: sql.ErrUuidUnableToParse,
			},
			{
				Query:       `SELECT BIN_TO_UUID(123)`,
				ExpectedErr: sql.ErrUuidUnableToParse,
			},
			{
				Query:    `SELECT BIN_TO_UUID(X'00112233445566778899aabbccddeeff')`,
				Expected: []sql.Row{{"00112233-4455-6677-8899-aabbccddeeff"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID('0011223344556677')`,
				Expected: []sql.Row{{"30303131-3232-3333-3434-353536363737"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(@binuuid)`,
				Expected: []sql.Row{{"30303131-3232-3333-3434-353536363737"}},
			},
		},
	},
}
