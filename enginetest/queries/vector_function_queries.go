// Copyright 2024 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type VectorFunctionTestCase struct {
	stringInput    string
	expectedVector []float32
	bitLength      int
	charLength     int
	hex            string
	length         int
	base64         string
	md5            string
	sha1           string
	sha2           string
}

var VectorFunctionTestCases = []VectorFunctionTestCase{
	{
		stringInput:    "[1.0]",
		expectedVector: []float32{1.0},
		bitLength:      32,
		charLength:     4,
		hex:            "0000803F",
		length:         4,
		base64:         "AACAPw==",
		md5:            "429d81ed2795e3c586906c6c335aa136",
		sha1:           "5bb96baed2a67ef718989bf7de91433ca9b9f8cf",
		sha2:           "e00e5eb9444182f352323374ef4e08ebcb784725fdd4fd612d7730540b3e0c8c",
	},
	{
		stringInput:    "[2.0, 3.0]",
		expectedVector: []float32{2.0, 3.0},
		bitLength:      64,
		charLength:     8,
		hex:            "0000004000004040",
		length:         8,
		base64:         "AAAAQAAAQEA=",
		md5:            "f37b6e459e9e2d49261fe42d3a7bff07",
		sha1:           "fd3352c0e141970e5b1c45d1755760d018cfe32d",
		sha2:           "2fd848aa90e817e10e20985de4e8ac6a09b0fe70623d6b952e46800be6b025b9",
	},
}

func MakeVectorFunctionTest(testCase VectorFunctionTestCase) ScriptTest {
	return ScriptTest{
		Name: testCase.stringInput,
		Assertions: []ScriptTestAssertion{
			{
				Query:    fmt.Sprintf(`select BIT_LENGTH(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.bitLength}},
			},
			{
				Query:    fmt.Sprintf(`select CHAR_LENGTH(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.charLength}},
			},
			{
				Query:    fmt.Sprintf(`select CHARACTER_LENGTH(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.charLength}},
			},
			{
				Query:    fmt.Sprintf(`select HEX(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.hex}},
			},
			{
				Query:    fmt.Sprintf(`select LENGTH(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.length}},
			},
			{
				Query:    fmt.Sprintf(`select TO_BASE64(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.base64}},
			},
			{
				Query:    fmt.Sprintf(`select MD5(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.md5}},
			},
			{
				Query:    fmt.Sprintf(`select SHA1(STRING_TO_VECTOR("%s"));`, testCase.stringInput),
				Expected: []sql.Row{{testCase.sha1}},
			},
			{
				Query:    fmt.Sprintf(`select SHA2(STRING_TO_VECTOR("%s"), 0);`, testCase.stringInput),
				Expected: []sql.Row{{testCase.sha2}},
			},
		},
	}
}

var VectorFunctionQueries = []ScriptTest{
	{
		Name: "VECTOR conversion functions",
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT STRING_TO_VECTOR("[1.0, 2.0]");`,
				Expected: []sql.Row{{floatsToBytes(1.0, 2.0)}},
			},
			{
				Query:    `SELECT TO_VECTOR("[1.0, 2.0]");`,
				Expected: []sql.Row{{floatsToBytes(1.0, 2.0)}},
			},
			{
				Query:    `SELECT VEC_FromText("[1.0, 2.0]");`,
				Expected: []sql.Row{{floatsToBytes(1.0, 2.0)}},
			},
			{
				// Example that tests the actual bytes instead of using floatsToBytes
				Query:    `SELECT STRING_TO_VECTOR("[0.0]");`,
				Expected: []sql.Row{{[]byte{0x0, 0x0, 0x0, 0x0}}},
			},
			{
				// Example that tests the actual bytes instead of using floatsToBytes
				Query:    `SELECT STRING_TO_VECTOR("[123456.78e2, -8765432.0]");`,
				Expected: []sql.Row{{[]byte{0x4e, 0x61, 0x3c, 0x4b, 0xf8, 0xbf, 0x05, 0xcb}}},
			},
			{
				Query:    `SELECT VECTOR_TO_STRING(STRING_TO_VECTOR("[1.0, 2.0]"));`,
				Expected: []sql.Row{{"[1, 2]"}},
			},
			{
				Query:    `select VECTOR_TO_STRING(0x0000803F);`,
				Expected: []sql.Row{{"[1]"}},
			},

			{
				Query:    `SELECT FROM_VECTOR(TO_VECTOR("[1.0, 2.0]"));`,
				Expected: []sql.Row{{"[1, 2]"}},
			},
			{
				Query:    `SELECT VEC_ToText(VEC_FromText("[1.0, 2.0]"));`,
				Expected: []sql.Row{{"[1, 2]"}},
			},
		},
	},
	{
		Name: "VECTOR distance functions",
		SetUpScript: []string{
			"create table vectors (id int primary key, v json);",
			`insert into vectors values (1, '[3.0,4.0]'), (2, '[0.0,0.0]'), (3, '[1.0,-1.0]'), (4, '[-2.0,0.0]');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select VEC_DISTANCE('[10.0]', '[20.0]');",
				Expected: []sql.Row{{100.0}},
			},
			{
				Query:    "select VEC_DISTANCE_L2_SQUARED('[1.0, 2.0]', '[5.0, 5.0]');",
				Expected: []sql.Row{{25.0}},
			},
			{
				Query:    "select VEC_DISTANCE_EUCLIDEAN('[1.0, 2.0]', '[5.0, 5.0]');",
				Expected: []sql.Row{{5.0}},
			},
			{
				Query:    `SELECT DISTANCE(STRING_TO_VECTOR("[0.0, 0.0]"), STRING_TO_VECTOR("[3.0, 4.0]"), "EUCLIDEAN");`,
				Expected: []sql.Row{{5.0}},
			},
			{
				Query:    "select VEC_DISTANCE_COSINE(STRING_TO_VECTOR('[0.0, 3.0]'), '[5.0, 5.0]');",
				Expected: []sql.Row{{0.29289321881345254}},
			},
			{
				Query:    `SELECT DISTANCE("[1.0, 1.0]", STRING_TO_VECTOR("[-1.0, 1.0]"), "COSINE");`,
				Expected: []sql.Row{{1.0}},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[1.0, -1.0]`)},
					{4, types.MustJSON(`[-2.0, 0.0]`)},
					{1, types.MustJSON(`[3.0, 4.0]`)},
				},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE_L2_SQUARED('[-2.0,0.0]', v)",
				Expected: []sql.Row{
					{4, types.MustJSON(`[-2.0, 0.0]`)},
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[1.0, -1.0]`)},
					{1, types.MustJSON(`[3.0, 4.0]`)},
				},
			},
		},
	},
}
