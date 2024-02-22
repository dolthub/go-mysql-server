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
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var longChar = make([]byte, 32746)

var RowLimitTests = []ScriptTest{
	{
		Name: "row length limit",
		Assertions: []ScriptTestAssertion{
			{
				// latin1 is 1 byte per char
				// numbers are chosen to match Dolt's |MaxTupleDataSize|
				Query: `create table two_col (pk smallint primary key, c1 VARCHAR(32747) NOT NULL, c2 VARCHAR(32746) NOT NULL) CHARACTER SET latin1;`,
			},
			{
				Query: fmt.Sprintf("insert into two_col values (0, '%s', '%s')", longChar, longChar),
			},
			{
				// 65535 - 16 - 20
				Query: "create table one_col (id int primary key, c1 VARCHAR(65499) NOT NULL) CHARACTER SET latin1;",
			},
			{
				Query: fmt.Sprintf("insert into one_col values (0, '%s')", longChar),
			},
			{
				Query: `
CREATE TABLE one_ref (
    id smallint primary key,
    a VARCHAR(10000),
    b VARCHAR(10000),
    c VARCHAR(10000),
    d VARCHAR(10000),
    e VARCHAR(10000),
    f VARCHAR(10000),
    i TEXT
) character set latin1;`,
			},
			{
				Query: fmt.Sprintf("insert into one_ref values (0,'%s', '%s','%s', '%s','%s', '%s','%s')", longChar[:10000], longChar[:10000], longChar[:10000], longChar[:10000], longChar[:10000], longChar[:10000], longChar[:6000]),
			},
		},
	},
	{
		Name: "row length limit errors",
		SetUpScript: []string{
			"create table t (id smallint primary key, a VARCHAR(5000), b VARCHAR(5000), c VARCHAR(5000))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add column d VARCHAR(5000)",
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
			{
				Query:       "alter table t modify column c VARCHAR(7000)",
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
			{
				Query:       fmt.Sprintf("insert into t values (1, '%s', 'a', 'a')", longChar),
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				Query:       "create table t1 (c1 VARCHAR(16883) NOT NULL)",
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
			{
				Query:       "create table t1 (c1 VARCHAR(65536) NOT NULL) CHARACTER SET latin1;",
				ExpectedErr: types.ErrLengthTooLarge,
			},
			{
				Query: `
CREATE TABLE one_ref (
    id int primary key,
    a VARCHAR(10000),
    b VARCHAR(10000),
    c VARCHAR(10000),
    d VARCHAR(10000),
    e VARCHAR(10000),
    f VARCHAR(10000),
    i VARCHAR(6000)
) character set latin1;`,
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
		},
	},
}
