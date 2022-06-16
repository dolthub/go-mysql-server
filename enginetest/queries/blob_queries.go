// Copyright 2022 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

var BlobQueries = []QueryTest{
	{
		Query: "select i, hex(b) from blobt",
		Expected: []sql.Row{
			{1, "666972737420726F77"},
			{2, "7365636F6E6420726F77"},
			{3, "746869726420726F77"},
		},
	},
	{
		Query: "select * from blobt where i = 1",
		Expected: []sql.Row{
			{1, []uint8("first row")},
		},
	},
	{
		Query: "select * from blobt order by b desc",
		Expected: []sql.Row{
			{3, []uint8("third row")},
			{2, []uint8("second row")},
			{1, []uint8("first row")},
		},
	},
}

var BlobWriteQueries = []WriteQueryTest{
	{
		WriteQuery:          "insert into blobt values (4, '100000000')",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from blobt where i = 4",
		ExpectedSelect:      []sql.Row{{4, []uint8("100000000")}},
	},
	{
		WriteQuery:          "update blobt set b = '100000000' where i = 1",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from blobt where i = 1",
		ExpectedSelect:      []sql.Row{{1, []uint8("100000000")}},
	},
	{
		WriteQuery:          "delete from blobt where i = 1",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{2, []uint8("second row")},
			{3, []uint8("third row")},
		},
	},
	{
		WriteQuery:          "alter table mytable modify s blob",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []uint8("first row")},
			{2, []uint8("second row")},
			{3, []uint8("third row")},
		},
	},
	{
		WriteQuery:          "alter table blobt rename column b to v, add v1 int",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []uint8("first row"), nil},
			{2, []uint8("second row"), nil},
			{3, []uint8("third row"), nil},
		},
	},
	{
		WriteQuery:          "ALTER TABLE blobt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER b",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []uint8("first row"), 3},
			{2, []uint8("second row"), 4},
			{3, []uint8("third row"), 5},
		},
	},
}

var BlobErrors = []QueryErrorTest{
	{
		Query:       "alter table blobt add index bidx (b)",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
	{
		Query:       "alter table blobt add column b2 blob default '1'",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "create table b (b blob primary key)",
		ExpectedErr: sql.ErrInvalidBinaryPrimaryKey,
	},
	{
		Query:       "create table b (b smallblob primary key)",
		ExpectedErr: sql.ErrInvalidBinaryPrimaryKey,
	},
	{
		Query:       "create table b (i int primary key, b blob, index bidx(b))",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 BINARY(20), INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 VARBINARY, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 TEXT, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBinaryIndex,
	},
}

var BlobUnsupported = []QueryTest{
	{
		Query: "select convert(`b` using utf8) from blobt",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
}
