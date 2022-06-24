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
			{1, []byte("first row")},
		},
	},
	{
		Query: "select * from blobt order by b desc",
		Expected: []sql.Row{
			{3, []byte("third row")},
			{2, []byte("second row")},
			{1, []byte("first row")},
		},
	},
	{
		Query: "select * from blobt where b <= 'second row'",
		Expected: []sql.Row{
			{2, []byte("second row")},
			{1, []byte("first row")},
		},
	},
	{
		Query: "select i, hex(t) from textt",
		Expected: []sql.Row{
			{1, "666972737420726F77"},
			{2, "7365636F6E6420726F77"},
			{3, "746869726420726F77"},
		},
	},
	{
		Query: "select * from textt where i = 1",
		Expected: []sql.Row{
			{1, "first row"},
		},
	},
	{
		Query: "select * from textt order by t desc",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "select * from textt where t <= 'second row'",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
		},
	},
}

var BlobWriteQueries = []WriteQueryTest{
	{
		WriteQuery:          "insert into blobt values (4, '100000000')",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from blobt where i = 4",
		ExpectedSelect:      []sql.Row{{4, []byte("100000000")}},
	},
	{
		WriteQuery:          "update blobt set b = '100000000' where i = 1",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from blobt where i = 1",
		ExpectedSelect:      []sql.Row{{1, []byte("100000000")}},
	},
	{
		WriteQuery:          "delete from blobt where i = 1",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{2, []byte("second row")},
			{3, []byte("third row")},
		},
	},
	{
		WriteQuery:          "alter table mytable modify s blob",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []byte("first row")},
			{2, []byte("second row")},
			{3, []byte("third row")},
		},
	},
	{
		WriteQuery:          "alter table blobt rename column b to v, add v1 int",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []byte("first row"), nil},
			{2, []byte("second row"), nil},
			{3, []byte("third row"), nil},
		},
	},
	{
		WriteQuery:          "ALTER TABLE blobt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER b",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.Row{
			{1, []byte("first row"), 3},
			{2, []byte("second row"), 4},
			{3, []byte("third row"), 5},
		},
	},
	{
		WriteQuery:          "insert into textt values (4, '100000000')",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from textt where i = 4",
		ExpectedSelect:      []sql.Row{{4, "100000000"}},
	},
	{
		WriteQuery:          "update textt set t = '100000000' where i = 1",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from textt where i = 1",
		ExpectedSelect:      []sql.Row{{1, "100000000"}},
	},
	{
		WriteQuery:          "delete from textt where i = 1",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.Row{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "alter table mytable modify s text",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "alter table textt rename column t to v, add v1 int",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.Row{
			{1, "first row", nil},
			{2, "second row", nil},
			{3, "third row", nil},
		},
	},
	{
		WriteQuery:          "ALTER TABLE textt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER t",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.Row{
			{1, "first row", 3},
			{2, "second row", 4},
			{3, "third row", 5},
		},
	},
}

var BlobErrors = []QueryErrorTest{
	{
		Query:       "alter table blobt add index bidx (b)",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "alter table textt add index tidx (i, b)",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "alter table blobt add column b2 blob default '1'",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "alter table text add index tidx (t)",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "alter table textt add column t2 text default '1'",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "alter table text add index tidx (i, t)",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "create table b (b blob primary key)",
		ExpectedErr: sql.ErrInvalidBytePrimaryKey,
	},
	{
		Query:       "create table b (b smallblob primary key)",
		ExpectedErr: sql.ErrInvalidBytePrimaryKey,
	},
	{
		Query:       "create table t (t text primary key)",
		ExpectedErr: sql.ErrInvalidBytePrimaryKey,
	},
	{
		Query:       "create table t (t text, primary key (t))",
		ExpectedErr: sql.ErrInvalidBytePrimaryKey,
	},
	{
		Query:       "create table b (b blob, primary key (b))",
		ExpectedErr: sql.ErrInvalidBytePrimaryKey,
	},
	{
		Query:       "create table b (i int primary key, b blob, index bidx(b))",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 TEXT, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidByteIndex,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 TINYTEXT, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidByteIndex,
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
