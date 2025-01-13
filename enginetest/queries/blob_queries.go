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

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var BlobQueries = []QueryTest{
	{
		Query: "select i, hex(b) from blobt",
		Expected: []sql.UntypedSqlRow{
			{1, "666972737420726F77"},
			{2, "7365636F6E6420726F77"},
			{3, "746869726420726F77"},
		},
	},
	{
		Query: "select * from blobt where i = 1",
		Expected: []sql.UntypedSqlRow{
			{1, []byte("first row")},
		},
	},
	{
		Query: "select * from blobt order by b desc",
		Expected: []sql.UntypedSqlRow{
			{3, []byte("third row")},
			{2, []byte("second row")},
			{1, []byte("first row")},
		},
	},
	{
		Query: "select * from blobt where b <= 'second row'",
		Expected: []sql.UntypedSqlRow{
			{2, []byte("second row")},
			{1, []byte("first row")},
		},
	},
	{
		Query: "select i, hex(t) from textt",
		Expected: []sql.UntypedSqlRow{
			{1, "666972737420726F77"},
			{2, "7365636F6E6420726F77"},
			{3, "746869726420726F77"},
		},
	},
	{
		Query: "select * from textt where i = 1",
		Expected: []sql.UntypedSqlRow{
			{1, "first row"},
		},
	},
	{
		Query: "select * from textt order by t desc",
		Expected: []sql.UntypedSqlRow{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "select * from textt where t <= 'second row'",
		Expected: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
		},
	},
}

var BlobWriteQueries = []WriteQueryTest{
	{
		WriteQuery:          "insert into blobt values (4, '100000000')",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "select * from blobt where i = 4",
		ExpectedSelect:      []sql.UntypedSqlRow{{4, []byte("100000000")}},
	},
	{
		WriteQuery:          "update blobt set b = '100000000' where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from blobt where i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, []byte("100000000")}},
	},
	{
		WriteQuery:          "delete from blobt where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{2, []byte("second row")},
			{3, []byte("third row")},
		},
	},
	{
		WriteQuery:          "alter table blobt rename column b to v, add v1 int",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, []byte("first row"), nil},
			{2, []byte("second row"), nil},
			{3, []byte("third row"), nil},
		},
	},
	{
		WriteQuery:          "ALTER TABLE blobt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER b",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "select * from blobt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, []byte("first row"), 3},
			{2, []byte("second row"), 4},
			{3, []byte("third row"), 5},
		},
	},
	{
		WriteQuery:          "insert into textt values (4, '100000000')",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "select * from textt where i = 4",
		ExpectedSelect:      []sql.UntypedSqlRow{{4, "100000000"}},
	},
	{
		WriteQuery:          "update textt set t = '100000000' where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from textt where i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, "100000000"}},
	},
	{
		WriteQuery:          "delete from textt where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "alter table textt rename column t to v, add v1 int",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row", nil},
			{2, "second row", nil},
			{3, "third row", nil},
		},
	},
	{
		WriteQuery:          "ALTER TABLE textt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER t",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "select * from textt",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row", 3},
			{2, "second row", 4},
			{3, "third row", 5},
		},
	},
}

var BlobErrors = []QueryErrorTest{
	{
		Query:       "alter table mytable modify s blob",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "alter table mytable modify s text",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "alter table blobt add index bidx (b)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "alter table blobt add index tidx (i, b)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "alter table blobt add index bidx (b(3073))",
		ExpectedErr: sql.ErrKeyTooLong,
	},
	{
		Query:       "alter table textt add index tidx (t)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "alter table textt add index tidx (t(769))",
		ExpectedErr: sql.ErrKeyTooLong,
	},
	{
		Query:       "alter table textt add index tidx (i, t)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table b (b blob primary key)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table b (b tinyblob primary key)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table t (t text primary key)",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table t (t text, primary key (t))",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table b (b blob, primary key (b))",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table b (b blob, primary key (b(3073)))",
		ExpectedErr: sql.ErrKeyTooLong,
	},
	{
		Query:       "create table t (t text, primary key (t(769)))",
		ExpectedErr: sql.ErrKeyTooLong,
	},
	{
		Query:       "create table b (i int primary key, b blob, index bidx(b))",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "create table b (i int primary key, b blob, index bidx(b(3073)))",
		ExpectedErr: sql.ErrKeyTooLong,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 TEXT, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
	{
		Query:       "CREATE TABLE b (pk BIGINT PRIMARY KEY, v1 TINYTEXT, INDEX (v1));",
		ExpectedErr: sql.ErrInvalidBlobTextKey,
	},
}

var BlobUnsupported = []QueryTest{
	{
		Query: "select convert(`b` using utf8) from blobt",
		Expected: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
}
