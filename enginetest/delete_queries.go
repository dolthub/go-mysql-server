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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var DeleteTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE FROM mytable;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = 2;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = ?;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i < 3;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i > 1;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i <= 2;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i >= 2;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'first row';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s <> 'dne';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i in (2,3);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s LIKE '%row';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'dne';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = 'invalid';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE (i,s) = (1, 'first row');",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
	},
}

var DeleteErrorTests = []GenericErrorQueryTest{
	{
		Name:  "invalid table",
		Query: "DELETE FROM invalidtable WHERE x < 1;",
	},
	{
		Name:  "invalid column",
		Query: "DELETE FROM mytable WHERE z = 'dne';",
	},
	{
		Name:  "missing binding",
		Query: "DELETE FROM mytable WHERE i = ?;",
	},
	{
		Name:  "negative limit",
		Query: "DELETE FROM mytable LIMIT -1;",
	},
	{
		Name:  "negative offset",
		Query: "DELETE FROM mytable LIMIT 1 OFFSET -1;",
	},
	{
		Name:  "missing keyword from",
		Query: "DELETE mytable WHERE id = 1;",
	},
	{
		Name:  "targets join",
		Query: "DELETE FROM mytable one, mytable two WHERE id = 1;",
	},
	{
		Name:  "targets subquery alias",
		Query: "DELETE FROM (SELECT * FROM mytable) mytable WHERE id = 1;",
	},
}
