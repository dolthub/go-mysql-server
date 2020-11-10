// Copyright 2020 Liquidata, Inc.
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
		"DELETE FROM mytable;",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
		nil,
	},
	{
		"DELETE FROM mytable WHERE i = 2;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE i = ?;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		map[string]sql.Expression{
			"v1": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		"DELETE FROM mytable WHERE i < 3;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE i > 1;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE i <= 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE i >= 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE s = 'first row';",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE s <> 'dne';",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
		nil,
	},
	{
		"DELETE FROM mytable WHERE i in (2,3);",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE s LIKE '%row';",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
		nil,
	},
	{
		"DELETE FROM mytable WHERE s = 'dne';",
		[]sql.Row{{sql.NewOkResult(0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable WHERE i = 'invalid';",
		[]sql.Row{{sql.NewOkResult(0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
		nil,
	},
	{
		"DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
		nil,
	},
	{
		"DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		nil,
	},
}

var DeleteErrorTests = []GenericErrorQueryTest{
	{
		"invalid table",
		"DELETE FROM invalidtable WHERE x < 1;",
		nil,
	},
	{
		"invalid column",
		"DELETE FROM mytable WHERE z = 'dne';",
		nil,
	},
	{
		"missing binding",
		"DELETE FROM mytable WHERE i = ?;",
		nil,
	},
	{
		"negative limit",
		"DELETE FROM mytable LIMIT -1;",
		nil,
	},
	{
		"negative offset",
		"DELETE FROM mytable LIMIT 1 OFFSET -1;",
		nil,
	},
	{
		"missing keyword from",
		"DELETE mytable WHERE id = 1;",
		nil,
	},
}
