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

import "github.com/liquidata-inc/go-mysql-server/sql"

var DeleteTests = []WriteQueryTest{
	{
		"DELETE FROM mytable;",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
	},
	{
		"DELETE FROM mytable WHERE i = 2;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable WHERE i < 3;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable WHERE i > 1;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}},
	},
	{
		"DELETE FROM mytable WHERE i <= 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable WHERE i >= 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}},
	},
	{
		"DELETE FROM mytable WHERE s = 'first row';",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable WHERE s <> 'dne';",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
	},
	{
		"DELETE FROM mytable WHERE s LIKE '%row';",
		[]sql.Row{{sql.NewOkResult(3)}},
		"SELECT * FROM mytable;",
		nil,
	},
	{
		"DELETE FROM mytable WHERE s = 'dne';",
		[]sql.Row{{sql.NewOkResult(0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable WHERE i = 'invalid';",
		[]sql.Row{{sql.NewOkResult(0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(3), "third row"}},
	},
	{
		"DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
	},
	{
		"DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
}

var DeleteErrorTests = []GenericErrorQueryTest{
	{
		"invalid table",
		"DELETE FROM invalidtable WHERE x < 1;",
	},
	{
		"invalid column",
		"DELETE FROM mytable WHERE z = 'dne';",
	},
	{
		"negative limit",
		"DELETE FROM mytable LIMIT -1;",
	},
	{
		"negative offset",
		"DELETE FROM mytable LIMIT 1 OFFSET -1;",
	},
	{
		"missing keyword from",
		"DELETE mytable WHERE id = 1;",
	},
}

