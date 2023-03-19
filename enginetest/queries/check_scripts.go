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
	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/plan"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

var ChecksOnUpdateScriptTests = []ScriptTest{
	{
		Name: "Single table updates",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER)",
			"ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (b > 10) NOT ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0)",
			"ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK ((a + b) / 2 >= 1) ENFORCED",
			"INSERT INTO t1 VALUES (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:       "UPDATE t1 set b = 0;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set a = 0, b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set b = 0 WHERE b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set a = 0, b = 1 WHERE b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "Update join updates",
		SetUpScript: []string{
			"CREATE TABLE sales (year_built int primary key, CONSTRAINT `valid_year_built` CHECK (year_built <= 2022));",
			"INSERT INTO sales VALUES (1981);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE sales JOIN (SELECT year_built FROM sales) AS t ON sales.year_built = t.year_built SET sales.year_built = 1901;",
				Expected: []sql.Row{{types.OkResult{1, 0, plan.UpdateInfo{1, 1, 0}}}},
			},
			{
				Query:    "select * from sales;",
				Expected: []sql.Row{{1901}},
			},
			{
				Query:    "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET S1.year_built = 1902;",
				Expected: []sql.Row{{types.OkResult{1, 0, plan.UpdateInfo{1, 1, 0}}}},
			},
			{
				Query:    "select * from sales;",
				Expected: []sql.Row{{1902}},
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET t.year_built = 1903;",
				ExpectedErr: plan.ErrUpdateForTableNotSupported,
			},
			{
				Query:       "UPDATE sales JOIN (SELECT year_built FROM sales) AS t SET sales.year_built = 2030;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET s1.year_built = 2030;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET t.year_built = 2030;",
				ExpectedErr: plan.ErrUpdateForTableNotSupported,
			},
		},
	},
}
