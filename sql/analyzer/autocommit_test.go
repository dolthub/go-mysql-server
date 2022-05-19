// Copyright 2022 DoltHub, Inc.
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

package analyzer

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"

	"github.com/stretchr/testify/require"
)

var testCasesWithoutCurrentDb = map[string]string{
	"CREATE PROCEDURE mydb.p1() SELECT 5": "mydb",
	"create database db123":               "db123",
}

var testCasesWithCurrentDb = map[string]string{
	"use db1":                                "db1",
	"select 1":                               "foo",
	"select * from t1":                       "foo",
	"call myFunction()":                      "foo",
	"insert into db2.t2 values(1, 2, 3)":     "db2",
	"insert into t2 values(42, 42)":          "foo",
	"create table db1.t3(i int primary key)": "db1",
	"create database db123":                  "db123",
}

var multiDbErrorTestCases = []string{
	"select * from db1.t1, db2.t2 where db1.t1.id = db2.t2.id",
	"select * from foo.t1 union select * from db1.t2",
	"create table t1 like db3.t1",
}

func TestGetTransactionDatabase(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewContext(context.Background())

	t.Run("transaction database detected with no current database", func(t *testing.T) {
		for query, expectedDatabase := range testCasesWithoutCurrentDb {
			t.Run(query, func(t *testing.T) {
				parsed, err := parse.Parse(ctx, query)
				require.NoError(err, "unable to parse test query: %s", query)

				database, err := GetTransactionDatabase(ctx, parsed)
				require.NoError(err)
				require.Equal(expectedDatabase, database)
			})
		}
	})

	ctx = ctx.WithCurrentDB("foo")
	t.Run("transaction database detected with current database", func(t *testing.T) {
		for query, expectedDatabase := range testCasesWithCurrentDb {
			t.Run(query, func(t *testing.T) {
				parsed, err := parse.Parse(ctx, query)
				require.NoError(err, "unable to parse test query: %s", query)

				database, err := GetTransactionDatabase(ctx, parsed)
				require.NoError(err)
				require.Equal(expectedDatabase, database)
			})
		}
	})

	t.Run("multi-database transactions return errors", func(t *testing.T) {
		for _, multiDbQuery := range multiDbErrorTestCases {
			t.Run(multiDbQuery, func(t *testing.T) {
				parsed, err := parse.Parse(ctx, multiDbQuery)
				require.NoError(err, "unable to parse test query: %s", multiDbQuery)

				_, err = GetTransactionDatabase(ctx, parsed)
				require.True(sql.ErrMultipleDatabaseTransaction.Is(err),
					"expected a multiple database error, but didn't get one")
			})
		}
	})
}
