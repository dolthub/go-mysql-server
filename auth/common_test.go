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

package auth_test

import (
	"context"
	dsql "database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

const port = 33336

func authEngine(au auth.Auth) (*sqle.Engine, *sql.IndexRegistry, error) {
	db := memory.NewDatabase("test")
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	idxReg := sql.NewIndexRegistry()

	tblName := "test"

	table := memory.NewTable(tblName, sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, Source: tblName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: tblName},
	})

	db.AddTable(tblName, table)

	a := analyzer.NewBuilder(catalog).Build()
	config := &sqle.Config{Auth: au}

	return sqle.New(catalog, a, config), idxReg, nil
}

func authServer(a auth.Auth) (*server.Server, *sql.IndexRegistry, error) {
	engine, idxReg, err := authEngine(a)
	if err != nil {
		return nil, nil, err
	}

	config := server.Config{
		Protocol:       "tcp",
		Address:        fmt.Sprintf("localhost:%d", port),
		Auth:           a,
		MaxConnections: 1000,
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		return nil, nil, err
	}

	go s.Start()

	return s, idxReg, nil
}

func connString(user, password string) string {
	return fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/test", user, password, port)
}

type authenticationTest struct {
	user     string
	password string
	success  bool
}

func testAuthentication(
	t *testing.T,
	a auth.Auth,
	tests []authenticationTest,
	extra func(t *testing.T, c authenticationTest),
) {
	t.Helper()
	req := require.New(t)

	s, _, err := authServer(a)
	req.NoError(err)

	for _, c := range tests {
		t.Run(fmt.Sprintf("%s-%s", c.user, c.password), func(t *testing.T) {
			r := require.New(t)

			var db *dsql.DB
			db, err = dsql.Open("mysql", connString(c.user, c.password))
			r.NoError(err)
			_, err = db.Query("SELECT 1")

			if c.success {
				r.NoError(err)
			} else {
				r.Error(err)
				r.Contains(err.Error(), "Access denied")
			}

			err = db.Close()
			r.NoError(err)

			if extra != nil {
				extra(t, c)
			}
		})
	}

	err = s.Close()
	req.NoError(err)
}

var queries = map[string]string{
	"select":       "select * from test",
	"create_index": "create index t on test (name)",
	"drop_index":   "drop index t on test",
	"insert":       "insert into test (id, name) values ('id', 'name')",
	"lock":         "lock tables test read",
	"unlock":       "unlock tables",
}

type authorizationTest struct {
	user    string
	query   string
	success bool
}

func testAuthorization(
	t *testing.T,
	a auth.Auth,
	tests []authorizationTest,
	extra func(t *testing.T, c authorizationTest),
) {
	t.Helper()
	req := require.New(t)

	e, idxReg, err := authEngine(a)
	req.NoError(err)

	for i, c := range tests {
		t.Run(fmt.Sprintf("%s-%s", c.user, c.query), func(t *testing.T) {
			req := require.New(t)

			session := sql.NewSession("localhost", sql.Client{Address: "client", User: c.user}, uint32(i))
			ctx := sql.NewContext(context.TODO(),
				sql.WithSession(session),
				sql.WithPid(uint64(i)),
				sql.WithIndexRegistry(idxReg),
				sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("test")

			_, _, err := e.Query(ctx, c.query)

			if c.success {
				req.NoError(err)
				return
			}

			req.Error(err)
			if extra != nil {
				extra(t, c)
			} else {
				req.True(auth.ErrNotAuthorized.Is(err))
			}
		})
	}
}

func testAudit(
	t *testing.T,
	a auth.Auth,
	tests []authorizationTest,
	extra func(t *testing.T, c authorizationTest),
) {
	t.Helper()
	req := require.New(t)

	s, _, err := authServer(a)
	req.NoError(err)

	for _, c := range tests {
		t.Run(c.query, func(t *testing.T) {
			r := require.New(t)

			var db *dsql.DB
			db, err = dsql.Open("mysql", connString(c.user, ""))
			r.NoError(err)
			_, err = db.Query(c.query)

			if c.success {
				r.NoError(err)
			} else {
				r.Error(err)
			}

			err = db.Close()
			r.NoError(err)

			if extra != nil {
				extra(t, c)
			}
		})
	}

	err = s.Close()
	req.NoError(err)
}
