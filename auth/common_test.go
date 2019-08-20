package auth_test

import (
	"context"
	dsql "database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/index/pilosa"
	"github.com/stretchr/testify/require"
)

const port = 3336

func authEngine(au auth.Auth) (string, *sqle.Engine, error) {
	db := memory.NewDatabase("test")
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	tblName := "test"

	table := memory.NewTable(tblName, sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, Source: tblName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: tblName},
	})

	db.AddTable(tblName, table)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	if err != nil {
		return "", nil, err
	}

	err = os.MkdirAll(tmpDir, 0644)
	if err != nil {
		return "", nil, err
	}

	catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	a := analyzer.NewBuilder(catalog).Build()
	config := &sqle.Config{Auth: au}

	return tmpDir, sqle.New(catalog, a, config), nil
}

func authServer(a auth.Auth) (string, *server.Server, error) {
	tmpDir, engine, err := authEngine(a)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, err
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("localhost:%d", port),
		Auth:     a,
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, err
	}

	go s.Start()

	return tmpDir, s, nil
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

	tmpDir, s, err := authServer(a)
	req.NoError(err)
	defer os.RemoveAll(tmpDir)

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
	"create_index": "create index t on test using pilosa (name) with (async = false)",
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

	tmpDir, e, err := authEngine(a)
	req.NoError(err)
	defer os.RemoveAll(tmpDir)

	for i, c := range tests {
		t.Run(fmt.Sprintf("%s-%s", c.user, c.query), func(t *testing.T) {
			req := require.New(t)

			session := sql.NewSession("localhost", "client", c.user, uint32(i))
			ctx := sql.NewContext(context.TODO(),
				sql.WithSession(session),
				sql.WithPid(uint64(i)))

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

	tmpDir, s, err := authServer(a)
	req.NoError(err)
	defer os.RemoveAll(tmpDir)

	for _, c := range tests {
		t.Run(c.user, func(t *testing.T) {
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
