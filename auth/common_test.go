package auth_test

import (
	"context"
	dsql "database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index/pilosa"
)

const port = 3336

func authEngine(au auth.Auth) (string, *sqle.Engine, error) {
	db := mem.NewDatabase("test")
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	tblName := "test"

	table := mem.NewTable(tblName, sql.Schema{
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
			req := require.New(t)

			db, err := dsql.Open("mysql", connString(c.user, c.password))
			req.NoError(err)
			_, err = db.Query("SELECT 1")

			if c.success {
				req.NoError(err)
			} else {
				req.Error(err)
				req.Contains(err.Error(), "Access denied")
			}

			err = db.Close()
			req.NoError(err)

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
		t.Run(fmt.Sprintf("%s", c.user), func(t *testing.T) {
			req := require.New(t)

			db, err := dsql.Open("mysql", connString(c.user, ""))
			req.NoError(err)
			_, err = db.Query(c.query)

			if c.success {
				req.NoError(err)
			} else {
				req.Error(err)
			}

			err = db.Close()
			req.NoError(err)

			if extra != nil {
				extra(t, c)
			}
		})
	}

	err = s.Close()
	req.NoError(err)
}
