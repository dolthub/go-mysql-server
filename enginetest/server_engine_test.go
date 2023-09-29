package enginetest_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"

	gosql "database/sql"
	sqle "github.com/dolthub/go-mysql-server"
	_ "github.com/go-sql-driver/mysql"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/require"
)

var (
	address = "localhost"
	noUserFmt = "no_user:@tcp(%s:%d)/"
)

func findEmptyPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return -1, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		return -1, err

	}
	return port, nil
}

// initTestServer initializes an in-memory server with the given port, but does not start it.
func initTestServer(port int) (*server.Server, error) {
	pro := memory.NewDBProvider()
	engine := sqle.NewDefault(pro)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	sessBuilder := func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		return memory.NewSession(sql.NewBaseSession(), pro), nil
	}
	s, err := server.NewServer(config, engine, sessBuilder, nil)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// TestSanity checks that an in-memory server can be started and stopped without error.
func TestSanity(t *testing.T) {
	port, err := findEmptyPort()
	require.NoError(t, err)

	s, err := initTestServer(port)
	require.NoError(t, err)
	go s.Start()
	defer s.Close()

	conn, err := dbr.Open("mysql", fmt.Sprintf(noUserFmt, address, port), nil)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Ping())
}

type serverScriptTestAssertion struct {
	query  string
	isExec bool
	args   []any
	skip   bool

	expectedErr          bool
	expectedRowsAffected int64
	expectedRows         []any

	// can't avoid writing custom comparator because of how gosql.Rows.Scan() works
	checkRows func(rows *gosql.Rows, expectedRows []any) (bool, error)
}

type serverScriptTest struct {
	name string
	setup []string
	assertions []serverScriptTestAssertion
}

func TestServerPreparedStatements(t *testing.T) {
	port, perr := findEmptyPort()
	require.NoError(t, perr)

	s, serr := initTestServer(port)
	require.NoError(t, serr)
	go s.Start()
	defer s.Close()

	conn, cerr := dbr.Open("mysql", fmt.Sprintf(noUserFmt, address, port), nil)
	require.NoError(t, cerr)
	defer conn.Close()

	tests := []serverScriptTest{
		{
			name: "prepared inserts with big ints",
			setup: []string{
				"create database test_db;",
				"use test_db;",
				"create table signed_tbl (i bigint signed);",
				"create table unsigned_tbl (i bigint unsigned);",
			},
			assertions: []serverScriptTestAssertion{
				{
					query: "insert into unsigned_tbl values (?)",
					args: []any{uint64(math.MaxInt64)},
					isExec: true,
					expectedRowsAffected: 1,
				},
				{
					query: "insert into unsigned_tbl values (?)",
					args: []any{uint64(math.MaxInt64+1)},
					isExec: true,
					expectedRowsAffected: 1,
				},
				{
					query: "insert into unsigned_tbl values (?)",
					args: []any{uint64(math.MaxUint64)},
					isExec: true,
					expectedRowsAffected: 1,
				},
				{
					query: "insert into unsigned_tbl values (?)",
					args: []any{int64(-1)},
					isExec: true,
					expectedErr: true,
				},
				{
					query: "insert into unsigned_tbl values (?)",
					args: []any{int64(math.MinInt64)},
					isExec: true,
					expectedErr: true,
				},
				{
					query: "select * from unsigned_tbl order by i",
					expectedRows: []any{
						[]uint64{uint64(math.MaxInt64)},
						[]uint64{uint64(math.MaxInt64 + 1)},
						[]uint64{uint64(math.MaxUint64)},
					},
					checkRows: func(rows *gosql.Rows, expectedRows []any) (bool, error) {
						var i uint64
						var rowNum int
						for rows.Next() {
							if err := rows.Scan(&i); err != nil {
								return false, err
							}
							if rowNum >= len(expectedRows) {
								return false, nil
							}
							if i != expectedRows[rowNum].([]uint64)[0] {
								return false, nil
							}
							rowNum++
						}
						return true, nil
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, stmt := range test.setup {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}
			for _, assertion := range test.assertions {
				if assertion.skip {
					continue
				}
				if assertion.isExec {
					res, err := conn.Exec(assertion.query, assertion.args...)
					if assertion.expectedErr {
						require.Error(t, err)
						continue
					}
					rowsAffected, err := res.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, assertion.expectedRowsAffected, rowsAffected)
					continue
				}
				rows, err := conn.Query(assertion.query, assertion.args...)
				if assertion.expectedErr {
					require.Error(t, err)
					continue
				}
				ok, err := assertion.checkRows(rows, assertion.expectedRows)
				require.NoError(t, err)
				require.True(t, ok)
			}
		})
	}
}