package enginetest_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"net"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

var (
	address   = "localhost"
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
	s, err := server.NewServer(config, engine, sql.NewContext, sessBuilder, nil)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// TestSmoke checks that an in-memory server can be started and stopped without error.
func TestSmoke(t *testing.T) {
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

	expectErr            bool
	expectedRowsAffected int64
	expectedRows         []any

	// can't avoid writing custom comparator because of how gosql.Rows.Scan() works
	checkRows func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error)
}

type serverScriptTest struct {
	name       string
	setup      []string
	assertions []serverScriptTestAssertion
}

func TestServerPreparedStatements(t *testing.T) {
	tests := []serverScriptTest{
		{
			name: "read json-wrapped decimal values",
			setup: []string{
				"create table json_tbl (id int primary key, j json);",
				"insert into json_tbl values (0, cast(213.4 as json));",
				`insert into json_tbl values (1, cast("213.4" as json));`,
			},
			assertions: []serverScriptTestAssertion{
				{
					query:  "select cast(321.4 as json)",
					isExec: false,
					expectedRows: []any{
						[]float64{321.4},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
						var i float64
						var rowNum int
						for rows.Next() {
							if err := rows.Scan(&i); err != nil {
								return false, err
							}
							if rowNum >= len(expectedRows) {
								return false, nil
							}
							if i != expectedRows[rowNum].([]float64)[0] {
								return false, nil
							}
							rowNum++
						}
						return true, nil
					},
				},
				{
					query:  "select j from json_tbl",
					isExec: false,
					expectedRows: []any{
						[]float64{213.4},
						[]float64{213.4},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
						var i float64
						var rowNum int
						for rows.Next() {
							if err := rows.Scan(&i); err != nil {
								return false, err
							}
							if rowNum >= len(expectedRows) {
								return false, nil
							}
							if i != expectedRows[rowNum].([]float64)[0] {
								return false, nil
							}
							rowNum++
						}
						return true, nil
					},
				},
			},
		},
		{
			name: "prepared inserts with big ints",
			setup: []string{
				"create table signed_tbl (i bigint signed);",
				"create table unsigned_tbl (i bigint unsigned);",
			},
			assertions: []serverScriptTestAssertion{
				{
					query:                "insert into unsigned_tbl values (?)",
					args:                 []any{uint64(math.MaxInt64)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query:                "insert into unsigned_tbl values (?)",
					args:                 []any{uint64(math.MaxInt64 + 1)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query:                "insert into unsigned_tbl values (?)",
					args:                 []any{uint64(math.MaxUint64)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query:     "insert into unsigned_tbl values (?)",
					args:      []any{int64(-1)},
					isExec:    true,
					expectErr: true,
				},
				{
					query:     "insert into unsigned_tbl values (?)",
					args:      []any{int64(math.MinInt64)},
					isExec:    true,
					expectErr: true,
				},
				{
					query: "select * from unsigned_tbl order by i",
					expectedRows: []any{
						[]uint64{uint64(math.MaxInt64)},
						[]uint64{uint64(math.MaxInt64 + 1)},
						[]uint64{uint64(math.MaxUint64)},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
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

				{
					query:                "insert into signed_tbl values (?)",
					args:                 []any{uint64(math.MaxInt64)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query:     "insert into signed_tbl values (?)",
					args:      []any{uint64(math.MaxInt64 + 1)},
					isExec:    true,
					expectErr: true,
				},
				{
					query:                "insert into signed_tbl values (?)",
					args:                 []any{int64(-1)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query:                "insert into signed_tbl values (?)",
					args:                 []any{int64(math.MinInt64)},
					isExec:               true,
					expectedRowsAffected: 1,
				},
				{
					query: "select * from signed_tbl order by i",
					expectedRows: []any{
						[]int64{int64(math.MinInt64)},
						[]int64{int64(-1)},
						[]int64{int64(math.MaxInt64)},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
						var i int64
						var rowNum int
						for rows.Next() {
							if err := rows.Scan(&i); err != nil {
								return false, err
							}
							if rowNum >= len(expectedRows) {
								return false, fmt.Errorf("expected %d rows, got more", len(expectedRows))
							}
							if i != expectedRows[rowNum].([]int64)[0] {
								return false, fmt.Errorf("expected %d, got %d", expectedRows[rowNum].([]int64)[0], i)
							}
							rowNum++
						}
						return true, nil
					},
				},
			},
		},
		{
			name: "regression test for incorrectly setting QFlagMax1Row flag",
			setup: []string{
				"create table test(c0 int not null, c1 int not null, pk int primary key, key (c0, c1));",
				"insert into test values (2, 3, 1), (5, 6, 4), (2, 3, 7);",
			},
			assertions: []serverScriptTestAssertion{
				{
					query: "select * from test where c0 = 2 and c1 = 3 order by pk;",
					expectedRows: []any{
						[]uint64{uint64(2), uint64(3), uint64(1)},
						[]uint64{uint64(2), uint64(3), uint64(7)},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
						var c0, c1, pk uint64
						var rowNum int
						for rows.Next() {
							err := rows.Scan(&c0, &c1, &pk)
							require.NoError(t, err)
							if err != nil {
								return false, err
							}
							require.Less(t, rowNum, len(expectedRows))
							if rowNum >= len(expectedRows) {
								return false, nil
							}
							require.Equal(t, c0, expectedRows[rowNum].([]uint64)[0])
							if c0 != expectedRows[rowNum].([]uint64)[0] {
								return false, nil
							}
							require.Equal(t, c1, expectedRows[rowNum].([]uint64)[1])
							if c1 != expectedRows[rowNum].([]uint64)[1] {
								return false, nil
							}
							require.Equal(t, pk, expectedRows[rowNum].([]uint64)[2])
							if pk != expectedRows[rowNum].([]uint64)[2] {
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

	port, perr := findEmptyPort()
	require.NoError(t, perr)

	s, serr := initTestServer(port)
	require.NoError(t, serr)
	go s.Start()
	defer s.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, cerr := dbr.Open("mysql", fmt.Sprintf(noUserFmt, address, port), nil)
			require.NoError(t, cerr)
			defer conn.Close()

			commonSetup := []string{
				"create database test_db;",
				"use test_db;",
			}
			commonTeardown := []string{
				"drop database test_db",
			}
			for _, stmt := range append(commonSetup, test.setup...) {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}
			for _, assertion := range test.assertions {
				t.Run(assertion.query, func(t *testing.T) {
					if assertion.skip {
						t.Skip()
					}
					if assertion.isExec {
						res, err := conn.Exec(assertion.query, assertion.args...)
						if assertion.expectErr {
							require.Error(t, err)
							return
						}
						require.NoError(t, err)
						rowsAffected, err := res.RowsAffected()
						require.NoError(t, err)
						require.Equal(t, assertion.expectedRowsAffected, rowsAffected)
						return
					}
					rows, err := conn.Query(assertion.query, assertion.args...)
					if assertion.expectErr {
						require.Error(t, err)
						return
					} else {
						require.NoError(t, err)
					}
					ok, err := assertion.checkRows(t, rows, assertion.expectedRows)
					require.NoError(t, err)
					require.True(t, ok)
				})
			}
			for _, stmt := range append(commonTeardown) {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}
		})
	}
}

func TestServerQueries(t *testing.T) {
	port, perr := findEmptyPort()
	require.NoError(t, perr)

	s, serr := initTestServer(port)
	require.NoError(t, serr)

	go s.Start()
	defer s.Close()

	tests := []serverScriptTest{
		{
			name:  "test that config variables are properly set",
			setup: []string{},
			assertions: []serverScriptTestAssertion{
				{
					query: "select @@hostname, @@port",
					//query:  "select @@hostname, @@port, @@max_connections",
					isExec: false,
					expectedRows: []any{
						sql.Row{"macbook.local", port},
					},
					checkRows: func(t *testing.T, rows *gosql.Rows, expectedRows []any) (bool, error) {
						var resHostname string
						var resPort int
						var rowNum int
						for rows.Next() {
							if err := rows.Scan(&resHostname, &resPort); err != nil {
								return false, err
							}
							if rowNum >= len(expectedRows) {
								return false, nil
							}
							expectedRow := expectedRows[rowNum].(sql.Row)
							require.Equal(t, expectedRow[0].(string), resHostname)
							require.Equal(t, expectedRow[1].(int), resPort)
						}
						return true, nil
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, cerr := dbr.Open("mysql", fmt.Sprintf(noUserFmt, address, port), nil)
			require.NoError(t, cerr)
			defer conn.Close()
			commonSetup := []string{
				"create database test_db;",
				"use test_db;",
			}
			commonTeardown := []string{
				"drop database test_db",
			}
			for _, stmt := range append(commonSetup, test.setup...) {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}
			for _, assertion := range test.assertions {
				t.Run(assertion.query, func(t *testing.T) {
					if assertion.skip {
						t.Skip()
					}
					rows, err := conn.Query(assertion.query, assertion.args...)
					if assertion.expectErr {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)

					ok, err := assertion.checkRows(t, rows, assertion.expectedRows)
					require.NoError(t, err)
					require.True(t, ok)
				})
			}
			for _, stmt := range append(commonTeardown) {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}
		})
	}
}
