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

package server

import (
	"context"
	"io/ioutil"
	"net"
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func setupMemDB(require *require.Assertions) *sqle.Engine {
	e := sqle.NewDefault()
	db := memory.NewDatabase("test")
	e.AddDatabase(db)

	tableTest := memory.NewTable("test", sql.Schema{{Name: "c1", Type: sql.Int32, Source: "test"}})

	for i := 0; i < 1010; i++ {
		require.NoError(tableTest.Insert(
			sql.NewEmptyContext(),
			sql.NewRow(int32(i)),
		))
	}

	db.AddTable("test", tableTest)

	return e
}

func getFreePort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

func testServer(t *testing.T, ready chan struct{}, port string, breakConn bool) {
	l, err := net.Listen("tcp", ":"+port)
	defer func() {
		_ = l.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	conn, err := l.Accept()
	if err != nil {
		return
	}

	if !breakConn {
		defer func() {
			_ = conn.Close()
		}()

		_, err = ioutil.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
	} // else: dirty return without closing or reading to force the socket into TIME_WAIT
}
func okTestServer(t *testing.T, ready chan struct{}, port string) {
	testServer(t, ready, port, false)
}
func brokenTestServer(t *testing.T, ready chan struct{}, port string) {
	testServer(t, ready, port, true)
}

// This session builder is used as dummy mysql Conn is not complete and
// causes panic when accessing remote address.
func testSessionBuilder(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error) {
	return sql.NewSession(addr, sql.Client{Address: "127.0.0.1:34567", User: c.User, Capabilities: c.Capabilities}, c.ConnectionID), sql.NewIndexRegistry(), sql.NewViewRegistry(), nil
}

type mockConn struct {
	net.Conn
}

func (c *mockConn) Close() error { return nil }

func newConn(id uint32) *mysql.Conn {
	conn := &mysql.Conn{
		ConnectionID: id,
	}

	// Set conn so it does not panic when we close it
	val := reflect.ValueOf(conn).Elem()
	field := val.FieldByName("Conn")
	field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	field.Set(reflect.ValueOf(new(mockConn)))

	return conn
}
