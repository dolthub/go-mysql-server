package server

import (
	"fmt"
	"net"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestBrokenConnection(t *testing.T) {
	require := require.New(t)
	e := setupMemDB(require)

	port, err := getFreePort()
	require.NoError(err)

	ready := make(chan struct{})
	go brokenTestServer(t, ready, port)
	<-ready
	conn, err := net.Dial("tcp", "localhost:"+port)
	require.NoError(err)

	h := NewHandler(
		e,
		NewSessionManager(
			testSessionBuilder,
			opentracing.NoopTracer{},
			sql.NewMemoryManager(nil),
			"foo",
		),
		0,
	)
	h.AddNetConnection(&conn)
	c := newConn(1)
	h.NewConnection(c)

	// (juanjux) Note that this is a little fuzzy because sometimes sockets take one or two seconds
	// to go into TIME_WAIT but 4 seconds hopefully is enough
	wait := tcpCheckerSleepTime * 2
	if wait < 4 {
		wait = 4
	}
	q := fmt.Sprintf("SELECT SLEEP(%d)", wait)
	err = h.ComQuery(c, q, func(res *sqltypes.Result) error {
		return nil
	})
	require.EqualError(err, "connection was closed")
}
