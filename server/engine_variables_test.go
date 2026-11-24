// Copyright 2026 Dolthub, Inc.
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
	"net"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

type variableTestListener struct{ addr net.Addr }

func (l variableTestListener) Addr() net.Addr { return l.addr }
func (l variableTestListener) Accept()        {}
func (l variableTestListener) Close()         {}

type variableNetListener struct{ addr net.Addr }

func (l variableNetListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l variableNetListener) Close() error              { return nil }
func (l variableNetListener) Addr() net.Addr            { return l.addr }

type replicationContextRecorder struct {
	binlogreplication.BinlogPrimaryController
	registered *sql.Context
	dumped     *sql.Context
}

func (r *replicationContextRecorder) RegisterReplica(ctx *sql.Context, _ *mysql.Conn, _ string, _ uint16) error {
	r.registered = ctx
	return nil
}

func (r *replicationContextRecorder) BinlogDumpGtid(ctx *sql.Context, _ *mysql.Conn, _ mysql.GTIDSet) error {
	r.dumped = ctx
	return nil
}

func newVariableTestServer(t *testing.T, vars *sql.EngineVariables, port int) (*Server, mysql.ListenerConfig) {
	t.Helper()
	provider := memory.NewDBProvider()
	e := sqle.New(analyzer.NewDefault(provider), &sqle.Config{EngineVariables: vars})
	l := variableNetListener{addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}}
	var listenerCfg mysql.ListenerConfig
	cfg := Config{
		Listener: l, ConnReadTimeout: 2 * time.Second, ConnWriteTimeout: 3 * time.Second,
		DisableConnectionWatcher: true,
		ProtocolListenerFactory: func(_ Config, lc mysql.ListenerConfig, _ ServerEventListener) (ProtocolListener, error) {
			listenerCfg = lc
			return variableTestListener{addr: l.Addr()}, nil
		},
	}
	s, err := NewServer(cfg, e, sql.ContextFactoryWithVariables(vars), memory.NewSessionBuilderWithVariables(provider, vars), nil)
	require.NoError(t, err)
	return s, listenerCfg
}

func TestIndependentServerLifecycleVariables(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	sa, la := newVariableTestServer(t, a, 33061)
	sb, lb := newVariableTestServer(t, b, 33062)
	pa, err := getPort(la)
	require.NoError(t, err)
	pb, err := getPort(lb)
	require.NoError(t, err)
	require.NotEqual(t, pa, pb)
	_, got, _ := a.System.GetGlobal("port")
	require.Equal(t, pa, got)
	_, got, _ = b.System.GetGlobal("port")
	require.Equal(t, pb, got)
	_, got, _ = a.System.GetGlobal("net_read_timeout")
	require.Equal(t, int64(2), got)
	_, got, _ = b.System.GetGlobal("net_write_timeout")
	require.Equal(t, int64(3), got)

	ca, cb := newConn(11), newConn(22)
	sa.handler.(*Handler).NewConnection(ca)
	sb.handler.(*Handler).NewConnection(cb)
	sa.handler.(*Handler).NewConnection(newConn(12))
	_, got, _ = a.Status.GetGlobal("Threads_connected")
	require.Equal(t, uint64(2), got)
	_, got, _ = b.Status.GetGlobal("Threads_connected")
	require.Equal(t, uint64(1), got)
	_, got, _ = a.Status.GetGlobal("Connections")
	require.Equal(t, uint64(2), got)
	_, got, _ = b.Status.GetGlobal("Connections")
	require.Equal(t, uint64(1), got)
	require.Eventually(t, func() bool {
		_, aMax, _ := a.Status.GetGlobal("Max_used_connections")
		_, bMax, _ := b.Status.GetGlobal("Max_used_connections")
		return aMax == uint64(2) && bMax == uint64(1)
	}, time.Second, time.Millisecond)
}

func TestReplicationCallbacksUseEngineContext(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	sa, _ := newVariableTestServer(t, a, 33064)
	sb, _ := newVariableTestServer(t, b, 33065)
	ra, rb := &replicationContextRecorder{}, &replicationContextRecorder{}
	sa.Engine.Analyzer.Catalog.BinlogPrimaryController = ra
	sb.Engine.Analyzer.Catalog.BinlogPrimaryController = rb
	ca, cb := newConn(41), newConn(42)
	require.NoError(t, sa.handler.(*Handler).ComRegisterReplica(ca, "replica-a", 3306, "", ""))
	require.NoError(t, sb.handler.(*Handler).ComRegisterReplica(cb, "replica-b", 3306, "", ""))
	require.Same(t, a, ra.registered.EngineVariables())
	require.Same(t, b, rb.registered.EngineVariables())
	require.NoError(t, sa.handler.(*Handler).ComBinlogDumpGTID(ca, "", 0, nil))
	require.NoError(t, sb.handler.(*Handler).ComBinlogDumpGTID(cb, "", 0, nil))
	require.Same(t, a, ra.dumped.EngineVariables())
	require.Same(t, b, rb.dumped.EngineVariables())
}

func TestServerRejectsWrongSessionAndOptionPairing(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	provider := memory.NewDBProvider()
	e := sqle.New(analyzer.NewDefault(provider), &sqle.Config{EngineVariables: a})
	sm := NewSessionManagerWithVariables(sql.NewContext, memory.NewSessionBuilderWithVariables(provider, b), sql.NoopTracer,
		e.Analyzer.Catalog.Database, e.MemoryManager, e.ProcessList, "", a)
	sm.engine = e
	err := sm.NewSession(context.Background(), newConn(1))
	require.ErrorContains(t, err, "session builder returned")
	wrongFactory := func(context.Context, ...sql.ContextOption) *sql.Context {
		return sql.NewContextWithVariables(context.Background(), b)
	}
	sm = NewSessionManagerWithVariables(wrongFactory, memory.NewSessionBuilderWithVariables(provider, a), sql.NoopTracer,
		e.Analyzer.Catalog.Database, e.MemoryManager, e.ProcessList, "", a)
	sm.engine = e
	sm.connWatcher = newConnWatcher(0, 0, true)
	_, err = sm.NewContextWithQuery(context.Background(), newConn(2), "SELECT 1")
	require.ErrorContains(t, err, "context factory returned")

	l := variableNetListener{addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 33063}}
	other := sqle.New(analyzer.NewDefault(memory.NewDBProvider()), &sqle.Config{EngineVariables: b})
	cfg := Config{Listener: l, DisableConnectionWatcher: true, Options: []Option{func(_ *sqle.Engine, sm *SessionManager, h mysql.Handler) (*sqle.Engine, *SessionManager, mysql.Handler) {
		return other, sm, h
	}}}
	_, err = NewServer(cfg, e, sql.NewContext, memory.NewSessionBuilderWithVariables(provider, a), nil)
	require.ErrorContains(t, err, "engine and session manager do not match")

	cfg.Options = []Option{func(e *sqle.Engine, sm *SessionManager, h mysql.Handler) (*sqle.Engine, *SessionManager, mysql.Handler) {
		e.ProcessList = sqle.NewProcessListWithVariables(a)
		return e, sm, h
	}}
	_, err = NewServer(cfg, e, sql.NewContext, memory.NewSessionBuilderWithVariables(provider, a), nil)
	require.ErrorContains(t, err, "process list and session manager do not match")
}
