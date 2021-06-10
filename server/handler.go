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
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/netutil"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/internal/sockstate"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var regKillCmd = regexp.MustCompile(`^kill (?:(query|connection) )?(\d+)$`)

var errConnectionNotFound = errors.NewKind("connection not found: %c")

// ErrRowTimeout will be returned if the wait for the row is longer than the connection timeout
var ErrRowTimeout = errors.NewKind("row read wait bigger than connection timeout")

// ErrConnectionWasClosed will be returned if we try to use a previously closed connection
var ErrConnectionWasClosed = errors.NewKind("connection was closed")

var ErrUnsupportedOperation = errors.NewKind("unsupported operation")

// TODO parametrize
const rowsBatch = 100
const tcpCheckerSleepTime = 1

// Handler is a connection handler for a SQLe engine.
type Handler struct {
	mu          sync.Mutex
	e           *sqle.Engine
	sm          *SessionManager
	readTimeout time.Duration
}

// NewHandler creates a new Handler given a SQLe engine.
func NewHandler(e *sqle.Engine, sm *SessionManager, rt time.Duration) *Handler {
	return &Handler{
		e:           e,
		sm:          sm,
		readTimeout: rt,
	}
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	logrus.Infof("NewConnection: client %v", c.ConnectionID)
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
	return h.sm.SetDB(c, schemaName)
}

func (h *Handler) ComPrepare(c *mysql.Conn, query string) ([]*query.Field, error) {
	ctx, err := h.sm.NewContextWithQuery(c, query)
	if err != nil {
		return nil, err
	}
	schema, err := h.e.AnalyzeQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return schemaToFields(schema), nil
}

func (h *Handler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return h.errorWrappedDoQuery(c, prepare.PrepareStmt, prepare.BindVars, callback)
}

func (h *Handler) ComResetConnection(c *mysql.Conn) {
	// TODO: handle reset logic
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	ctx, _ := h.sm.NewContextWithQuery(c, "")
	h.sm.CloseConn(c)

	// If connection was closed, kill its associated queries.
	h.e.Catalog.ProcessList.Kill(c.ConnectionID)
	if err := h.e.Catalog.UnlockTables(ctx, c.ConnectionID); err != nil {
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	}

	logrus.Infof("ConnectionClosed: client %v", c.ConnectionID)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result) error,
) error {
	return h.errorWrappedDoQuery(c, query, nil, callback)
}

func bindingsToExprs(bindings map[string]*query.BindVariable) (map[string]sql.Expression, error) {
	res := make(map[string]sql.Expression, len(bindings))
	for k, v := range bindings {
		v, err := sqltypes.NewValue(v.Type, v.Value)
		if err != nil {
			return nil, err
		}
		switch {
		case v.Type() == sqltypes.Year:
			v, err := sql.Year.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, sql.Year)
		case sqltypes.IsSigned(v.Type()):
			v, err := strconv.ParseInt(string(v.ToBytes()), 0, 64)
			if err != nil {
				return nil, err
			}
			t := sql.Int64
			c, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case sqltypes.IsUnsigned(v.Type()):
			v, err := strconv.ParseUint(string(v.ToBytes()), 0, 64)
			if err != nil {
				return nil, err
			}
			t := sql.Uint64
			c, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case sqltypes.IsFloat(v.Type()):
			v, err := strconv.ParseFloat(string(v.ToBytes()), 64)
			if err != nil {
				return nil, err
			}
			t := sql.Float64
			c, err := t.Convert(v)
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(c, t)
		case v.Type() == sqltypes.Decimal:
			t := sql.MustCreateDecimalType(sql.DecimalTypeMaxPrecision, sql.DecimalTypeMaxScale)
			v, err := t.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Bit:
			t := sql.MustCreateBitType(sql.BitTypeMaxBits)
			v, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Null:
			res[k] = expression.NewLiteral(nil, sql.Null)
		case v.Type() == sqltypes.Blob || v.Type() == sqltypes.VarBinary || v.Type() == sqltypes.Binary:
			t, err := sql.CreateBinary(v.Type(), int64(len(v.ToBytes())))
			if err != nil {
				return nil, err
			}
			v, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Text || v.Type() == sqltypes.VarChar || v.Type() == sqltypes.Char:
			t, err := sql.CreateStringWithDefaults(v.Type(), int64(len(v.ToBytes())))
			if err != nil {
				return nil, err
			}
			v, err := t.Convert(v.ToBytes())
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Date || v.Type() == sqltypes.Datetime || v.Type() == sqltypes.Timestamp:
			t, err := sql.CreateDatetimeType(v.Type())
			if err != nil {
				return nil, err
			}
			v, err := t.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		case v.Type() == sqltypes.Time:
			t := sql.Time
			v, err := t.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, t)
		default:
			return nil, ErrUnsupportedOperation.New()
		}
	}
	return res, nil
}

func (h *Handler) doQuery(
	c *mysql.Conn,
	query string,
	bindings map[string]*query.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	logrus.Tracef("connection %d: received query %s", c.ConnectionID, query)

	handled, err := h.handleKill(c, query)
	if err != nil {
		return err
	}

	if handled {
		return callback(&sqltypes.Result{})
	}

	ctx, err := h.sm.NewContextWithQuery(c, query)
	if err != nil {
		return err
	}

	finish := observeQuery(ctx, query)
	defer finish(err)

	// TODO: it would be nice to put this logic in the engine, not the handler, but we don't want the process to be
	//  marked done until we're done spooling rows over the wire
	ctx, err = h.e.Catalog.AddProcess(ctx, query)
	defer func() {
		if err != nil && ctx != nil {
			h.e.Catalog.Done(ctx.Pid())
		}
	}()

	start := time.Now()

	parsed, _ := parse.Parse(ctx, query)
	switch n := parsed.(type) {
	case *plan.LoadData:
		if n.Local {
			// tell the connection to undergo the load data process with this metadata
			tmpdir, err := ctx.GetSessionVariable(ctx, "tmpdir")
			if err != nil {
				return err
			}
			err = c.HandleLoadDataLocalQuery(tmpdir.(string), plan.TmpfileName, n.File)
			if err != nil {
				return err
			}
		}
	}

	logrus.WithField("query", query).Tracef("connection %d: executing query", c.ConnectionID)

	var sqlBindings map[string]sql.Expression
	if len(bindings) > 0 {
		sqlBindings, err = bindingsToExprs(bindings)
		if err != nil {
			logrus.Tracef("Error processing bindings for query %s: %s", query, err)
			return err
		}
	}

	// TODO: it would be nice to put this logic in the engine itself, not the handler, but we wouldn't get accurate
	//  timing without some more work
	defer func() {
		if q, ok := h.e.Auth.(*auth.Audit); ok {
			q.Query(ctx, time.Since(start), err)
		}
	}()

	schema, rows, err := h.e.QueryNodeWithBindings(ctx, query, parsed, sqlBindings)
	if err != nil {
		logrus.Tracef("connection %d: error running query: %s", c.ConnectionID, err)
		return err
	}

	var r *sqltypes.Result
	var proccesedAtLeastOneBatch bool

	// Reads rows from the row reading goroutine
	rowChan := make(chan sql.Row)
	// To send errors from the two goroutines to the main one
	errChan := make(chan error)
	// To close the goroutines
	quit := make(chan struct{})

	// Default waitTime is one minute if there is no timeout configured, in which case
	// it will loop to iterate again unless the socket died by the OS timeout or other problems.
	// If there is a timeout, it will be enforced to ensure that Vitess has a chance to
	// call Handler.CloseConnection()
	waitTime := 1 * time.Minute

	if h.readTimeout > 0 {
		waitTime = h.readTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// Read rows off the row iterator and send them to the row channel.
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				row, err := rows.Next()
				if err != nil {
					errChan <- err
					return
				}
				rowChan <- row
			}
		}
	}()

	go h.pollForClosedConnection(c, errChan, quit)

rowLoop:
	for {
		if r == nil {
			r = &sqltypes.Result{Fields: schemaToFields(schema)}
		}

		if r.RowsAffected == rowsBatch {
			if err := callback(r); err != nil {
				close(quit)
				return err
			}

			r = nil
			proccesedAtLeastOneBatch = true
			continue
		}

		select {
		case err = <-errChan:
			if err == io.EOF {
				break rowLoop
			}

			logrus.Tracef("connection %d: got error %s", c.ConnectionID, err.Error())
			close(quit)
			return err
		case row := <-rowChan:
			if sql.IsOkResult(row) {
				if len(r.Rows) > 0 {
					panic("Got OkResult mixed with RowResult")
				}
				r = resultFromOkResult(row[0].(sql.OkResult))
				break rowLoop
			}

			outputRow, err := rowToSQL(schema, row)
			if err != nil {
				close(quit)
				return err
			}

			logrus.Tracef("connection %d spooling result row %s", c.ConnectionID, outputRow)
			r.Rows = append(r.Rows, outputRow)
			r.RowsAffected++
		case <-timer.C:
			if h.readTimeout != 0 {
				// Cancel and return so Vitess can call the CloseConnection callback
				logrus.Tracef("connection %d got timeout", c.ConnectionID)
				close(quit)
				return ErrRowTimeout.New()
			}
		}
		timer.Reset(waitTime)
	}
	close(quit)

	err = rows.Close(ctx)
	if err != nil {
		return err
	}

	if err = setConnStatusFlags(ctx, c); err != nil {
		return err
	}
	if err = setResultInfo(ctx, c, r, parsed); err != nil {
		return err
	}

	switch len(r.Rows) {
	case 0:
		logrus.Tracef("connection %d returning empty result", c.ConnectionID)
	case 1:
		logrus.Tracef("connection %d returning result %v", c.ConnectionID, r)
	}

	// TODO(andy): logic doesn't match comment?
	// Even if r.RowsAffected = 0, the callback must be
	// called to update the state in the go-vitess' listener
	// and avoid returning errors when the query doesn't
	// produce any results.
	if r != nil && (r.RowsAffected == 0 && proccesedAtLeastOneBatch) {
		return nil
	}

	return callback(r)
}

// See https://dev.mysql.com/doc/internals/en/status-flags.html
func setConnStatusFlags(ctx *sql.Context, c *mysql.Conn) error {
	ok, err := isSessionAutocommit(ctx)
	if err != nil {
		return err
	}
	if ok {
		c.StatusFlags |= uint16(mysql.ServerStatusAutocommit)
	} else {
		c.StatusFlags &= ^uint16(mysql.ServerStatusAutocommit)
	}

	if t := ctx.GetTransaction(); t != nil {
		c.StatusFlags |= uint16(mysql.ServerInTransaction)
	} else {
		c.StatusFlags &= ^uint16(mysql.ServerInTransaction)
	}

	return nil
}

func setResultInfo(ctx *sql.Context, conn *mysql.Conn, r *sqltypes.Result, parsedQuery sql.Node) error {
	lastId := ctx.Session.GetLastQueryInfo(sql.LastInsertId)
	r.InsertID = uint64(lastId)

	// cc. https://dev.mysql.com/doc/internals/en/capability-flags.html
	// Check if the CLIENT_FOUND_ROWS Compatibility Flag is set
	if shouldUseFoundRowsOutput(conn, parsedQuery) {
		r.RowsAffected = uint64(ctx.GetLastQueryInfo(sql.FoundRows))
	}

	return nil
}

// When CLIENT_FOUND_ROWS is set we should return the number of rows MATCHED as the number of affected.
// This should only happen on UPDATE and INSERT ON DUPLICATE queries
func shouldUseFoundRowsOutput(conn *mysql.Conn, parsedQuery sql.Node) bool {
	if (conn.Capabilities & mysql.CapabilityClientFoundRows) < 0 {
		return false
	}

	// TODO: Add support for INSERT ON DUPLICATE
	switch parsedQuery.(type) {
	case *plan.Update:
		return true
	default:
		return false
	}
}

func isSessionAutocommit(ctx *sql.Context) (bool, error) {
	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return sql.ConvertToBool(autoCommitSessionVar)
}

// Call doQuery and cast known errors to SQLError
func (h *Handler) errorWrappedDoQuery(
	c *mysql.Conn,
	query string,
	bindings map[string]*query.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	err := h.doQuery(c, query, bindings, callback)
	err, ok := sql.CastSQLError(err)
	if ok {
		return nil
	} else {
		return err
	}
}

// Periodically polls the connection socket to determine if it is has been closed by the client, sending an error on
// the supplied error channel if it has. Meant to be run in a separate goroutine from the query handler routine.
// Returns immediately on platforms that can't support TCP socket checks.
func (h *Handler) pollForClosedConnection(c *mysql.Conn, errChan chan error, quit chan struct{}) {
	tcpConn, ok := maybeGetTCPConn(c.Conn)
	if !ok {
		logrus.Debug("Connection checker exiting, connection isn't TCP")
		return
	}

	inode, err := sockstate.GetConnInode(tcpConn)
	if err != nil || inode == 0 {
		if !sockstate.ErrSocketCheckNotImplemented.Is(err) {
			logrus.Trace("Connection checker exiting, connection isn't TCP")
		}
		return
	}

	t, ok := tcpConn.LocalAddr().(*net.TCPAddr)
	if !ok {
		logrus.Debug("Connection checker exiting, could not get local port")
		return
	}

	for {
		select {
		case <-quit:
			return
		default:
		}

		st, err := sockstate.GetInodeSockState(t.Port, inode)
		switch st {
		case sockstate.Broken:
			logrus.Trace("socket state is broken, returning error")
			errChan <- ErrConnectionWasClosed.New()
			return
		case sockstate.Error:
			logrus.Infof("Connection checker exiting, got err checking sockstate: %v", err)
			return
		default: // Established
			// (juanjux) this check is not free, each iteration takes about 9 milliseconds to run on my machine
			// thus the small wait between checks
			time.Sleep(tcpCheckerSleepTime * time.Second)
		}
	}
}

func maybeGetTCPConn(conn net.Conn) (*net.TCPConn, bool) {
	wrap, ok := conn.(netutil.ConnWithTimeouts)
	if ok {
		conn = wrap.Conn
	}

	tcp, ok := conn.(*net.TCPConn)
	if ok {
		return tcp, true
	}

	return nil, false
}

func resultFromOkResult(result sql.OkResult) *sqltypes.Result {
	infoStr := ""
	if result.Info != nil {
		infoStr = result.Info.String()
	}
	return &sqltypes.Result{
		RowsAffected: result.RowsAffected,
		InsertID:     result.InsertID,
		Info:         infoStr,
	}
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery callback if the result does not contain any fields,
// or after the last ComQuery call completes.
func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	if sess := h.sm.session(c); sess != nil {
		return sess.WarningCount()
	}

	return 0
}

func (h *Handler) handleKill(conn *mysql.Conn, query string) (bool, error) {
	q := strings.ToLower(query)
	s := regKillCmd.FindStringSubmatch(q)
	if s == nil {
		return false, nil
	}

	logrus.Tracef("killing query %s", query)

	id, err := strconv.ParseUint(s[2], 10, 32)
	if err != nil {
		return false, err
	}

	// KILL CONNECTION and KILL should close the connection. KILL QUERY only
	// cancels the query.
	//
	// https://dev.mysql.com/doc/refman/8.0/en/kill.html
	//
	// KILL [CONNECTION | QUERY] processlist_id
	// - KILL QUERY terminates the statement the connection is currently executing,
	// but leaves the connection itself intact.

	// - KILL CONNECTION is the same as KILL with no modifier:
	// It terminates the connection associated with the given processlist_id,
	// after terminating any statement the connection is executing.
	connID := uint32(id)
	h.e.Catalog.Kill(connID)
	if s[1] != "query" {
		logrus.Infof("kill connection: id %d", connID)
		h.sm.CloseConn(conn)
		conn.Close()
	}

	return true, nil
}

func rowToSQL(s sql.Schema, row sql.Row) ([]sqltypes.Value, error) {
	o := make([]sqltypes.Value, len(row))
	var err error
	for i, v := range row {
		if v == nil {
			o[i] = sqltypes.NULL
			continue
		}

		o[i], err = s[i].Type.SQL(v)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func schemaToFields(s sql.Schema) []*query.Field {
	fields := make([]*query.Field, len(s))
	for i, c := range s {
		var charset uint32 = mysql.CharacterSetUtf8
		if sql.IsBlob(c.Type) {
			charset = mysql.CharacterSetBinary
		}

		fields[i] = &query.Field{
			Name:    c.Name,
			Type:    c.Type.Type(),
			Charset: charset,
		}
	}

	return fields
}

var (
	// QueryCounter describes a metric that accumulates number of queries monotonically.
	QueryCounter = discard.NewCounter()

	// QueryErrorCounter describes a metric that accumulates number of failed queries monotonically.
	QueryErrorCounter = discard.NewCounter()

	// QueryHistogram describes a queries latency.
	QueryHistogram = discard.NewHistogram()
)

func observeQuery(ctx *sql.Context, query string) func(err error) {
	span, _ := ctx.Span("query", opentracing.Tag{Key: "query", Value: query})

	t := time.Now()
	return func(err error) {
		if err != nil {
			QueryErrorCounter.With("query", query, "error", err.Error()).Add(1)
		} else {
			QueryCounter.With("query", query).Add(1)
			QueryHistogram.With("query", query, "duration", "seconds").Observe(time.Since(t).Seconds())
		}

		span.Finish()
	}
}
