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
	"github.com/dolthub/go-mysql-server/internal/sockstate"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
)

var errConnectionNotFound = errors.NewKind("connection not found: %c")

// ErrRowTimeout will be returned if the wait for the row is longer than the connection timeout
var ErrRowTimeout = errors.NewKind("row read wait bigger than connection timeout")

// ErrConnectionWasClosed will be returned if we try to use a previously closed connection
var ErrConnectionWasClosed = errors.NewKind("connection was closed")

var ErrUnsupportedOperation = errors.NewKind("unsupported operation")

const rowsBatch = 128

var tcpCheckerSleepDuration time.Duration = 1 * time.Second

type MultiStmtMode int

const (
	MultiStmtModeOff MultiStmtMode = 0
	MultiStmtModeOn  MultiStmtMode = 1
)

// Handler is a connection handler for a SQLe engine.
type Handler struct {
	mu                sync.Mutex
	e                 *sqle.Engine
	sm                *SessionManager
	readTimeout       time.Duration
	disableMultiStmts bool
	sel               ServerEventListener
}

// NewHandler creates a new Handler given a SQLe engine.
func NewHandler(e *sqle.Engine, sm *SessionManager, rt time.Duration, disableMultiStmts bool, listener ServerEventListener) *Handler {
	return &Handler{
		e:                 e,
		sm:                sm,
		readTimeout:       rt,
		disableMultiStmts: disableMultiStmts,
		sel:               listener,
	}
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	if h.sel != nil {
		h.sel.ClientConnected()
	}

	c.DisableClientMultiStatements = h.disableMultiStmts
	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).WithField("DisableClientMultiStatements", c.DisableClientMultiStatements).Infof("NewConnection")
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
	return h.sm.SetDB(c, schemaName)
}

// ComPrepare parses, partially analyzes, and caches a prepared statement's plan
// with the given [c.ConnectionID].
func (h *Handler) ComPrepare(c *mysql.Conn, query string) ([]*query.Field, error) {
	ctx, err := h.sm.NewContextWithQuery(c, query)
	if err != nil {
		return nil, err
	}

	var analyzed sql.Node
	if analyzer.PreparedStmtDisabled {
		analyzed, err = h.e.AnalyzeQuery(ctx, query)
	} else {
		analyzed, err = h.e.PrepareQuery(ctx, query)
	}
	if err != nil {
		return nil, err
	}

	if sql.IsOkResultSchema(analyzed.Schema()) {
		return nil, nil
	}
	return schemaToFields(analyzed.Schema()), nil
}

func (h *Handler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	_, err := h.errorWrappedDoQuery(c, prepare.PrepareStmt, MultiStmtModeOff, prepare.BindVars, func(res *sqltypes.Result, more bool) error {
		return callback(res)
	})
	return err
}

func (h *Handler) ComResetConnection(c *mysql.Conn) {
	// TODO: handle reset logic
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	defer func() {
		if h.sel != nil {
			h.sel.ClientDisconnected()
		}
	}()

	ctx, _ := h.sm.NewContextWithQuery(c, "")
	h.sm.CloseConn(c)

	// If connection was closed, kill its associated queries.
	ctx.ProcessList.Kill(c.ConnectionID)
	if err := h.e.Analyzer.Catalog.UnlockTables(ctx, c.ConnectionID); err != nil {
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	}

	defer h.e.CloseSession(ctx)

	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).Infof("ConnectionClosed")
}

func (h *Handler) ComMultiQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	return h.errorWrappedDoQuery(c, query, MultiStmtModeOn, nil, callback)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	_, err := h.errorWrappedDoQuery(c, query, MultiStmtModeOff, nil, callback)
	return err
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
			v, err := sql.InternalDecimalType.Convert(string(v.ToBytes()))
			if err != nil {
				return nil, err
			}
			res[k] = expression.NewLiteral(v, sql.InternalDecimalType)
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

var queryLoggingRegex = regexp.MustCompile(`[\r\n\t ]+`)

func (h *Handler) doQuery(
	c *mysql.Conn,
	query string,
	mode MultiStmtMode,
	bindings map[string]*query.BindVariable,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	ctx, err := h.sm.NewContext(c)
	if err != nil {
		return "", err
	}

	var remainder string
	var parsed sql.Node
	if mode == MultiStmtModeOn {
		var prequery string
		parsed, prequery, remainder, _ = parse.ParseOne(ctx, query)
		if prequery != "" {
			query = prequery
		}
	}

	ctx = ctx.WithQuery(query)
	more := remainder != ""

	ctx.SetLogger(ctx.GetLogger().
		WithField("query", string(queryLoggingRegex.ReplaceAll([]byte(query), []byte(" ")))))
	ctx.GetLogger().Debugf("Starting query")

	finish := observeQuery(ctx, query)
	defer finish(err)

	start := time.Now()

	if parsed == nil {
		parsed, _ = parse.Parse(ctx, query)
	}

	ctx.GetLogger().Tracef("beginning execution")

	var sqlBindings map[string]sql.Expression
	if len(bindings) > 0 {
		sqlBindings, err = bindingsToExprs(bindings)
		if err != nil {
			ctx.GetLogger().WithError(err).Errorf("Error processing bindings")
			return remainder, err
		}
	}

	oCtx := ctx
	eg, ctx := ctx.NewErrgroup()

	// TODO: it would be nice to put this logic in the engine, not the handler, but we don't want the process to be
	//  marked done until we're done spooling rows over the wire
	ctx, err = ctx.ProcessList.AddProcess(ctx, query)
	defer func() {
		if err != nil && ctx != nil {
			ctx.ProcessList.Done(ctx.Pid())
		}
	}()

	schema, rowIter, err := h.e.QueryNodeWithBindings(ctx, query, parsed, sqlBindings)
	if err != nil {
		ctx.GetLogger().WithError(err).Warn("error running query")
		return remainder, err
	}

	var rowChan chan sql.Row
	var row2Chan chan sql.Row2

	var rowIter2 sql.RowIter2
	if ri2, ok := rowIter.(sql.RowIterTypeSelector); ok && ri2.IsNode2() {
		rowIter2 = rowIter.(sql.RowIter2)
		row2Chan = make(chan sql.Row2, 512)
	} else {
		rowChan = make(chan sql.Row, 512)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	// Read rows off the row iterator and send them to the row channel.
	eg.Go(func() error {
		defer wg.Done()
		if rowIter2 != nil {
			defer close(row2Chan)

			frame := sql.NewRowFrame()
			defer frame.Recycle()

			for {
				frame.Clear()
				err := rowIter2.Next2(ctx, frame)
				if err != nil {
					if err == io.EOF {
						return rowIter2.Close(ctx)
					}
					cerr := rowIter2.Close(ctx)
					if cerr != nil {
						ctx.GetLogger().WithError(cerr).Warn("error closing row iter")
					}
					return err
				}
				select {
				case row2Chan <- frame.Row2Copy():
				case <-ctx.Done():
					return nil
				}
			}
		} else {
			defer close(rowChan)
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
					row, err := rowIter.Next(ctx)
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}
					select {
					case rowChan <- row:
					case <-ctx.Done():
						return nil
					}
				}
			}
		}
	})

	pollCtx, cancelF := ctx.NewSubContext()
	eg.Go(func() error {
		return h.pollForClosedConnection(pollCtx, c)
	})

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

	var r *sqltypes.Result
	var proccesedAtLeastOneBatch bool

	// reads rows from the channel, converts them to wire format,
	// and calls |callback| to give them to vitess.
	eg.Go(func() error {
		defer cancelF()
		defer wg.Done()
		for {
			if r == nil {
				r = &sqltypes.Result{Fields: schemaToFields(schema)}
			}

			if r.RowsAffected == rowsBatch {
				if err := callback(r, more); err != nil {
					return err
				}
				r = nil
				proccesedAtLeastOneBatch = true
				continue
			}

			if rowIter2 != nil {
				select {
				case <-ctx.Done():
					return nil
				case row, ok := <-row2Chan:
					if !ok {
						return nil
					}
					// TODO: OK result for Row2
					// if sql.IsOkResult(row) {
					// 	if len(r.Rows) > 0 {
					// 		panic("Got OkResult mixed with RowResult")
					// 	}
					// 	r = resultFromOkResult(row[0].(sql.OkResult))
					// 	continue
					// }

					outputRow, err := row2ToSQL(schema, row)
					if err != nil {
						return err
					}

					ctx.GetLogger().Tracef("spooling result row %s", outputRow)
					r.Rows = append(r.Rows, outputRow)
					r.RowsAffected++
				case <-timer.C:
					if h.readTimeout != 0 {
						// Cancel and return so Vitess can call the CloseConnection callback
						ctx.GetLogger().Tracef("connection timeout")
						return ErrRowTimeout.New()
					}
				}
			} else {
				select {
				case <-ctx.Done():
					return nil
				case row, ok := <-rowChan:
					if !ok {
						return nil
					}
					if sql.IsOkResult(row) {
						if len(r.Rows) > 0 {
							panic("Got OkResult mixed with RowResult")
						}
						r = resultFromOkResult(row[0].(sql.OkResult))
						continue
					}

					outputRow, err := rowToSQL(schema, row)
					if err != nil {
						return err
					}

					ctx.GetLogger().Tracef("spooling result row %s", outputRow)
					r.Rows = append(r.Rows, outputRow)
					r.RowsAffected++
				case <-timer.C:
					if h.readTimeout != 0 {
						// Cancel and return so Vitess can call the CloseConnection callback
						ctx.GetLogger().Tracef("connection timeout")
						return ErrRowTimeout.New()
					}
				}
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(waitTime)
		}
	})

	// Close() kills this PID in the process list,
	// wait until all rows have be sent over the wire
	eg.Go(func() error {
		wg.Wait()
		return rowIter.Close(ctx)
	})

	err = eg.Wait()
	if err != nil {
		ctx.GetLogger().WithError(err).Warn("error running query")
		return remainder, err
	}

	// errGroup context is now canceled
	ctx = oCtx

	if err = setConnStatusFlags(ctx, c); err != nil {
		return remainder, err
	}

	switch len(r.Rows) {
	case 0:
		if len(r.Info) > 0 {
			ctx.GetLogger().Debugf("returning result %s", r.Info)
		} else {
			ctx.GetLogger().Debugf("returning empty result")
		}
	case 1:
		ctx.GetLogger().Debugf("returning result %v", r)
	}

	ctx.GetLogger().Debugf("Query took %dms", time.Since(start).Milliseconds())

	// processedAtLeastOneBatch means we already called callback() at least
	// once, so no need to call it if RowsAffected == 0.
	if r != nil && (r.RowsAffected == 0 && proccesedAtLeastOneBatch) {
		return remainder, nil
	}

	return remainder, callback(r, more)
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
	mode MultiStmtMode,
	bindings map[string]*query.BindVariable,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	start := time.Now()
	if h.sel != nil {
		h.sel.QueryStarted()
	}

	remainder, err := h.doQuery(c, query, mode, bindings, callback)
	err, _, ok := sql.CastSQLError(err)

	var retErr error
	if !ok {
		retErr = err
	}

	if h.sel != nil {
		h.sel.QueryCompleted(retErr == nil, time.Since(start))
	}

	return remainder, retErr
}

// Periodically polls the connection socket to determine if it is has been closed by the client, returning an error
// if it has been. Meant to be run in an errgroup from the query handler routine. Returns immediately with no error
// on platforms that can't support TCP socket checks.
func (h *Handler) pollForClosedConnection(ctx *sql.Context, c *mysql.Conn) error {
	tcpConn, ok := maybeGetTCPConn(c.Conn)
	if !ok {
		ctx.GetLogger().Trace("Connection checker exiting, connection isn't TCP")
		return nil
	}

	inode, err := sockstate.GetConnInode(tcpConn)
	if err != nil || inode == 0 {
		if !sockstate.ErrSocketCheckNotImplemented.Is(err) {
			ctx.GetLogger().Trace("Connection checker exiting, connection isn't TCP")
		}
		return nil
	}

	t, ok := tcpConn.LocalAddr().(*net.TCPAddr)
	if !ok {
		ctx.GetLogger().Trace("Connection checker exiting, could not get local port")
		return nil
	}

	timer := time.NewTimer(tcpCheckerSleepDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		st, err := sockstate.GetInodeSockState(t.Port, inode)
		switch st {
		case sockstate.Broken:
			ctx.GetLogger().Warn("socket state is broken, returning error")
			return ErrConnectionWasClosed.New()
		case sockstate.Error:
			ctx.GetLogger().WithError(err).Warn("Connection checker exiting, got err checking sockstate")
			return nil
		default: // Established
			// (juanjux) this check is not free, each iteration takes about 9 milliseconds to run on my machine
			// thus the small wait between checks
			timer.Reset(tcpCheckerSleepDuration)
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

func rowToSQL(s sql.Schema, row sql.Row) ([]sqltypes.Value, error) {
	o := make([]sqltypes.Value, len(row))
	var err error
	for i, v := range row {
		if v == nil {
			o[i] = sqltypes.NULL
			continue
		}

		o[i], err = s[i].Type.SQL(nil, v)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func row2ToSQL(s sql.Schema, row sql.Row2) ([]sqltypes.Value, error) {
	o := make([]sqltypes.Value, len(row))
	var err error
	for i := 0; i < row.Len(); i++ {
		v := row.GetField(i)
		if v.IsNull() {
			o[i] = sqltypes.NULL
			continue
		}

		o[i], err = s[i].Type.(sql.Type2).SQL2(v)
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
		if sql.IsBinaryType(c.Type) {
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
