// Copyright 2020-2024 Dolthub, Inc.
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
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"regexp"
	"runtime/trace"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/netutil"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/internal/sockstate"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/iters"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ErrRowTimeout will be returned if the wait for the row is longer than the connection timeout
var ErrRowTimeout = errors.NewKind("row read wait bigger than connection timeout")

// ErrConnectionWasClosed will be returned if we try to use a previously closed connection
var ErrConnectionWasClosed = errors.NewKind("connection was closed")

// set this to true to get verbose error logging on every query (print a stack trace for most errors)
var verboseErrorLogging = false

const rowsBatch = 128

var tcpCheckerSleepDuration time.Duration = 5 * time.Second

type MultiStmtMode int

const (
	MultiStmtModeOff MultiStmtMode = 0
	MultiStmtModeOn  MultiStmtMode = 1
)

// Handler is a connection handler for a SQLe engine, implementing the Vitess mysql.Handler interface.
type Handler struct {
	e                 *sqle.Engine
	sm                *SessionManager
	readTimeout       time.Duration
	disableMultiStmts bool
	maxLoggedQueryLen int
	encodeLoggedQuery bool
	sel               ServerEventListener
}

var _ mysql.Handler = (*Handler)(nil)
var _ mysql.ExtendedHandler = (*Handler)(nil)
var _ mysql.BinlogReplicaHandler = (*Handler)(nil)
var _ sql.ContextProvider = (*Handler)(nil)

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	if h.sel != nil {
		h.sel.ClientConnected()
	}

	h.sm.AddConn(c)
	updateMaxUsedConnectionsStatusVariable()
	sql.StatusVariables.IncrementGlobal("Connections", 1)

	c.DisableClientMultiStatements = h.disableMultiStmts
	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).WithField("DisableClientMultiStatements", c.DisableClientMultiStatements).Infof("NewConnection")
}

func (h *Handler) ConnectionAborted(_ *mysql.Conn, _ string) error {
	sql.StatusVariables.IncrementGlobal("Aborted_connects", 1)
	return nil
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
	err := h.sm.SetDB(c, schemaName)
	if err != nil {
		logrus.WithField("database", schemaName).Errorf("unable to process ComInitDB: %s", err.Error())
		err = sql.CastSQLError(err)
	}
	return err
}

// ComPrepare parses, partially analyzes, and caches a prepared statement's plan
// with the given [c.ConnectionID].
func (h *Handler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	logrus.WithField("query", query).
		WithField("paramsCount", prepare.ParamsCount).
		WithField("statementId", prepare.StatementID).Debugf("preparing query")

	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return nil, err
	}
	var analyzed sql.Node
	if analyzer.PreparedStmtDisabled {
		analyzed, err = h.e.AnalyzeQuery(sqlCtx, query)
	} else {
		analyzed, err = h.e.PrepareQuery(sqlCtx, query)
	}
	if err != nil {
		logrus.WithField("query", query).Errorf("unable to prepare query: %s", err.Error())
		err := sql.CastSQLError(err)
		return nil, err
	}

	// A nil result signals to the handler that the query is not a SELECT statement.
	if nodeReturnsOkResultSchema(analyzed) || types.IsOkResultSchema(analyzed.Schema()) {
		return nil, nil
	}

	return schemaToFields(sqlCtx, analyzed.Schema()), nil
}

// These nodes will eventually return an OK result, but their intermediate forms here return a different schema
// than they will at execution time.
func nodeReturnsOkResultSchema(node sql.Node) bool {
	switch node.(type) {
	case *plan.InsertInto, *plan.Update, *plan.UpdateJoin, *plan.DeleteFrom:
		return true
	}
	return false
}

func (h *Handler) ComPrepareParsed(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement, prepare *mysql.PrepareData) (mysql.ParsedQuery, []*querypb.Field, error) {
	logrus.WithField("query", query).
		WithField("paramsCount", prepare.ParamsCount).
		WithField("statementId", prepare.StatementID).Debugf("preparing query")

	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := h.e.PrepareParsedQuery(sqlCtx, query, query, parsed)
	if err != nil {
		logrus.WithField("query", query).Errorf("unable to prepare query: %s", err.Error())
		err := sql.CastSQLError(err)
		return nil, nil, err
	}

	var fields []*querypb.Field
	// The return result fields should only be directly translated if it doesn't correspond to an OK result.
	// See comment in ComPrepare
	if !(nodeReturnsOkResultSchema(analyzed) || types.IsOkResultSchema(analyzed.Schema())) {
		fields = nil
	} else {
		fields = schemaToFields(sqlCtx, analyzed.Schema())
	}

	return analyzed, fields, nil
}

func (h *Handler) NewContext(ctx context.Context, c *mysql.Conn, query string) (*sql.Context, error) {
	return h.sm.NewContext(ctx, c, query)
}

func (h *Handler) ComBind(ctx context.Context, c *mysql.Conn, query string, parsedQuery mysql.ParsedQuery, prepare *mysql.PrepareData) (mysql.BoundQuery, []*querypb.Field, error) {
	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return nil, nil, err
	}

	stmt, ok := parsedQuery.(sqlparser.Statement)
	if !ok {
		return nil, nil, fmt.Errorf("parsedQuery must be a sqlparser.Statement, but got %T", parsedQuery)
	}

	bindingExprs, err := bindingsToExprs(prepare.BindVars)
	if err != nil {
		return nil, nil, err
	}

	queryPlan, err := h.e.BoundQueryPlan(sqlCtx, query, stmt, bindingExprs)
	if err != nil {
		return nil, nil, err
	}

	return queryPlan, schemaToFields(sqlCtx, queryPlan.Schema()), nil
}

func (h *Handler) ComExecuteBound(ctx context.Context, conn *mysql.Conn, query string, boundQuery mysql.BoundQuery, callback mysql.ResultSpoolFn) error {
	plan, ok := boundQuery.(sql.Node)
	if !ok {
		return fmt.Errorf("boundQuery must be a sql.Node, but got %T", boundQuery)
	}

	return h.errorWrappedComExec(ctx, conn, query, plan, callback)
}

func (h *Handler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	_, err := h.errorWrappedDoQuery(ctx, c, prepare.PrepareStmt, nil, MultiStmtModeOff, prepare.BindVars, func(res *sqltypes.Result, more bool) error {
		return callback(res)
	}, &sql.QueryFlags{})
	return err
}

// ComResetConnection implements the mysql.Handler interface.
//
// This command resets the connection's session, clearing out any cached prepared statements, locks, user and
// session variables. The currently selected database is preserved.
//
// The COM_RESET command can be sent manually through the mysql client by issuing the "resetconnection" (or "\x")
// client command.
func (h *Handler) ComResetConnection(c *mysql.Conn) error {
	logrus.WithField("connectionId", c.ConnectionID).Debug("COM_RESET_CONNECTION command received")

	// Grab the currently selected database name
	db := h.sm.GetCurrentDB(c)

	// Dispose of the connection's current session
	h.maybeReleaseAllLocks(c)
	h.e.CloseSession(c.ConnectionID)

	// Create a new session and set the current database
	err := h.sm.NewSession(context.Background(), c)
	if err != nil {
		return err
	}

	return h.sm.SetDB(c, db)
}

func (h *Handler) ParserOptionsForConnection(c *mysql.Conn) (sqlparser.ParserOptions, error) {
	ctx, err := h.sm.NewContext(context.Background(), c, "")
	if err != nil {
		return sqlparser.ParserOptions{}, err
	}
	return sql.LoadSqlMode(ctx).ParserOptions(), nil
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	defer func() {
		if h.sel != nil {
			h.sel.ClientDisconnected()
		}
	}()

	defer h.sm.RemoveConn(c)
	defer h.e.CloseSession(c.ConnectionID)

	h.maybeReleaseAllLocks(c)

	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).Infof("ConnectionClosed")
}

// maybeReleaseAllLocks makes a best effort attempt to release all locks on the given connection. If the attempt fails,
// an error is logged but not returned.
func (h *Handler) maybeReleaseAllLocks(c *mysql.Conn) {
	if ctx, err := h.sm.NewContextWithQuery(context.Background(), c, ""); err != nil {
		logrus.Errorf("unable to release all locks on session close: %s", err)
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	} else {
		_, err = h.e.LS.ReleaseAll(ctx)
		if err != nil {
			logrus.Errorf("unable to release all locks on session close: %s", err)
		}
		if err = h.e.Analyzer.Catalog.UnlockTables(ctx, c.ConnectionID); err != nil {
			logrus.Errorf("unable to unlock tables on session close: %s", err)
		}
	}
}

func (h *Handler) ComMultiQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) (string, error) {
	return h.errorWrappedDoQuery(ctx, c, query, nil, MultiStmtModeOn, nil, callback, &sql.QueryFlags{})
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) error {
	_, err := h.errorWrappedDoQuery(ctx, c, query, nil, MultiStmtModeOff, nil, callback, &sql.QueryFlags{})
	return err
}

// ComParsedQuery executes a pre-parsed SQL query on the SQLe engine.
func (h *Handler) ComParsedQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	parsed sqlparser.Statement,
	callback mysql.ResultSpoolFn,
) error {
	_, err := h.errorWrappedDoQuery(ctx, c, query, parsed, MultiStmtModeOff, nil, callback, &sql.QueryFlags{})
	return err
}

// ComRegisterReplica implements the mysql.BinlogReplicaHandler interface.
func (h *Handler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	// TODO: replicaUser and replicaPassword should be validated at this layer (GMS);
	//       confirm if that has been done already (doesn't seem likely)

	logrus.StandardLogger().
		WithField("connectionId", c.ConnectionID).
		WithField("replicaHost", replicaHost).
		WithField("replicaPort", replicaPort).
		Debug("Handling COM_REGISTER_REPLICA")

	if !h.e.Analyzer.Catalog.HasBinlogPrimaryController() {
		return nil
	}

	newCtx := sql.NewContext(context.Background())
	primaryController := h.e.Analyzer.Catalog.GetBinlogPrimaryController()
	return primaryController.RegisterReplica(newCtx, c, replicaHost, replicaPort)
}

// ComBinlogDumpGTID implements the mysql.BinlogReplicaHandler interface.
func (h *Handler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet mysql.GTIDSet) error {
	if !h.e.Analyzer.Catalog.HasBinlogPrimaryController() {
		return nil
	}

	logrus.StandardLogger().
		WithField("connectionId", c.ConnectionID).
		Debug("Handling COM_BINLOG_DUMP_GTID")

	// TODO: is logfile and logpos ever actually needed for COM_BINLOG_DUMP_GTID?
	newCtx := sql.NewContext(context.Background())
	primaryController := h.e.Analyzer.Catalog.GetBinlogPrimaryController()
	return primaryController.BinlogDumpGtid(newCtx, c, gtidSet)
}

var queryLoggingRegex = regexp.MustCompile(`[\r\n\t ]+`)

func (h *Handler) doQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	parsed sqlparser.Statement,
	analyzedPlan sql.Node,
	mode MultiStmtMode,
	queryExec QueryExecutor,
	bindings map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result, bool) error,
	qFlags *sql.QueryFlags,
) (string, error) {
	sqlCtx, err := h.sm.NewContext(ctx, c, query)
	if err != nil {
		return "", err
	}

	start := time.Now()

	var remainder string
	var prequery string
	if parsed == nil {
		_, inPreparedCache := h.e.PreparedDataCache.GetCachedStmt(sqlCtx.Session.ID(), query)
		if mode == MultiStmtModeOn && !inPreparedCache {
			parsed, prequery, remainder, err = h.e.Parser.Parse(sqlCtx, query, true)
			if prequery != "" {
				query = prequery
			}
		}
	}

	sqlCtx.SetParsedQuery(parsed)

	more := remainder != ""

	var queryStr string
	if h.encodeLoggedQuery {
		queryStr = base64.StdEncoding.EncodeToString([]byte(query))
	} else if logrus.IsLevelEnabled(logrus.DebugLevel) {
		// this is expensive, so skip this unless we're logging at DEBUG level
		queryStr = string(queryLoggingRegex.ReplaceAll([]byte(query), []byte(" ")))
		if h.maxLoggedQueryLen > 0 && len(queryStr) > h.maxLoggedQueryLen {
			queryStr = queryStr[:h.maxLoggedQueryLen] + "..."
		}
	}

	if queryStr != "" {
		sqlCtx.SetLogger(sqlCtx.GetLogger().WithField("query", queryStr))
	}
	sqlCtx.GetLogger().Debugf("Starting query")

	finish := observeQuery(sqlCtx, query)
	defer finish(err)

	sqlCtx.GetLogger().Tracef("beginning execution")

	oCtx := ctx

	// TODO: it would be nice to put this logic in the engine, not the handler, but we don't want the process to be
	//  marked done until we're done spooling rows over the wire
	ctx, err = sqlCtx.ProcessList.BeginQuery(sqlCtx, query)
	defer func() {
		if err != nil && ctx != nil {
			sqlCtx.ProcessList.EndQuery(sqlCtx)
		}
	}()

	qFlags.Set(sql.QFlagDeferProjections)
	schema, rowIter, qFlags, err := queryExec(sqlCtx, query, parsed, analyzedPlan, bindings, qFlags)
	if err != nil {
		sqlCtx.GetLogger().WithError(err).Warn("error running query")
		if verboseErrorLogging {
			fmt.Printf("Err: %+v", err)
		}
		return remainder, err
	}

	// create result before goroutines to avoid |ctx| racing
	resultFields := schemaToFields(sqlCtx, schema)
	var r *sqltypes.Result
	var processedAtLeastOneBatch bool

	// zero/single return schema use spooling shortcut
	if types.IsOkResultSchema(schema) {
		r, err = resultForOkIter(sqlCtx, rowIter)
	} else if schema == nil {
		r, err = resultForEmptyIter(sqlCtx, rowIter, resultFields)
	} else if analyzer.FlagIsSet(qFlags, sql.QFlagMax1Row) {
		r, err = resultForMax1RowIter(sqlCtx, schema, rowIter, resultFields)
	} else {
		r, processedAtLeastOneBatch, err = h.resultForDefaultIter(sqlCtx, c, schema, rowIter, callback, resultFields, more)
	}
	if err != nil {
		return remainder, err
	}

	// errGroup context is now canceled
	ctx = oCtx

	if err = setConnStatusFlags(sqlCtx, c); err != nil {
		return remainder, err
	}

	switch len(r.Rows) {
	case 0:
		if len(r.Info) > 0 {
			sqlCtx.GetLogger().Tracef("returning result %s", r.Info)
		} else {
			sqlCtx.GetLogger().Tracef("returning empty result")
		}
	case 1:
		sqlCtx.GetLogger().Tracef("returning result %v", r)
	}

	sqlCtx.GetLogger().Debugf("Query finished in %d ms", time.Since(start).Milliseconds())

	// processedAtLeastOneBatch means we already called callback() at least
	// once, so no need to call it if RowsAffected == 0.
	if r != nil && (r.RowsAffected == 0 && processedAtLeastOneBatch) {
		return remainder, nil
	}

	return remainder, callback(r, more)
}

// resultForOkIter reads a maximum of one result row from a result iterator.
func resultForOkIter(ctx *sql.Context, iter sql.RowIter) (*sqltypes.Result, error) {
	defer trace.StartRegion(ctx, "Handler.resultForOkIter").End()

	row, err := iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	_, err = iter.Next(ctx)
	if err != io.EOF {
		return nil, fmt.Errorf("result schema iterator returned more than one row")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}
	return resultFromOkResult(row[0].(types.OkResult)), nil
}

// resultForEmptyIter ensures that an expected empty iterator returns no rows.
func resultForEmptyIter(ctx *sql.Context, iter sql.RowIter, resultFields []*querypb.Field) (*sqltypes.Result, error) {
	defer trace.StartRegion(ctx, "Handler.resultForEmptyIter").End()
	if _, err := iter.Next(ctx); err != io.EOF {
		return nil, fmt.Errorf("result schema iterator returned more than zero rows")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}
	return &sqltypes.Result{Fields: resultFields}, nil
}

// GetDeferredProjections looks for a top-level deferred projection, retrieves its projections, and removes it from the
// iterator tree.
func GetDeferredProjections(iter sql.RowIter) (sql.RowIter, []sql.Expression) {
	switch i := iter.(type) {
	case *rowexec.ExprCloserIter:
		_, projs := GetDeferredProjections(i.GetIter())
		return i, projs
	case *plan.TrackedRowIter:
		_, projs := GetDeferredProjections(i.GetIter())
		return i, projs
	case *rowexec.TransactionCommittingIter:
		newChild, projs := GetDeferredProjections(i.GetIter())
		if projs != nil {
			i.WithChildIter(newChild)
		}
		return i, projs
	case *iters.LimitIter:
		newChild, projs := GetDeferredProjections(i.ChildIter)
		if projs != nil {
			i.ChildIter = newChild
		}
		return i, projs
	case *rowexec.ProjectIter:
		if i.CanDefer() {
			return i.GetChildIter(), i.GetProjections()
		}
		return i, nil
	}
	return iter, nil
}

// resultForMax1RowIter ensures that an empty iterator returns at most one row
func resultForMax1RowIter(ctx *sql.Context, schema sql.Schema, iter sql.RowIter, resultFields []*querypb.Field) (*sqltypes.Result, error) {
	defer trace.StartRegion(ctx, "Handler.resultForMax1RowIter").End()
	row, err := iter.Next(ctx)
	if err == io.EOF {
		return &sqltypes.Result{Fields: resultFields}, nil
	} else if err != nil {
		return nil, err
	}

	if _, err = iter.Next(ctx); err != io.EOF {
		return nil, fmt.Errorf("result max1Row iterator returned more than one row")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}
	outputRow, err := RowToSQL(ctx, schema, row, nil)
	if err != nil {
		return nil, err
	}

	ctx.GetLogger().Tracef("spooling result row %s", outputRow)

	return &sqltypes.Result{Fields: resultFields, Rows: [][]sqltypes.Value{outputRow}, RowsAffected: 1}, nil
}

// resultForDefaultIter reads batches of rows from the iterator
// and writes results into the callback function.
func (h *Handler) resultForDefaultIter(
	ctx *sql.Context,
	c *mysql.Conn,
	schema sql.Schema,
	iter sql.RowIter,
	callback func(*sqltypes.Result, bool) error,
	resultFields []*querypb.Field,
	more bool) (r *sqltypes.Result, processedAtLeastOneBatch bool, returnErr error) {
	defer trace.StartRegion(ctx, "Handler.resultForDefaultIter").End()

	eg, ctx := ctx.NewErrgroup()

	pan2err := func() {
		if recoveredPanic := recover(); recoveredPanic != nil {
			returnErr = fmt.Errorf("handler caught panic: %v", recoveredPanic)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	// Read rows off the row iterator and send them to the row channel.
	iter, projs := GetDeferredProjections(iter)
	var rowChan = make(chan sql.Row, 512)
	eg.Go(func() error {
		defer pan2err()
		defer wg.Done()
		defer close(rowChan)
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				row, err := iter.Next(ctx)
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
	})

	pollCtx, cancelF := ctx.NewSubContext()
	eg.Go(func() error {
		defer pan2err()
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

	// Reads rows from the channel, converts them to wire format,
	// and calls |callback| to give them to vitess.
	eg.Go(func() error {
		defer pan2err()
		defer cancelF()
		defer wg.Done()
		for {
			if r == nil {
				r = &sqltypes.Result{Fields: resultFields}
			}
			if r.RowsAffected == rowsBatch {
				if err := callback(r, more); err != nil {
					return err
				}
				r = nil
				processedAtLeastOneBatch = true
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case row, ok := <-rowChan:
				if !ok {
					return nil
				}
				if types.IsOkResult(row) {
					if len(r.Rows) > 0 {
						panic("Got OkResult mixed with RowResult")
					}
					r = resultFromOkResult(row[0].(types.OkResult))
					continue
				}

				outputRow, err := RowToSQL(ctx, schema, row, projs)
				if err != nil {
					return err
				}

				ctx.GetLogger().Tracef("spooling result row %s", outputRow)
				r.Rows = append(r.Rows, outputRow)
				r.RowsAffected++
			case <-timer.C:
				// TODO: timer should probably go in its own thread, as rowChan is blocking
				if h.readTimeout != 0 {
					// Cancel and return so Vitess can call the CloseConnection callback
					ctx.GetLogger().Tracef("connection timeout")
					return ErrRowTimeout.New()
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
		defer pan2err()
		wg.Wait()
		return iter.Close(ctx)
	})

	err := eg.Wait()
	if err != nil {
		ctx.GetLogger().WithError(err).Warn("error running query")
		if verboseErrorLogging {
			fmt.Printf("Err: %+v", err)
		}
		returnErr = err
	}
	return
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
	return sql.ConvertToBool(ctx, autoCommitSessionVar)
}

// Call doQuery and cast known errors to SQLError
func (h *Handler) errorWrappedDoQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	parsed sqlparser.Statement,
	mode MultiStmtMode,
	bindings map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result, bool) error,
	qFlags *sql.QueryFlags,
) (string, error) {
	start := time.Now()
	if h.sel != nil {
		h.sel.QueryStarted()
	}

	remainder, err := h.doQuery(ctx, c, query, parsed, nil, mode, h.executeQuery, bindings, callback, qFlags)
	if err != nil {
		err = sql.CastSQLError(err)
	}

	if h.sel != nil {
		h.sel.QueryCompleted(err == nil, time.Since(start))
	}

	return remainder, err
}

// Call doQuery and cast known errors to SQLError
func (h *Handler) errorWrappedComExec(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	analyzedPlan sql.Node,
	callback func(*sqltypes.Result, bool) error,
) error {
	start := time.Now()
	if h.sel != nil {
		h.sel.QueryStarted()
	}

	_, err := h.doQuery(ctx, c, query, nil, analyzedPlan, MultiStmtModeOff, h.executeBoundPlan, nil, callback, nil)

	if err != nil {
		err = sql.CastSQLError(err)
	}

	if h.sel != nil {
		h.sel.QueryCompleted(err == nil, time.Since(start))
	}

	return err
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

func resultFromOkResult(result types.OkResult) *sqltypes.Result {
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

// getMaxUsedConnections returns the maximum number of connections that have been established at the same time for
// this sql-server, as tracked by the Max_used_connections status variable. If any error is encountered, it will be
// logged and 0 will be returned.
func getMaxUsedConnections() uint64 {
	_, maxUsedConnectionsValue, ok := sql.StatusVariables.GetGlobal("Max_used_connections")
	if !ok {
		logrus.Errorf("unable to find Max_used_connections status variable")
		return 0
	}
	maxUsedConnections, ok := maxUsedConnectionsValue.(uint64)
	if !ok {
		logrus.Errorf("unexpected type for Max_used_connections status variable: %T", maxUsedConnectionsValue)
		return 0
	}
	return maxUsedConnections
}

// getThreadsConnected returns the current number of connected threads, as tracked by the Threads_connected status
// variable. If any error is encountered, it will be logged and 0 will be returned.
func getThreadsConnected() uint64 {
	_, threadsConnectedValue, ok := sql.StatusVariables.GetGlobal("Threads_connected")
	if !ok {
		logrus.Errorf("unable to find Threads_connected status variable")
		return 0
	}
	threadsConnected, ok := threadsConnectedValue.(uint64)
	if !ok {
		logrus.Errorf("unexpected type for Threads_connected status variable: %T", threadsConnectedValue)
		return 0
	}
	return threadsConnected
}

// updateMaxUsedConnectionsStatusVariable updates the Max_used_connections status
// variables if the current number of connected threads is greater than the current
// value of Max_used_connections.
func updateMaxUsedConnectionsStatusVariable() {
	go func() {
		maxUsedConnections := getMaxUsedConnections()
		threadsConnected := getThreadsConnected()
		if threadsConnected > maxUsedConnections {
			sql.StatusVariables.SetGlobal("Max_used_connections", threadsConnected)
			// TODO: When Max_used_connections is updated, we should also update
			//       Max_used_connections_time with the current time, but our status
			//       variables support currently only supports Uint values.
		}
	}()
}

func RowToSQL(ctx *sql.Context, sch sql.Schema, row sql.Row, projs []sql.Expression) ([]sqltypes.Value, error) {
	// need to make sure the schema is not null as some plan schema is defined as null (e.g. IfElseBlock)
	if len(sch) == 0 {
		return []sqltypes.Value{}, nil
	}

	outVals := make([]sqltypes.Value, len(sch))
	if len(projs) == 0 {
		for i, col := range sch {
			if row[i] == nil {
				outVals[i] = sqltypes.NULL
				continue
			}
			var err error
			outVals[i], err = col.Type.SQL(ctx, nil, row[i])
			if err != nil {
				return nil, err
			}
		}
		return outVals, nil
	}

	for i, col := range sch {
		field, err := projs[i].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if field == nil {
			outVals[i] = sqltypes.NULL
			continue
		}
		outVals[i], err = col.Type.SQL(ctx, nil, field)
		if err != nil {
			return nil, err
		}
	}
	return outVals, nil
}

func schemaToFields(ctx *sql.Context, s sql.Schema) []*querypb.Field {
	charSetResults := ctx.GetCharacterSetResults()
	fields := make([]*querypb.Field, len(s))
	for i, c := range s {
		charset := uint32(sql.Collation_Default.CharacterSet())
		if collatedType, ok := c.Type.(sql.TypeWithCollation); ok {
			charset = uint32(collatedType.Collation().CharacterSet())
		}

		// Binary types always use a binary collation, but non-binary types must
		// respect character_set_results if it is set.
		if types.IsBinaryType(c.Type) {
			charset = uint32(sql.Collation_binary)
		} else if charSetResults != sql.CharacterSet_Unspecified {
			charset = uint32(charSetResults)
		}

		var flags querypb.MySqlFlag
		if !c.Nullable {
			flags = flags | querypb.MySqlFlag_NOT_NULL_FLAG
		}
		if c.AutoIncrement {
			flags = flags | querypb.MySqlFlag_AUTO_INCREMENT_FLAG
		}
		if c.PrimaryKey {
			flags = flags | querypb.MySqlFlag_PRI_KEY_FLAG
		}
		if types.IsUnsigned(c.Type) {
			flags = flags | querypb.MySqlFlag_UNSIGNED_FLAG
		}

		fields[i] = &querypb.Field{
			Name:         c.Name,
			OrgName:      c.Name,
			Table:        c.Source,
			OrgTable:     c.Source,
			Database:     c.DatabaseSource,
			Type:         c.Type.Type(),
			Charset:      charset,
			ColumnLength: c.Type.MaxTextResponseByteLength(ctx),
			Flags:        uint32(flags),
		}

		if types.IsDecimal(c.Type) {
			decimalType := c.Type.(sql.DecimalType)
			fields[i].Decimals = uint32(decimalType.Scale())
		} else if types.IsDatetimeType(c.Type) {
			dtType := c.Type.(sql.DatetimeType)
			fields[i].Decimals = uint32(dtType.Precision())
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
	span, ctx := ctx.Span("query", otel.WithAttributes(attribute.String("query", query)))

	t := time.Now()
	return func(err error) {
		if err != nil {
			QueryErrorCounter.With("query", query, "error", err.Error()).Add(1)
		} else {
			QueryCounter.With("query", query).Add(1)
			QueryHistogram.With("query", query, "duration", "seconds").Observe(time.Since(t).Seconds())
		}

		span.End()
	}
}

// QueryExecutor is a function that executes a query and returns the result as a schema and iterator. Either of
// |parsed| or |analyzed| can be nil depending on the use case
type QueryExecutor func(
	ctx *sql.Context,
	query string,
	parsed sqlparser.Statement,
	analyzed sql.Node,
	bindings map[string]*querypb.BindVariable,
	qFlags *sql.QueryFlags,
) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

// executeQuery is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *Handler) executeQuery(ctx *sql.Context, query string, parsed sqlparser.Statement, _ sql.Node, bindings map[string]*querypb.BindVariable, qFlags *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	bindingExprs, err := bindingsToExprs(bindings)
	if err != nil {
		return nil, nil, nil, err
	}
	return h.e.QueryWithBindings(ctx, query, parsed, bindingExprs, qFlags)
}

// executeQuery is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *Handler) executeBoundPlan(
	ctx *sql.Context,
	query string,
	_ sqlparser.Statement,
	plan sql.Node,
	_ map[string]*querypb.BindVariable,
	_ *sql.QueryFlags,
) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return h.e.PrepQueryPlanForExecution(ctx, query, plan)
}

func bindingsToExprs(bindings map[string]*querypb.BindVariable) (map[string]sqlparser.Expr, error) {
	res := make(map[string]sqlparser.Expr, len(bindings))
	for name, bv := range bindings {
		val, err := sqltypes.BindVariableToValue(bv)
		if err != nil {
			return nil, err
		}
		expr, err := sqlparser.ExprFromValue(val)
		if err != nil {
			return nil, err
		}
		res[name] = expr
	}
	return res, nil
}
