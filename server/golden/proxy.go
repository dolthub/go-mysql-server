// Copyright 2022 Dolthub, Inc.
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

package golden

import (
	dsql "database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type MySqlProxy struct {
	ctx     *sql.Context
	connStr string
	logger  *logrus.Logger
	conns   map[uint32]proxyConn
}

func (h MySqlProxy) ParserOptionsForConnection(_ *mysql.Conn) (sqlparser.ParserOptions, error) {
	return sqlparser.ParserOptions{}, nil
}

type proxyConn struct {
	*dbr.Connection
	*logrus.Entry
}

// NewMySqlProxyHandler creates a new MySqlProxy.
func NewMySqlProxyHandler(logger *logrus.Logger, connStr string) (MySqlProxy, error) {
	// ensure parseTime=true
	cfg, err := mysql2.ParseDSN(connStr)
	if err != nil {
		return MySqlProxy{}, err
	}
	cfg.ParseTime = true
	connStr = cfg.FormatDSN()

	conn, err := newConn(connStr, 0, logger)
	if err != nil {
		return MySqlProxy{}, err
	}
	defer func() { _ = conn.Close() }()

	if err = conn.Ping(); err != nil {
		return MySqlProxy{}, err
	}

	return MySqlProxy{
		ctx:     sql.NewEmptyContext(),
		connStr: connStr,
		logger:  logger,
		conns:   make(map[uint32]proxyConn),
	}, nil
}

var _ mysql.Handler = MySqlProxy{}

func newConn(connStr string, connId uint32, lgr *logrus.Logger) (conn proxyConn, err error) {
	l := logrus.NewEntry(lgr).WithField("dsn", connStr).WithField(sql.ConnectionIdLogField, connId)
	var c *dbr.Connection
	for d := 100.0; d < 10000.0; d *= 1.6 {
		l.Debugf("Attempting connection to MySQL")
		if c, err = dbr.Open("mysql", connStr, nil); err == nil {
			if err = c.Ping(); err == nil {
				break
			}
		}
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	if err != nil {
		l.Debugf("Failed to establish connection %d", connId)
		return proxyConn{}, err
	}
	l.Debugf("Succesfully established connection")
	return proxyConn{Connection: c, Entry: l}, nil
}

// NewConnection implements mysql.Handler.
func (h MySqlProxy) NewConnection(c *mysql.Conn) {
	conn, err := newConn(h.connStr, c.ConnectionID, h.logger)
	if err == nil {
		h.conns[c.ConnectionID] = conn
	}
}

func (h MySqlProxy) getConn(connId uint32) (conn proxyConn, err error) {
	var ok bool
	conn, ok = h.conns[connId]
	if ok {
		return conn, nil
	} else {
		conn, err = newConn(h.connStr, connId, h.logger)
		if err != nil {
			return proxyConn{}, err
		}
	}
	if err = conn.Ping(); err != nil {
		return proxyConn{}, err
	}
	h.conns[connId] = conn
	return conn, nil
}

// ComInitDB implements mysql.Handler.
func (h MySqlProxy) ComInitDB(c *mysql.Conn, schemaName string) error {
	conn, err := h.getConn(c.ConnectionID)
	if err != nil {
		return err
	}
	if schemaName != "" {
		_, err = conn.Exec("USE " + schemaName + " ;")
	}
	return err
}

// ComPrepare implements mysql.Handler.
func (h MySqlProxy) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return nil, fmt.Errorf("ComPrepare unsupported")
}

// ComStmtExecute implements mysql.Handler.
func (h MySqlProxy) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("ComStmtExecute unsupported")
}

// ComResetConnection implements mysql.Handler.
func (h MySqlProxy) ComResetConnection(c *mysql.Conn) {
	return
}

// ConnectionClosed implements mysql.Handler.
func (h MySqlProxy) ConnectionClosed(c *mysql.Conn) {
	conn, ok := h.conns[c.ConnectionID]
	if !ok {
		return
	}
	if err := conn.Close(); err != nil {
		lgr := logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID)
		lgr.Errorf("Error closing connection")
	}
	delete(h.conns, c.ConnectionID)
}

// ComMultiQuery implements mysql.Handler.
func (h MySqlProxy) ComMultiQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	conn, err := h.getConn(c.ConnectionID)
	if err != nil {
		return "", err
	}
	conn.Entry = conn.Entry.WithField("query", query)

	remainder, err := h.processQuery(c, conn, query, true, callback)
	if err != nil {
		conn.Errorf("Failed to process MySQL results: %s", err)
	}
	return remainder, err
}

// ComQuery implements mysql.Handler.
func (h MySqlProxy) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	conn, err := h.getConn(c.ConnectionID)
	if err != nil {
		return err
	}
	conn.Entry = conn.Entry.WithField("query", query)

	_, err = h.processQuery(c, conn, query, false, callback)
	if err != nil {
		conn.Errorf("Failed to process MySQL results: %s", err)
	}
	return err
}

// ComParsedQuery implements mysql.Handler.
func (h MySqlProxy) ComParsedQuery(
	c *mysql.Conn,
	query string,
	parsed sqlparser.Statement,
	callback func(*sqltypes.Result, bool) error,
) error {
	return h.ComQuery(c, query, callback)
}

func (h MySqlProxy) processQuery(
	c *mysql.Conn,
	proxy proxyConn,
	query string,
	isMultiStatement bool,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	ctx := sql.NewContext(h.ctx)
	var remainder string
	if isMultiStatement {
		_, ri, err := sqlparser.ParseOne(query)
		if err != nil {
			return "", err
		}
		if ri != 0 && ri < len(query) {
			remainder = query[ri:]
			query = query[:ri]
			query = strings.TrimSpace(query)
			// trim spaces and empty statements
			query = strings.TrimRightFunc(query, func(r rune) bool {
				return r == ';' || unicode.IsSpace(r)
			})
		}
	}

	ctx = ctx.WithQuery(query)
	more := remainder != ""

	proxy.Debugf("Sending query to MySQL")
	rows, err := proxy.Query(query)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = cerr
		}
	}()

	var processedAtLeastOneBatch bool
	res := &sqltypes.Result{}
	ok := true
	for ok {
		if res, ok, err = fetchMySqlRows(ctx, rows, 128); err != nil {
			return "", err
		}
		if err := callback(res, more); err != nil {
			return "", err
		}
		processedAtLeastOneBatch = true
	}

	if err := setConnStatusFlags(ctx, c); err != nil {
		return remainder, err
	}

	switch len(res.Rows) {
	case 0:
		if len(res.Info) > 0 {
			ctx.GetLogger().Tracef("returning result %s", res.Info)
		} else {
			ctx.GetLogger().Tracef("returning empty result")
		}
	case 1:
		ctx.GetLogger().Tracef("returning result %v", res)
	}

	// processedAtLeastOneBatch means we already called resultsCB() at least
	// once, so no need to call it if RowsAffected == 0.
	if res != nil && (res.RowsAffected == 0 && processedAtLeastOneBatch) {
		return remainder, nil
	}

	return remainder, nil
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery resultsCB if the result does not contain any fields,
// or after the last ComQuery call completes.
func (h MySqlProxy) WarningCount(c *mysql.Conn) uint16 {
	return 0
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
	return types.ConvertToBool(autoCommitSessionVar)
}

func fetchMySqlRows(ctx *sql.Context, results *dsql.Rows, count int) (res *sqltypes.Result, more bool, err error) {
	cols, err := results.ColumnTypes()
	if err != nil {
		return nil, false, err
	}

	types, fields, err := schemaToFields(ctx, cols)
	if err != nil {
		return nil, false, err
	}

	rows := make([][]sqltypes.Value, 0, count)
	for results.Next() {
		if len(rows) == count {
			more = true
			break
		}

		scanRow, err := scanResultRow(results)
		if err != nil {
			return nil, false, err
		}

		row := make([]sqltypes.Value, len(fields))
		for i := range row {
			scanRow[i], _, err = types[i].Convert(scanRow[i])
			if err != nil {
				return nil, false, err
			}
			row[i], err = types[i].SQL(ctx, nil, scanRow[i])
			if err != nil {
				return nil, false, err
			}
		}
		rows = append(rows, row)
	}

	res = &sqltypes.Result{
		Fields:       fields,
		RowsAffected: uint64(len(rows)),
		Rows:         rows,
	}
	return
}

var typeDefaults = map[string]string{
	"char":      "char(255)",
	"binary":    "binary(255)",
	"varchar":   "varchar(65535)",
	"varbinary": "varbinary(65535)",
}

func schemaToFields(ctx *sql.Context, cols []*dsql.ColumnType) ([]sql.Type, []*querypb.Field, error) {
	types := make([]sql.Type, len(cols))
	fields := make([]*querypb.Field, len(cols))

	var err error
	for i, col := range cols {
		typeStr := strings.ToLower(col.DatabaseTypeName())
		if length, ok := col.Length(); ok {
			// append length specifier to type
			typeStr = fmt.Sprintf("%s(%d)", typeStr, length)
		} else if ts, ok := typeDefaults[typeStr]; ok {
			// if no length specifier if given,
			// default to the maximum width
			typeStr = ts
		}
		types[i], err = planbuilder.ParseColumnTypeString(typeStr)
		if err != nil {
			return nil, nil, err
		}

		var charset uint32
		switch types[i].Type() {
		case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
			charset = mysql.CharacterSetBinary
		default:
			charset = mysql.CharacterSetUtf8
		}

		fields[i] = &querypb.Field{
			Name:         col.Name(),
			Type:         types[i].Type(),
			Charset:      charset,
			ColumnLength: math.MaxUint32,
		}
	}
	return types, fields, nil
}

func scanResultRow(results *dsql.Rows) (sql.Row, error) {
	cols, err := results.ColumnTypes()
	if err != nil {
		return nil, err
	}

	scanRow := make(sql.Row, len(cols))
	for i := range cols {
		scanRow[i] = reflect.New(cols[i].ScanType()).Interface()
	}

	for i, columnType := range cols {
		scanRow[i] = reflect.New(columnType.ScanType()).Interface()
	}

	if err = results.Scan(scanRow...); err != nil {
		return nil, err
	}
	for i, val := range scanRow {
		v := reflect.ValueOf(val).Elem().Interface()
		switch t := v.(type) {
		case dsql.RawBytes:
			if t == nil {
				scanRow[i] = nil
			} else {
				scanRow[i] = string(t)
			}
		case dsql.NullBool:
			if t.Valid {
				scanRow[i] = t.Bool
			} else {
				scanRow[i] = nil
			}
		case dsql.NullByte:
			if t.Valid {
				scanRow[i] = t.Byte
			} else {
				scanRow[i] = nil
			}
		case dsql.NullFloat64:
			if t.Valid {
				scanRow[i] = t.Float64
			} else {
				scanRow[i] = nil
			}
		case dsql.NullInt16:
			if t.Valid {
				scanRow[i] = t.Int16
			} else {
				scanRow[i] = nil
			}
		case dsql.NullInt32:
			if t.Valid {
				scanRow[i] = t.Int32
			} else {
				scanRow[i] = nil
			}
		case dsql.NullInt64:
			if t.Valid {
				scanRow[i] = t.Int64
			} else {
				scanRow[i] = nil
			}
		case dsql.NullString:
			if t.Valid {
				scanRow[i] = t.String
			} else {
				scanRow[i] = nil
			}
		case dsql.NullTime:
			if t.Valid {
				scanRow[i] = t.Time
			} else {
				scanRow[i] = nil
			}
		default:
			scanRow[i] = t
		}
	}
	return scanRow, nil
}
