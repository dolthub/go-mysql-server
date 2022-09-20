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

package golden

import (
	dsql "database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
)

type MySqlProxy struct {
	ctx   *sql.Context
	conns map[uint32]*dbr.Connection
	dsn   string
}

// NewMySqlProxyHandler creates a new MySqlProxy.
func NewMySqlProxyHandler(connStr string) (MySqlProxy, error) {
	// eg: "user:pass@tcp(host:port)/database"
	conn, err := dbr.Open("mysql", connStr, nil)
	if err != nil {
		return MySqlProxy{}, err
	}
	defer func() { _ = conn.Close() }()

	if err = conn.Ping(); err != nil {
		return MySqlProxy{}, err
	}

	return MySqlProxy{
		ctx:   sql.NewEmptyContext(),
		conns: make(map[uint32]*dbr.Connection),
		dsn:   connStr,
	}, nil
}

var _ mysql.Handler = MySqlProxy{}

func (h MySqlProxy) getConn(connId uint32) (conn *dbr.Connection, err error) {
	var ok bool
	conn, ok = h.conns[connId]
	if ok {
		return conn, nil
	}
	conn, err = dbr.Open("mysql", h.dsn, nil)
	if err != nil {
		return nil, err
	}
	h.conns[connId] = conn
	return conn, nil
}

// NewConnection reports that a new connection has been established.
func (h MySqlProxy) NewConnection(c *mysql.Conn) {
	return
}

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

// ComPrepare parses, partially analyzes, and caches a prepared statement's plan
// with the given [c.ConnectionID].
func (h MySqlProxy) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return nil, fmt.Errorf("ComPrepare unsupported")
}

func (h MySqlProxy) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("ComStmtExecute unsupported")
}

func (h MySqlProxy) ComResetConnection(c *mysql.Conn) {
	return
}

// ConnectionClosed reports that a connection has been closed.
func (h MySqlProxy) ConnectionClosed(c *mysql.Conn) {
	conn, ok := h.conns[c.ConnectionID]
	if !ok {
		return
	}
	if err := conn.Close(); err != nil {
		lgr := logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID)
		lgr.Errorf("Error closing connection")
	}
}

func (h MySqlProxy) ComMultiQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	return h.processQuery(c, query, true, nil, callback)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h MySqlProxy) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	_, err := h.processQuery(c, query, false, nil, callback)
	return err
}

func (h MySqlProxy) processQuery(
	c *mysql.Conn,
	query string,
	isMultiStatement bool,
	_ map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	ctx := sql.NewContext(h.ctx)
	var remainder string
	if isMultiStatement {
		_, query, remainder, _ = parse.ParseOne(ctx, query)
	}

	ctx = ctx.WithQuery(query)
	more := remainder != ""

	conn, err := h.getConn(c.ConnectionID)
	if err != nil {
		return "", err
	}
	rows, err := conn.Query(query)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = cerr
		}
	}()

	var proccesedAtLeastOneBatch bool
	res := &sqltypes.Result{}
	ok := true
	for ok {
		if res, ok, err = fetchMySqlRows(ctx, rows, 128); err != nil {
			return "", err
		}
		if err := callback(res, more); err != nil {
			return "", err
		}
		proccesedAtLeastOneBatch = true
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
	if res != nil && (res.RowsAffected == 0 && proccesedAtLeastOneBatch) {
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
	return sql.ConvertToBool(autoCommitSessionVar)
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
		scanRow, err := scanResultRow(results)
		if err != nil {
			return nil, false, err
		}

		row := make([]sqltypes.Value, len(fields))
		for i := range row {
			row[i], err = types[i].SQL(ctx, nil, scanRow[i])
			if err != nil {
				return nil, false, err
			}
		}
		rows = append(rows, row)

		if len(rows) == count {
			more = true
			break
		}
	}

	res = &sqltypes.Result{
		Fields:       fields,
		RowsAffected: uint64(len(rows)),
		Rows:         rows,
	}
	return
}

var typeStrTransforms = map[string]string{
	"varchar":   "varchar(65535)",
	"varbinary": "varbinary(65535)",
}

func schemaToFields(ctx *sql.Context, cols []*dsql.ColumnType) ([]sql.Type, []*querypb.Field, error) {
	types := make([]sql.Type, len(cols))
	fields := make([]*querypb.Field, len(cols))

	var err error
	for i, col := range cols {
		typeStr := strings.ToLower(col.DatabaseTypeName())
		if ts, ok := typeStrTransforms[typeStr]; ok {
			typeStr = ts
		}
		types[i], err = parse.ParseColumnTypeString(ctx, typeStr)
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
		scanType := columnType.ScanType()
		switch scanType {
		case reflect.TypeOf(dsql.RawBytes{}):
			scanType = reflect.TypeOf([]byte{})
		case reflect.TypeOf(dsql.NullBool{}):
			scanType = reflect.TypeOf(true)
		case reflect.TypeOf(dsql.NullByte{}):
			scanType = reflect.TypeOf(byte(0))
		case reflect.TypeOf(dsql.NullFloat64{}):
			scanType = reflect.TypeOf(float64(0))
		case reflect.TypeOf(dsql.NullInt16{}):
			scanType = reflect.TypeOf(int16(0))
		case reflect.TypeOf(dsql.NullInt32{}):
			scanType = reflect.TypeOf(int32(0))
		case reflect.TypeOf(dsql.NullInt64{}):
			scanType = reflect.TypeOf(int64(0))
		case reflect.TypeOf(dsql.NullString{}):
			scanType = reflect.TypeOf("")
		case reflect.TypeOf(dsql.NullTime{}):
			scanType = reflect.TypeOf(time.Time{})
		}
		scanRow[i] = reflect.New(scanType).Interface()
	}

	if err = results.Scan(scanRow...); err != nil {
		return nil, err
	}
	for i, val := range scanRow {
		reflectVal := reflect.ValueOf(val)
		if reflectVal.IsNil() {
			scanRow[i] = nil
		} else {
			scanRow[i] = reflectVal.Elem().Interface()
			if byteSlice, ok := val.([]byte); ok {
				scanRow[i] = string(byteSlice)
			}
		}
	}
	return scanRow, nil
}
