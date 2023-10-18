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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/go-mysql-server/sql"
)

type Validator struct {
	handler mysql.Handler
	golden  MySqlProxy
	logger  *logrus.Logger
}

// NewValidatingHandler creates a new Validator wrapping a MySQL connection.
func NewValidatingHandler(handler mysql.Handler, mySqlConn string, logger *logrus.Logger) (Validator, error) {
	golden, err := NewMySqlProxyHandler(logger, mySqlConn)
	if err != nil {
		return Validator{}, err
	}

	// todo: setup mirroring
	//  - assert that both |handler| and |golden| are
	//    working against empty databases
	//  - possibly sync database set between both

	return Validator{
		handler: handler,
		golden:  golden,
		logger:  logger,
	}, nil
}

var _ mysql.Handler = Validator{}

// NewConnection reports that a new connection has been established.
func (v Validator) NewConnection(c *mysql.Conn) {
	return
}

func (v Validator) ComInitDB(c *mysql.Conn, schemaName string) error {
	if err := v.handler.ComInitDB(c, schemaName); err != nil {
		return err
	}
	return v.golden.ComInitDB(c, schemaName)
}

// ComPrepare parses, partially analyzes, and caches a prepared statement's plan
// with the given [c.ConnectionID].
func (v Validator) ComPrepare(c *mysql.Conn, query string) ([]*query.Field, error) {
	return nil, fmt.Errorf("ComPrepare unsupported")
}

func (v Validator) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("ComStmtExecute unsupported")
}

func (v Validator) ComResetConnection(c *mysql.Conn) {
	return
}

// ConnectionClosed reports that a connection has been closed.
func (v Validator) ConnectionClosed(c *mysql.Conn) {
	v.handler.ConnectionClosed(c)
	v.golden.ConnectionClosed(c)
}

func (v Validator) ComMultiQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	ag := newResultAggregator(callback)
	var remainder string
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() (err error) {
		remainder, err = v.handler.ComMultiQuery(c, query, ag.processResults)
		return
	})
	eg.Go(func() error {
		// ignore errors from MySQL connection
		_, _ = v.golden.ComMultiQuery(c, query, ag.processGoldenResults)
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return "", err
	}
	ag.compareResults(v.getLogger(c).WithField("query", query))

	return remainder, nil
}

// ComQuery executes a SQL query on the SQLe engine.
func (v Validator) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	ag := newResultAggregator(callback)
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return v.handler.ComQuery(c, query, ag.processResults)
	})
	eg.Go(func() error {
		// ignore errors from MySQL connection
		_ = v.golden.ComQuery(c, query, ag.processGoldenResults)
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return err
	}
	ag.compareResults(v.getLogger(c).WithField("query", query))
	return nil
}

// ComQuery executes a SQL query on the SQLe engine.
func (v Validator) ComParsedQuery(
	c *mysql.Conn,
	query string,
	parsed sqlparser.Statement,
	callback func(*sqltypes.Result, bool) error,
) error {
	return v.ComQuery(c, query, callback)
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery resultsCB if the result does not contain any fields,
// or after the last ComQuery call completes.
func (v Validator) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (v Validator) ParserOptionsForConnection(_ *mysql.Conn) (sqlparser.ParserOptions, error) {
	return sqlparser.ParserOptions{}, nil
}

func (v Validator) getLogger(c *mysql.Conn) *logrus.Entry {
	return logrus.NewEntry(v.logger).WithField(
		sql.ConnectionIdLogField, c.ConnectionID)
}

type aggregator struct {
	results  []*sqltypes.Result
	golden   []*sqltypes.Result
	callback func(*sqltypes.Result, bool) error
}

const maxRows = 1024

func newResultAggregator(cb func(*sqltypes.Result, bool) error) *aggregator {
	return &aggregator{callback: cb}
}

func (ag *aggregator) processResults(result *sqltypes.Result, more bool) error {
	if len(ag.results) <= maxRows {
		ag.results = append(ag.results, result)
	}
	return ag.callback(result, more)
}

func (ag *aggregator) processGoldenResults(result *sqltypes.Result, _ bool) error {
	if len(ag.golden) <= maxRows {
		ag.golden = append(ag.golden, result)
	}
	return nil
}

func (ag *aggregator) compareResults(logger *logrus.Entry) {
	actual, err := sortResults(ag.results)
	if err != nil {
		logger.Errorf("Error comparing result sets (%s)", err)
	}
	expected, err := sortResults(ag.golden)
	if err != nil {
		logger.Errorf("Error comparing result sets (%s)", err)
	}
	logger.Debugf("Validting query expected=(%d) actual=(%d)",
		len(actual), len(expected))

	if len(actual) > maxRows || len(expected) > maxRows {
		logger.Warnf("result set too large to validate")
		return
	}

	if len(actual) != len(expected) {
		logger.Warnf("Incorrect result set expected=%s actual=%s)",
			formatRowSet(actual), formatRowSet(expected))
		return
	}
	for i := range actual {
		left, right := actual[i], expected[i]
		cmp, err := compareRows(left, right)
		if err != nil {
			logger.Errorf("Error comparing result sets (%s)", err)
			return
		} else if cmp != 0 {
			logger.Warnf("Incorrect result set expected=%s actual=%s)",
				formatRowSet(actual), formatRowSet(expected))
			return
		}
	}
	return
}

func sortResults(results []*sqltypes.Result) ([][]sqltypes.Value, error) {
	var sz uint64
	for _, r := range results {
		sz += r.RowsAffected
	}
	rows := make([][]sqltypes.Value, 0, sz)
	for _, r := range results {
		rows = append(rows, r.Rows...)
	}

	var cerr error
	sort.Slice(rows, func(i, j int) bool {
		cmp, err := compareRows(rows[i], rows[j])
		if err != nil {
			cerr = err
		}
		return cmp < 0
	})
	if cerr != nil {
		return nil, cerr
	}
	return rows, nil
}

func compareRows(left, right []sqltypes.Value) (cmp int, err error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("rows differ in length (%s != %s)",
			formatRow(left), formatRow(right))
	}
	for i := range left {
		cmp, err = sqltypes.NullsafeCompare(left[i], right[i])
		if err != nil {
			// ignore incompatible types error if types equal
			if left[i].Type() == right[i].Type() {
				cmp = bytes.Compare(left[i].Raw(), right[i].Raw())
				err = nil
			} else {
				return 0, err
			}
		}
		if cmp != 0 {
			break
		}
	}
	return
}

func formatRowSet(rows [][]sqltypes.Value) string {
	var seenOne bool
	var sb strings.Builder
	sb.WriteString("{")
	for _, r := range rows {
		if seenOne {
			sb.WriteRune(',')
		}
		seenOne = true
		sb.WriteString(formatRow(r))
	}
	sb.WriteString("}")
	return sb.String()
}

func formatRow(row []sqltypes.Value) string {
	var seenOne bool
	var sb strings.Builder
	sb.WriteRune('[')
	for _, v := range row {
		if seenOne {
			sb.WriteRune(',')
		}
		seenOne = true
		sb.WriteString(v.String())
	}
	sb.WriteRune(']')
	return sb.String()
}
