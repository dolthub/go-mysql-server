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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

type Validator struct {
	handler   mysql.Handler
	golden    MySqlProxy
	resultsCB ResultCallback
}

type ResultCallback func(c *mysql.Conn, actual, expected [][]sqltypes.Value) error

func ValidateResults(actual, expected [][]sqltypes.Value) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("Incorrect result set expected=%s actual=%s)",
			formatRowSet(actual), formatRowSet(expected))
	}
	for i := range actual {
		left, right := actual[i], expected[i]
		cmp, err := compareRows(left, right)
		if err != nil {
			return fmt.Errorf("Error comparing result sets (%s)", err)
		} else if cmp != 0 {
			return fmt.Errorf("Incorrect result set expected=%s actual=%s)",
				formatRowSet(actual), formatRowSet(expected))
		}
	}
	return nil
}

// NewValidatingHandler creates a new Validator wrapping a MySQL connection.
func NewValidatingHandler(handler mysql.Handler, mySqlConn string, cb ResultCallback) (Validator, error) {
	golden, err := NewMySqlProxyHandler(mySqlConn)
	if err != nil {
		return Validator{}, err
	}

	// todo: setup mirroring
	//  - assert that both |handler| and |golden| are
	//    working against empty databases
	//  - possibly sync database set between both

	return Validator{
		handler:   handler,
		golden:    golden,
		resultsCB: cb,
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
	eg.Go(func() (err error) {
		_, err = v.golden.ComMultiQuery(c, query, ag.processGoldenResults)
		return
	})

	err := eg.Wait()
	if err != nil {
		return "", err
	}
	if err = ag.compareResults(c, v.resultsCB); err != nil {
		return "", err
	}
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
		return v.golden.ComQuery(c, query, ag.processGoldenResults)
	})

	err := eg.Wait()
	if err != nil {
		return err
	}
	return ag.compareResults(c, v.resultsCB)
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery resultsCB if the result does not contain any fields,
// or after the last ComQuery call completes.
func (v Validator) WarningCount(c *mysql.Conn) uint16 {
	return 0
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

func (ag *aggregator) compareResults(c *mysql.Conn, cb ResultCallback) error {
	if len(ag.results) > maxRows || len(ag.golden) > maxRows {
		return fmt.Errorf("result set too large to validate")
	}
	actual, err := sortResults(ag.results)
	if err != nil {
		return err
	}
	golden, err := sortResults(ag.golden)
	if err != nil {
		return err
	}
	return cb(c, actual, golden)
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
