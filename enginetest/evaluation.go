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

package enginetest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// RunQuery runs the query given and asserts that it doesn't result in an error.
func RunQuery(t *testing.T, e *sqle.Engine, harness Harness, query string) {
	ctx := NewContext(harness)
	RunQueryWithContext(t, e, harness, ctx, query)
}

// RunQueryWithContext runs the query given and asserts that it doesn't result in an error.
func RunQueryWithContext(t *testing.T, e *sqle.Engine, harness Harness, ctx *sql.Context, query string) {
	ctx = ctx.WithQuery(query)
	sch, iter, err := e.Query(ctx, query)
	require.NoError(t, err, "error running query %s: %v", query, err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(t, err)
	validateEngine(t, ctx, harness, e)
}

// TestScript runs the test script given, making any assertions given
func TestScript(t *testing.T, harness Harness, script queries.ScriptTest) {
	e := mustNewEngine(t, harness)
	defer e.Close()
	TestScriptWithEngine(t, e, harness, script)
}

// TestScriptWithEngine runs the test script given with the engine provided.
func TestScriptWithEngine(t *testing.T, e *sqle.Engine, harness Harness, script queries.ScriptTest) {
	t.Run(script.Name, func(t *testing.T) {
		for _, statement := range script.SetUpScript {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(statement) {
					t.Skip()
				}
			}
			ctx := NewContext(harness)
			RunQueryWithContext(t, e, harness, ctx, statement)
			validateEngine(t, ctx, harness, e)
		}

		assertions := script.Assertions
		if len(assertions) == 0 {
			assertions = []queries.ScriptTestAssertion{
				{
					Query:       script.Query,
					Expected:    script.Expected,
					ExpectedErr: script.ExpectedErr,
				},
			}
		}

		for _, assertion := range assertions {
			if assertion.ExpectedErr != nil {
				t.Run(assertion.Query, func(t *testing.T) {
					AssertErr(t, e, harness, assertion.Query, assertion.ExpectedErr)
				})
			} else if assertion.ExpectedErrStr != "" {
				t.Run(assertion.Query, func(t *testing.T) {
					AssertErr(t, e, harness, assertion.Query, nil, assertion.ExpectedErrStr)
				})
			} else if assertion.ExpectedWarning != 0 {
				t.Run(assertion.Query, func(t *testing.T) {
					AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
						assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
						assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
				})
			} else if assertion.SkipResultsCheck {
				RunQuery(t, e, harness, assertion.Query)
			} else {
				t.Run(assertion.Query, func(t *testing.T) {
					ctx := NewContext(harness)
					TestQueryWithContext(t, ctx, e, assertion.Query, assertion.Expected, nil, assertion.Bindings)
				})
			}
		}
	})
}

// TestScriptPrepared substitutes literals for bindvars, runs the test script given,
// and makes any assertions given
func TestScriptPrepared(t *testing.T, harness Harness, script queries.ScriptTest) bool {
	return t.Run(script.Name, func(t *testing.T) {
		if script.SkipPrepared {
			t.Skip()
		}

		e := mustNewEngine(t, harness)
		defer e.Close()
		TestScriptWithEnginePrepared(t, e, harness, script)
	})
}

// TestScriptWithEnginePrepared runs the test script with bindvars substituted for literals
// using the engine provided.
func TestScriptWithEnginePrepared(t *testing.T, e *sqle.Engine, harness Harness, script queries.ScriptTest) {
	ctx := NewContextWithEngine(harness, e)
	for _, statement := range script.SetUpScript {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(statement) {
				t.Skip()
			}
		}
		_, _, err := runQueryPreparedWithCtx(t, ctx, e, statement)
		require.NoError(t, err)
		validateEngine(t, ctx, harness, e)
	}

	assertions := script.Assertions
	if len(assertions) == 0 {
		assertions = []queries.ScriptTestAssertion{
			{
				Query:       script.Query,
				Expected:    script.Expected,
				ExpectedErr: script.ExpectedErr,
			},
		}
	}

	for _, assertion := range assertions {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(assertion.Query) {
				t.Skip()
			}
		}
		if assertion.ExpectedErr != nil {
			t.Run(assertion.Query, func(t *testing.T) {
				AssertErr(t, e, harness, assertion.Query, assertion.ExpectedErr)
			})
		} else if assertion.ExpectedErrStr != "" {
			t.Run(assertion.Query, func(t *testing.T) {
				AssertErr(t, e, harness, assertion.Query, nil, assertion.ExpectedErrStr)
			})
		} else if assertion.ExpectedWarning != 0 {
			t.Run(assertion.Query, func(t *testing.T) {
				AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
					assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
					assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
			})
		} else {
			TestPreparedQueryWithContext(t, ctx, e, harness, assertion.Query, assertion.Expected, nil)
		}
	}
}

// TestTransactionScript runs the test script given, making any assertions given
func TestTransactionScript(t *testing.T, harness Harness, script queries.TransactionTest) bool {
	// todo(max): these use dolt_commit, need harness reset to reset back to original commit
	return t.Run(script.Name, func(t *testing.T) {
		harness.Setup(setup.MydbData)
		e := mustNewEngine(t, harness)
		defer e.Close()
		TestTransactionScriptWithEngine(t, e, harness, script)
	})
}

// TestTransactionScriptWithEngine runs the transaction test script given with the engine provided.
func TestTransactionScriptWithEngine(t *testing.T, e *sqle.Engine, harness Harness, script queries.TransactionTest) {
	setupSession := NewSession(harness)
	for _, statement := range script.SetUpScript {
		RunQueryWithContext(t, e, harness, setupSession, statement)
	}

	clientSessions := make(map[string]*sql.Context)
	assertions := script.Assertions

	for _, assertion := range assertions {
		client := getClient(assertion.Query)

		clientSession, ok := clientSessions[client]
		if !ok {
			clientSession = NewSession(harness)
			clientSessions[client] = clientSession
		}

		t.Run(assertion.Query, func(t *testing.T) {
			if assertion.ExpectedErr != nil {
				AssertErrWithCtx(t, e, harness, clientSession, assertion.Query, assertion.ExpectedErr)
			} else if assertion.ExpectedErrStr != "" {
				AssertErrWithCtx(t, e, harness, clientSession, assertion.Query, nil, assertion.ExpectedErrStr)
			} else if assertion.ExpectedWarning != 0 {
				AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query, assertion.Expected,
					nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
					assertion.ExpectedWarningMessageSubstring, false)
			} else if assertion.SkipResultsCheck {
				RunQueryWithContext(t, e, harness, clientSession, assertion.Query)
			} else {
				TestQueryWithContext(t, clientSession, e, assertion.Query, assertion.Expected, nil, nil)
			}
		})
	}
}

// TestQuery runs a query on the engine given and asserts that results are as expected.
func TestQuery(t *testing.T, harness Harness, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]sql.Expression) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, q, expected, expectedCols, bindings)
	})
}

func TestQueryWithEngine(t *testing.T, harness Harness, e *sqle.Engine, tt queries.QueryTest) {
	t.Run(tt.Query, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query %s", tt.Query)
			}
		}

		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
	})
}

func TestQueryWithContext(t *testing.T, ctx *sql.Context, e *sqle.Engine, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]sql.Expression) {
	ctx = ctx.WithQuery(q)
	require := require.New(t)
	if len(bindings) > 0 {
		_, err := e.PrepareQuery(ctx, q)
		require.NoError(err)
	}
	sch, iter, err := e.QueryWithBindings(ctx, q, bindings)
	require.NoError(err, "Unexpected error for query %s", q)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err, "Unexpected error for query %s", q)

	checkResults(t, require, expected, expectedCols, sch, rows, q)

	require.Equal(0, ctx.Memory.NumCaches())
}

// TestPreparedQuery runs a prepared query on the engine given and asserts that results are as expected.
func TestPreparedQuery(t *testing.T, harness Harness, q string, expected []sql.Row, expectedCols []*sql.Column) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestPreparedQueryWithContext(t, ctx, e, harness, q, expected, expectedCols)
	})
}

func TestPreparedQueryWithEngine(t *testing.T, harness Harness, e *sqle.Engine, tt queries.QueryTest) {
	t.Run(tt.Query, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query %s", tt.Query)
			}
		}
		ctx := NewContext(harness)
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns)
	})
}

func TestPreparedQueryWithContext(
	t *testing.T,
	ctx *sql.Context,
	e *sqle.Engine,
	h Harness,
	q string,
	expected []sql.Row,
	expectedCols []*sql.Column,
) {
	require := require.New(t)
	rows, sch, err := runQueryPreparedWithCtx(t, ctx, e, q)
	require.NoError(err, "Unexpected error for query %s", q)

	checkResults(t, require, expected, expectedCols, sch, rows, q)

	require.Equal(0, ctx.Memory.NumCaches())
	validateEngine(t, ctx, h, e)
}

func runQueryPreparedWithCtx(
	t *testing.T,
	ctx *sql.Context,
	e *sqle.Engine,
	q string,
) ([]sql.Row, sql.Schema, error) {
	require := require.New(t)
	parsed, err := parse.Parse(ctx, q)
	if err != nil {
		return nil, nil, err
	}

	_, isInsert := parsed.(*plan.InsertInto)
	_, isDatabaser := parsed.(sql.Databaser)

	// *ast.MultiAlterDDL parses arbitrary nodes in a *plan.Block
	if bl, ok := parsed.(*plan.Block); ok {
		for _, n := range bl.Children() {
			if _, ok := n.(*plan.InsertInto); ok {
				isInsert = true
			} else if _, ok := n.(sql.Databaser); ok {
				isDatabaser = true
			}

		}
	}
	if isDatabaser && !isInsert {
		// DDL statements don't support prepared statements
		sch, iter, err := e.QueryNodeWithBindings(ctx, q, nil, nil)
		require.NoError(err, "Unexpected error for query %s", q)

		rows, err := sql.RowIterToRows(ctx, sch, iter)
		return rows, sch, err
	}

	bindVars := make(map[string]sql.Expression)
	var bindCnt int
	var foundBindVar bool
	insertBindings := func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := expr.(type) {
		case *expression.Literal:
			varName := fmt.Sprintf("v%d", bindCnt)
			bindVars[varName] = e
			bindCnt++
			return expression.NewBindVar(varName), transform.NewTree, nil
		case *expression.BindVar:
			if _, ok := bindVars[e.Name]; ok {
				return expr, transform.SameTree, nil
			}
			foundBindVar = true
			return expr, transform.NewTree, nil
		default:
			return expr, transform.SameTree, nil
		}
	}
	bound, _, err := transform.NodeWithOpaque(parsed, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			newSource, _, err := transform.NodeExprs(n.Source, insertBindings)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n.WithSource(newSource), transform.SameTree, nil
		default:
			return transform.NodeExprs(n, insertBindings)
		}
		return node, transform.SameTree, nil
	})

	if foundBindVar {
		t.Skip()
	}

	prepared, err := e.Analyzer.PrepareQuery(ctx, bound, nil)
	if err != nil {
		return nil, nil, err
	}
	e.CachePreparedStmt(ctx, prepared, q)

	sch, iter, err := e.QueryNodeWithBindings(ctx, q, nil, bindVars)
	require.NoError(err, "Unexpected error for query %s", q)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	return rows, sch, err
}

func checkResults(
	t *testing.T,
	require *require.Assertions,
	expected []sql.Row,
	expectedCols []*sql.Column,
	sch sql.Schema,
	rows []sql.Row,
	q string,
) {
	widenedRows := WidenRows(sch, rows)
	widenedExpected := WidenRows(sch, expected)

	upperQuery := strings.ToUpper(q)
	orderBy := strings.Contains(upperQuery, "ORDER BY ")

	// We replace all times for SHOW statements with the Unix epoch
	if strings.HasPrefix(upperQuery, "SHOW ") {
		for _, widenedRow := range widenedRows {
			for i, val := range widenedRow {
				if _, ok := val.(time.Time); ok {
					widenedRow[i] = time.Unix(0, 0).UTC()
				}
			}
		}
	}

	// .Equal gives better error messages than .ElementsMatch, so use it when possible
	if orderBy || len(expected) <= 1 {
		require.Equal(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	} else {
		require.ElementsMatch(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	}

	// If the expected schema was given, test it as well
	if expectedCols != nil {
		assert.Equal(t, expectedCols, stripSchema(sch))
	}
}

func stripSchema(s sql.Schema) []*sql.Column {
	fields := make([]*sql.Column, len(s))
	for i, c := range s {
		fields[i] = &sql.Column{
			Name: c.Name,
			Type: c.Type,
		}
	}
	return fields
}

// For a variety of reasons, the widths of various primitive types can vary when passed through different SQL queries
// (and different database implementations). We may eventually decide that this undefined behavior is a problem, but
// for now it's mostly just an issue when comparing results in tests. To get around this, we widen every type to its
// widest value in actual and expected results.
func WidenRows(sch sql.Schema, rows []sql.Row) []sql.Row {
	widened := make([]sql.Row, len(rows))
	for i, row := range rows {
		widened[i] = WidenRow(sch, row)
	}
	return widened
}

// See WidenRows
func WidenRow(sch sql.Schema, row sql.Row) sql.Row {
	widened := make(sql.Row, len(row))
	for i, v := range row {

		var vw interface{}
		if i < len(sch) && sql.IsJSON(sch[i].Type) {
			widened[i] = widenJSONValues(v)
			continue
		}

		switch x := v.(type) {
		case int:
			vw = int64(x)
		case int8:
			vw = int64(x)
		case int16:
			vw = int64(x)
		case int32:
			vw = int64(x)
		case uint:
			vw = uint64(x)
		case uint8:
			vw = uint64(x)
		case uint16:
			vw = uint64(x)
		case uint32:
			vw = uint64(x)
		case float32:
			vw = float64(x)
		default:
			vw = v
		}
		widened[i] = vw
	}
	return widened
}

func widenJSONValues(val interface{}) sql.JSONValue {
	if val == nil {
		return nil
	}

	js, ok := val.(sql.JSONValue)
	if !ok {
		panic(fmt.Sprintf("%v is not json", val))
	}

	doc, err := js.Unmarshall(sql.NewEmptyContext())
	if err != nil {
		panic(err)
	}

	doc.Val = widenJSON(doc.Val)
	return doc
}

func widenJSON(val interface{}) interface{} {
	switch x := val.(type) {
	case int:
		return float64(x)
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case []interface{}:
		return widenJSONArray(x)
	case map[string]interface{}:
		return widenJSONObject(x)
	default:
		return x
	}
}

func widenJSONObject(narrow map[string]interface{}) (wide map[string]interface{}) {
	wide = make(map[string]interface{}, len(narrow))
	for k, v := range narrow {
		wide[k] = widenJSON(v)
	}
	return
}

func widenJSONArray(narrow []interface{}) (wide []interface{}) {
	wide = make([]interface{}, len(narrow))
	for i, v := range narrow {
		wide[i] = widenJSON(v)
	}
	return
}

// AssertErr asserts that the given query returns an error during its execution, optionally specifying a type of error.
func AssertErr(t *testing.T, e *sqle.Engine, harness Harness, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	AssertErrWithCtx(t, e, harness, NewContext(harness), query, expectedErrKind, errStrs...)
}

// AssertErrWithBindings asserts that the given query returns an error during its execution, optionally specifying a
// type of error.
func AssertErrWithBindings(t *testing.T, e *sqle.Engine, harness Harness, query string, bindings map[string]sql.Expression, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx := NewContext(harness)
	sch, iter, err := e.QueryWithBindings(ctx, query, bindings)
	if err == nil {
		_, err = sql.RowIterToRows(ctx, sch, iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		require.True(t, expectedErrKind.Is(err), "Expected error of type %s but got %s", expectedErrKind, err)
	} else if len(errStrs) >= 1 {
		require.Equal(t, errStrs[0], err.Error())
	}
	validateEngine(t, ctx, harness, e)
}

// AssertErrWithCtx is the same as AssertErr, but uses the context given instead of creating one from a harness
func AssertErrWithCtx(t *testing.T, e *sqle.Engine, harness Harness, ctx *sql.Context, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx = ctx.WithQuery(query)
	sch, iter, err := e.Query(ctx, query)
	if err == nil {
		_, err = sql.RowIterToRows(ctx, sch, iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		_, orig, _ := sql.CastSQLError(err)
		require.True(t, expectedErrKind.Is(orig), "Expected error of type %s but got %s", expectedErrKind, err)
	}
	// If there are multiple error strings then we only match against the first
	if len(errStrs) >= 1 {
		require.Equal(t, errStrs[0], err.Error())
	}
	validateEngine(t, ctx, harness, e)
}

// AssertWarningAndTestQuery tests the query and asserts an expected warning code. If |ctx| is provided, it will be
// used. Otherwise the harness will be used to create a fresh context.
func AssertWarningAndTestQuery(
	t *testing.T,
	e *sqle.Engine,
	ctx *sql.Context,
	harness Harness,
	query string,
	expected []sql.Row,
	expectedCols []*sql.Column,
	expectedCode int,
	expectedWarningsCount int,
	expectedWarningMessageSubstring string,
	skipResultsCheck bool,
) {
	require := require.New(t)
	if ctx == nil {
		ctx = NewContext(harness)
	}
	ctx.ClearWarnings()
	ctx = ctx.WithQuery(query)

	sch, iter, err := e.Query(ctx, query)
	require.NoError(err, "Unexpected error for query %s", query)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err, "Unexpected error for query %s", query)

	if expectedWarningsCount > 0 {
		assert.Equal(t, expectedWarningsCount, len(ctx.Warnings()))
	}

	if expectedCode > 0 {
		for _, warning := range ctx.Warnings() {
			assert.Equal(t, expectedCode, warning.Code, "Unexpected warning code")
		}
	}

	if len(expectedWarningMessageSubstring) > 0 {
		for _, warning := range ctx.Warnings() {
			assert.Contains(t, warning.Message, expectedWarningMessageSubstring, "Unexpected warning message")
		}
	}

	if !skipResultsCheck {
		checkResults(t, require, expected, expectedCols, sch, rows, query)
	}
	validateEngine(t, ctx, harness, e)
}

func assertSchemasEqualWithDefaults(t *testing.T, expected, actual sql.Schema) bool {
	if len(expected) != len(actual) {
		return assert.Equal(t, expected, actual)
	}

	ec, ac := make(sql.Schema, len(expected)), make(sql.Schema, len(actual))
	for i := range expected {
		ecc := *expected[i]
		acc := *actual[i]

		ecc.Default = nil
		acc.Default = nil

		ac[i] = &acc
		ec[i] = &ecc

		// For the default, compare just the string representations. This makes it possible for integrators who don't reify
		// default value expressions at schema load time (best practice) to run these tests. We also trim off any parens
		// for the same reason.
		eds, ads := "NULL", "NULL"
		if expected[i].Default != nil {
			eds = strings.Trim(expected[i].Default.String(), "()")
		}
		if actual[i].Default != nil {
			ads = strings.Trim(actual[i].Default.String(), "()")
		}

		assert.Equal(t, eds, ads, "column default values differ")
	}

	return assert.Equal(t, ec, ac)
}

func ExtractQueryNode(node sql.Node) sql.Node {
	switch node := node.(type) {
	case *plan.QueryProcess:
		return ExtractQueryNode(node.Child())
	case *analyzer.Releaser:
		return ExtractQueryNode(node.Child)
	default:
		return node
	}
}

func runWriteQueryTest(t *testing.T, harness Harness, tt queries.WriteQueryTest) {
	t.Run(tt.WriteQuery, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.WriteQuery) {
				t.Logf("Skipping query %s", tt.WriteQuery)
				return
			}
			if sh.SkipQueryTest(tt.SelectQuery) {
				t.Logf("Skipping query %s", tt.SelectQuery)
				return
			}
		}
		e := mustNewEngine(t, harness)
		ctx := NewContext(harness)
		defer e.Close()
		TestQueryWithContext(t, ctx, e, tt.WriteQuery, tt.ExpectedWriteResult, nil, nil)
		TestQueryWithContext(t, ctx, e, tt.SelectQuery, tt.ExpectedSelect, nil, nil)
	})
}

func runWriteQueryTestPrepared(t *testing.T, harness Harness, tt queries.WriteQueryTest) {
	t.Run(tt.WriteQuery, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.WriteQuery) {
				t.Logf("Skipping query %s", tt.WriteQuery)
				return
			}
			if sh.SkipQueryTest(tt.SelectQuery) {
				t.Logf("Skipping query %s", tt.SelectQuery)
				return
			}
		}
		e := mustNewEngine(t, harness)
		ctx := NewContext(harness)
		defer e.Close()
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.WriteQuery, tt.ExpectedWriteResult, nil)
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.SelectQuery, tt.ExpectedSelect, nil)
	})
}

func runGenericErrorTest(t *testing.T, h Harness, tt queries.GenericErrorQueryTest) {
	t.Run(tt.Name, func(t *testing.T) {
		if sh, ok := h.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("skipping query %s", tt.Query)
			}
		}
		e := mustNewEngine(t, h)
		defer e.Close()
		AssertErr(t, e, h, tt.Query, nil)
	})
}

func runQueryErrorTest(t *testing.T, h Harness, tt queries.QueryErrorTest) {
	t.Run(tt.Query, func(t *testing.T) {
		if sh, ok := h.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("skipping query %s", tt.Query)
			}
		}
		e := mustNewEngine(t, h)
		defer e.Close()
		AssertErr(t, e, h, tt.Query, nil)
	})
}

func validateEngine(t *testing.T, ctx *sql.Context, harness Harness, e *sqle.Engine) {
	if harness == nil {
		assert.NotNil(t, harness)
	}
	require.NotNil(t, harness)
	if vh, ok := harness.(ValidatingHarness); ok {
		assert.NoError(t, vh.ValidateEngine(ctx, e))
	}
}
