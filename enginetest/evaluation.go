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

	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RunQuery runs the query given and asserts that it doesn't result in an error.
func RunQuery(t *testing.T, e QueryEngine, harness Harness, query string) {
	ctx := NewContext(harness)
	RunQueryWithContext(t, e, harness, ctx, query)
}

// RunQueryWithContext runs the query given and asserts that it doesn't result in an error.
func RunQueryWithContext(t *testing.T, e QueryEngine, harness Harness, ctx *sql.Context, query string) {
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
func TestScriptWithEngine(t *testing.T, e QueryEngine, harness Harness, script queries.ScriptTest) {
	t.Run(script.Name, func(t *testing.T) {
		for _, statement := range script.SetUpScript {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(statement) {
					t.Skip()
				}
			}
			ctx := NewContext(harness).WithQuery(statement)
			RunQueryWithContext(t, e, harness, ctx, statement)
		}

		assertions := script.Assertions
		if len(assertions) == 0 {
			assertions = []queries.ScriptTestAssertion{
				{
					Query:           script.Query,
					Expected:        script.Expected,
					ExpectedErr:     script.ExpectedErr,
					ExpectedIndexes: script.ExpectedIndexes,
				},
			}
		}

		for _, assertion := range assertions {
			t.Run(assertion.Query, func(t *testing.T) {
				if sh, ok := harness.(SkippingHarness); ok && sh.SkipQueryTest(assertion.Query) {
					t.Skip()
				}
				if assertion.Skip {
					t.Skip()
				}

				if assertion.ExpectedErr != nil {
					AssertErr(t, e, harness, assertion.Query, assertion.ExpectedErr)
				} else if assertion.ExpectedErrStr != "" {
					AssertErr(t, e, harness, assertion.Query, nil, assertion.ExpectedErrStr)
				} else if assertion.ExpectedWarning != 0 {
					AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
						assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
						assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
				} else if assertion.SkipResultsCheck {
					RunQuery(t, e, harness, assertion.Query)
				} else {
					ctx := NewContext(harness)
					TestQueryWithContext(t, ctx, e, harness, assertion.Query, assertion.Expected, assertion.ExpectedColumns, assertion.Bindings)
				}
				if assertion.ExpectedIndexes != nil {
					evalIndexTest(t, harness, e, assertion.Query, assertion.ExpectedIndexes, assertion.Skip)
				}
				if assertion.JoinTypes != nil {
					evalJoinTypeTest(t, harness, e, assertion.Query, assertion.JoinTypes, assertion.Skip)
				}
			})
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
func TestScriptWithEnginePrepared(t *testing.T, e QueryEngine, harness Harness, script queries.ScriptTest) {
	t.Run(script.Name, func(t *testing.T) {
		for _, statement := range script.SetUpScript {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(statement) {
					t.Skip()
				}
			}
			ctx := NewContext(harness).WithQuery(statement)
			RunQueryWithContext(t, e, harness, ctx, statement)
			validateEngine(t, ctx, harness, e)
		}

		assertions := script.Assertions
		if len(assertions) == 0 {
			assertions = []queries.ScriptTestAssertion{
				{
					Query:           script.Query,
					Expected:        script.Expected,
					ExpectedErr:     script.ExpectedErr,
					ExpectedIndexes: script.ExpectedIndexes,
				},
			}
		}

		for _, assertion := range assertions {
			t.Run(assertion.Query, func(t *testing.T) {

				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(assertion.Query) {
						t.Skip()
					}
				}
				if assertion.Skip {
					t.Skip()
				}

				if assertion.ExpectedErr != nil {
					AssertErrPrepared(t, e, harness, assertion.Query, assertion.ExpectedErr)
				} else if assertion.ExpectedErrStr != "" {
					AssertErrPrepared(t, e, harness, assertion.Query, nil, assertion.ExpectedErrStr)
				} else if assertion.ExpectedWarning != 0 {
					AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
						assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
						assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
				} else if assertion.SkipResultsCheck {
					ctx := NewContext(harness).WithQuery(assertion.Query)
					_, _, err := runQueryPreparedWithCtx(t, ctx, e, assertion.Query, assertion.Bindings)
					require.NoError(t, err)
				} else {
					ctx := NewContext(harness).WithQuery(assertion.Query)
					TestPreparedQueryWithContext(t, ctx, e, harness, assertion.Query, assertion.Expected, nil, assertion.Bindings)
				}
				if assertion.ExpectedIndexes != nil {
					evalIndexTest(t, harness, e, assertion.Query, assertion.ExpectedIndexes, assertion.Skip)
				}
			})
		}
	})
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
func TestTransactionScriptWithEngine(t *testing.T, e QueryEngine, harness Harness, script queries.TransactionTest) {
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
				TestQueryWithContext(t, clientSession, e, harness, assertion.Query, assertion.Expected, nil, nil)
			}
		})
	}
}

// TestQuery runs a query on the engine given and asserts that results are as expected.
// TODO: this should take en engine
func TestQuery(t *testing.T, harness Harness, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]*querypb.BindVariable) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, q, expected, expectedCols, bindings)
	})
}

// TestQuery runs a query on the engine given and asserts that results are as expected.
func TestQuery2(t *testing.T, harness Harness, e QueryEngine, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]*querypb.BindVariable) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, q, expected, expectedCols, bindings)
	})
}

// TODO: collapse into TestQuery
func TestQueryWithEngine(t *testing.T, harness Harness, e QueryEngine, tt queries.QueryTest) {
	t.Run(tt.Query, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query %s", tt.Query)
			}
		}

		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
	})
}

func TestQueryWithContext(t *testing.T, ctx *sql.Context, e QueryEngine, harness Harness, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]*querypb.BindVariable) {
	ctx = ctx.WithQuery(q)
	require := require.New(t)
	if len(bindings) > 0 {
		_, err := e.PrepareQuery(ctx, q)
		require.NoError(err)
	}

	sch, iter, err := e.QueryWithBindings(ctx, q, nil, bindings)
	require.NoError(err, "Unexpected error for query %s: %s", q, err)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err, "Unexpected error for query %s: %s", q, err)

	if expected != nil {
		checkResults(t, expected, expectedCols, sch, rows, q)
	}

	require.Equal(
		0, ctx.Memory.NumCaches())
	validateEngine(t, ctx, harness, e)
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
		TestPreparedQueryWithContext(t, ctx, e, harness, q, expected, expectedCols, nil)
	})
}

func TestPreparedQueryWithEngine(t *testing.T, harness Harness, e QueryEngine, tt queries.QueryTest) {
	t.Run(tt.Query, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query %s", tt.Query)
			}
		}
		ctx := NewContext(harness)
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	})
}

func TestPreparedQueryWithContext(
	t *testing.T,
	ctx *sql.Context,
	e QueryEngine,
	h Harness,
	q string,
	expected []sql.Row,
	expectedCols []*sql.Column,
	bindVars map[string]*querypb.BindVariable,
) {
	require := require.New(t)
	rows, sch, err := runQueryPreparedWithCtx(t, ctx, e, q, bindVars)
	if err != nil {
		print(q)
	}
	require.NoError(err, "Unexpected error for query %s", q)

	if expected != nil {
		// TODO fix expected cols for prepared?
		checkResults(t, expected, nil, sch, rows, q)
	}

	require.Equal(0, ctx.Memory.NumCaches())
	validateEngine(t, ctx, h, e)
}

func injectBindVarsAndPrepare(
	t *testing.T,
	ctx *sql.Context,
	e QueryEngine,
	q string,
) (string, map[string]*querypb.BindVariable, error) {
	sqlMode := sql.LoadSqlMode(ctx)
	parsed, err := sqlparser.ParseWithOptions(q, sqlMode.ParserOptions())
	if err != nil {
		return q, nil, sql.ErrSyntaxError.New(err)
	}

	resPlan, err := planbuilder.ParseWithOptions(ctx, e.EngineAnalyzer().Catalog, q, sqlMode.ParserOptions())
	if err != nil {
		return q, nil, err
	}

	b := planbuilder.New(ctx, sql.MapCatalog{})
	_, isInsert := resPlan.(*plan.InsertInto)
	bindVars := make(map[string]*querypb.BindVariable)
	var bindCnt int
	var foundBindVar bool
	var skipTypeConv bool
	err = sqlparser.Walk(func(n sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := n.(type) {
		case *sqlparser.SQLVal:
			switch n.Type {
			case sqlparser.HexNum, sqlparser.HexVal:
				return false, nil
			}
			e := b.ConvertVal(n)
			val, _, err := e.Type().Promote().Convert(e.(*expression.Literal).Value())
			if err != nil {
				skipTypeConv = true
				return false, nil
			}
			bindVar, err := sqltypes.BuildBindVariable(val)
			if err != nil {
				skipTypeConv = true
				return false, nil
			}
			varName := fmt.Sprintf("v%d", bindCnt+1)
			bindVars[varName] = bindVar
			n.Type = sqlparser.ValArg
			n.Val = []byte(fmt.Sprintf(":v%d", bindCnt+1))
			bindCnt++
		case *sqlparser.Insert:
			isInsert = true
		default:
		}
		return true, nil
	}, parsed)
	if err != nil {
		return "", nil, err
	}
	if skipTypeConv {
		return q, nil, nil
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	parsed.Format(buf)
	e.EnginePreparedDataCache().CacheStmt(ctx.Session.ID(), buf.String(), parsed)

	_, isDatabaser := resPlan.(sql.Databaser)

	// *ast.MultiAlterDDL parses arbitrary nodes in a *plan.Block
	if bl, ok := resPlan.(*plan.Block); ok {
		for _, n := range bl.Children() {
			if _, ok := n.(*plan.InsertInto); ok {
				isInsert = true
			} else if _, ok := n.(sql.Databaser); ok {
				isDatabaser = true
			}

		}
	}
	if isDatabaser && !isInsert {
		return q, nil, nil
	}

	if foundBindVar {
		t.Skip()
	}

	return buf.String(), bindVars, nil
}

func runQueryPreparedWithCtx(
	t *testing.T,
	ctx *sql.Context,
	e QueryEngine,
	q string,
	bindVars map[string]*querypb.BindVariable,
) ([]sql.Row, sql.Schema, error) {
	// If bindvars were not provided, try to inject some
	if bindVars == nil || len(bindVars) == 0 {
		var err error
		q, bindVars, err = injectBindVarsAndPrepare(t, ctx, e, q)
		if err != nil {
			return nil, nil, err
		}
	}

	sch, iter, err := e.QueryWithBindings(ctx, q, nil, bindVars)
	if err != nil {
		return nil, nil, err
	}

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	return rows, sch, err
}

// CustomValueValidator is an interface for custom validation of values in the result set
type CustomValueValidator interface {
	Validate(interface{}) (bool, error)
}

func checkResults(
	t *testing.T,
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

	// The result from SELECT or WITH queries can be decimal.Decimal type.
	// The exact expected value cannot be defined in enginetests, so convert the result to string format,
	// which is the value we get on sql shell.
	if strings.HasPrefix(upperQuery, "SELECT ") || strings.HasPrefix(upperQuery, "WITH ") || strings.HasPrefix(upperQuery, "CALL ") {
		for _, widenedRow := range widenedRows {
			for i, val := range widenedRow {
				if d, ok := val.(decimal.Decimal); ok {
					widenedRow[i] = d.StringFixed(d.Exponent() * -1)
				}
			}
		}
	}

	// Special case for custom values
	for i, row := range widenedExpected {
		for j, field := range row {
			if cvv, isCustom := field.(CustomValueValidator); isCustom {
				if i >= len(widenedRows) {
					continue
				}
				actual := widenedRows[i][j] // shouldn't panic, but fine if it does
				ok, err := cvv.Validate(actual)
				if err != nil {
					t.Error(err.Error())
				}
				if !ok {
					t.Errorf("Custom value validation, got %v", actual)
				}
				widenedExpected[i][j] = actual // ensure it passes equality check later
			}
		}
	}

	// .Equal gives better error messages than .ElementsMatch, so use it when possible
	if orderBy || len(expected) <= 1 {
		require.Equal(t, widenedExpected, widenedRows, "Unexpected result for query %s", q)
	} else {
		require.ElementsMatch(t, widenedExpected, widenedRows, "Unexpected result for query %s", q)
	}

	// If the expected schema was given, test it as well
	if expectedCols != nil {
		assert.Equal(t, simplifyResultSchema(expectedCols), simplifyResultSchema(sch))
	}
}

type resultSchemaCol struct {
	Name string
	Type querypb.Type
}

func simplifyResultSchema(s sql.Schema) []resultSchemaCol {
	fields := make([]resultSchemaCol, len(s))
	for i, c := range s {
		fields[i] = resultSchemaCol{
			Name: c.Name,
			Type: c.Type.Type(),
		}
	}
	return fields
}

// WidenRows returns a slice of rows with all values widened to their widest type.
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

// WidenRow returns a row with all values widened to their widest type
func WidenRow(sch sql.Schema, row sql.Row) sql.Row {
	widened := make(sql.Row, len(row))
	for i, v := range row {

		var vw interface{}
		if i < len(sch) && types.IsJSON(sch[i].Type) {
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

func widenJSONValues(val interface{}) sql.JSONWrapper {
	if val == nil {
		return nil
	}

	js, ok := val.(sql.JSONWrapper)
	if !ok {
		str, ok := val.(string)
		if !ok {
			panic(fmt.Sprintf("%v is not json", val))
		}
		js = types.MustJSON(str)
	}

	doc := js.ToInterface()

	if _, ok := js.(sql.Statistic); ok {
		// avoid comparing time values in statistics
		delete(doc.(map[string]interface{})["statistic"].(map[string]interface{}), "created_at")
	}

	doc = widenJSON(doc)
	return types.JSONDocument{Val: doc}
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
func AssertErr(t *testing.T, e QueryEngine, harness Harness, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	AssertErrWithCtx(t, e, harness, NewContext(harness), query, expectedErrKind, errStrs...)
}

// AssertErrWithBindings asserts that the given query returns an error during its execution, optionally specifying a
// type of error.
func AssertErrWithBindings(t *testing.T, e QueryEngine, harness Harness, query string, bindings map[string]*querypb.BindVariable, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx := NewContext(harness)
	sch, iter, err := e.QueryWithBindings(ctx, query, nil, bindings)
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
func AssertErrWithCtx(t *testing.T, e QueryEngine, harness Harness, ctx *sql.Context, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx = ctx.WithQuery(query)
	sch, iter, err := e.Query(ctx, query)
	if err == nil {
		_, err = sql.RowIterToRows(ctx, sch, iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		err = sql.UnwrapError(err)
		require.True(t, expectedErrKind.Is(err), "Expected error of type %s but got %s", expectedErrKind, err)
	}
	// If there are multiple error strings then we only match against the first
	if len(errStrs) >= 1 {
		require.Equal(t, errStrs[0], err.Error())
	}
	validateEngine(t, ctx, harness, e)
}

// AssertErrPrepared asserts that the given query returns an error during its execution, optionally specifying a type of error.
func AssertErrPrepared(t *testing.T, e QueryEngine, harness Harness, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	AssertErrPreparedWithCtx(t, e, harness, NewContext(harness), query, expectedErrKind, errStrs...)
}

// AssertErrPreparedWithCtx is the same as AssertErr, but uses the context given instead of creating one from a harness
func AssertErrPreparedWithCtx(t *testing.T, e QueryEngine, harness Harness, ctx *sql.Context, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx = ctx.WithQuery(query)
	_, _, err := runQueryPreparedWithCtx(t, ctx, e, query, nil)
	require.Error(t, err)
	if expectedErrKind != nil {
		err = sql.UnwrapError(err)
		require.True(t, expectedErrKind.Is(err), "Expected error of type %s but got %s", expectedErrKind, err)
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
	e QueryEngine,
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
		checkResults(t, expected, expectedCols, sch, rows, query)
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
	case *plan.Releaser:
		return ExtractQueryNode(node.Child)
	default:
		return node
	}
}

// RunWriteQueryTest runs the specified |tt| WriteQueryTest using the specified harness.
func RunWriteQueryTest(t *testing.T, harness Harness, tt queries.WriteQueryTest) {
	e := mustNewEngine(t, harness)
	defer e.Close()
	RunWriteQueryTestWithEngine(t, harness, e, tt)
}

// RunWriteQueryTestWithEngine runs the specified |tt| WriteQueryTest, using the specified harness and engine. Callers
// are still responsible for closing the engine.
func RunWriteQueryTestWithEngine(t *testing.T, harness Harness, e QueryEngine, tt queries.WriteQueryTest) {
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
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, tt.WriteQuery, tt.ExpectedWriteResult, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, tt.SelectQuery, tt.ExpectedSelect, nil, nil)
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
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.WriteQuery, tt.ExpectedWriteResult, nil, tt.Bindings)
		TestPreparedQueryWithContext(t, ctx, e, harness, tt.SelectQuery, tt.ExpectedSelect, nil, tt.Bindings)
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
		if tt.ExpectedErrStr == "" {
			AssertErr(t, e, h, tt.Query, tt.ExpectedErr)
		} else {
			AssertErr(t, e, h, tt.Query, tt.ExpectedErr, tt.ExpectedErrStr)
		}

	})
}

func validateEngine(t *testing.T, ctx *sql.Context, harness Harness, e QueryEngine) {
	if harness == nil {
		assert.NotNil(t, harness)
	}
	require.NotNil(t, harness)
	if vh, ok := harness.(ValidatingHarness); ok {
		if sqlEng, ok := e.(*sqle.Engine); ok {
			assert.NoError(t, vh.ValidateEngine(ctx, sqlEng))
		}
	}
}
