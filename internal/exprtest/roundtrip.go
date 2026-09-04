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

package exprtest

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ParseExpression parses the String representation of original as exactly one SELECT expression. Callers must inspect
// the returned AST fields and compare them to original; successfully parsing alone is not a round-trip assertion.
func ParseExpression(t testing.TB, original fmt.Stringer) sqlparser.SelectExpr {
	t.Helper()
	statement, err := sqlparser.Parse("SELECT " + original.String())
	require.NoError(t, err)

	selectStatement, ok := statement.(*sqlparser.Select)
	require.Truef(t, ok, "expected SELECT statement, found %T", statement)
	require.Len(t, selectStatement.SelectExprs, 1)
	return selectStatement.SelectExprs[0]
}

// RequireExpression returns the expression contained by a parsed SELECT expression.
func RequireExpression(t testing.TB, parsed sqlparser.SelectExpr) sqlparser.Expr {
	t.Helper()
	aliased, ok := parsed.(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased expression, found %T", parsed)
	return aliased.Expr
}

// RequireFunction returns a parsed function expression.
func RequireFunction(t testing.TB, parsed sqlparser.SelectExpr) *sqlparser.FuncExpr {
	t.Helper()
	expression := RequireExpression(t, parsed)
	function, ok := expression.(*sqlparser.FuncExpr)
	require.Truef(t, ok, "expected function expression, found %T", expression)
	return function
}

// RequireColumn returns a parsed column expression.
func RequireColumn(t testing.TB, parsed sqlparser.SelectExpr) *sqlparser.ColName {
	t.Helper()
	expression := RequireExpression(t, parsed)
	column, ok := expression.(*sqlparser.ColName)
	require.Truef(t, ok, "expected column expression, found %T", expression)
	return column
}

// RequireFunctionArgument returns a parsed function argument.
func RequireFunctionArgument(t testing.TB, function *sqlparser.FuncExpr, index int) sqlparser.Expr {
	t.Helper()
	require.Greater(t, len(function.Exprs), index)
	argument, ok := function.Exprs[index].(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased function argument, found %T", function.Exprs[index])
	return argument.Expr
}

// AssertFunctionRoundTrip compares the semantic fields parsed from original.String() to the fields of original.
func AssertFunctionRoundTrip(t testing.TB, original sql.FunctionExpression) {
	t.Helper()
	AssertFunctionRoundTripAs(t, original, original.FunctionName())
}

// AssertFunctionRoundTripAs is AssertFunctionRoundTrip for expressions whose parser-visible name differs from their
// registry name.
func AssertFunctionRoundTripAs(t testing.TB, original sql.FunctionExpression, parsedName string) {
	t.Helper()
	parsed := RequireFunction(t, ParseExpression(t, original))
	require.False(t, parsed.Distinct)
	assertFunction(t, parsed, original, parsedName)
}

// AssertDistinctFunctionRoundTrip additionally verifies the DISTINCT modifier on a function expression.
func AssertDistinctFunctionRoundTripAs(t testing.TB, original sql.FunctionExpression, parsedName string) {
	t.Helper()
	parsed := RequireFunction(t, ParseExpression(t, original))
	require.True(t, parsed.Distinct)
	assertFunction(t, parsed, original, parsedName)
}

// AssertColumnRoundTrip compares a parsed column name and qualifier to the original expression.
func AssertColumnRoundTrip(t testing.TB, original sql.Expression) {
	t.Helper()
	AssertExpressionValue(t, RequireExpression(t, ParseExpression(t, original)), original)
}

// AssertAliasRoundTrip compares both the parsed alias and its value to the original expression.
func AssertAliasRoundTrip(t testing.TB, original interface {
	sql.Expression
	sql.Nameable
}) {
	t.Helper()
	parsed, ok := ParseExpression(t, original).(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased expression, found %T", parsed)
	require.Equal(t, original.Name(), parsed.As.String())
	require.Len(t, original.Children(), 1)
	AssertExpressionValue(t, parsed.Expr, original.Children()[0])
}

// AssertLiteralRoundTrip compares the parsed literal token and value to the original literal value.
func AssertLiteralRoundTrip(t testing.TB, original sql.Expression) {
	t.Helper()
	parsed := RequireExpression(t, ParseExpression(t, original))
	valueExpression, ok := original.(interface{ Value() interface{} })
	require.Truef(t, ok, "expected literal expression, found %T", original)
	value := valueExpression.Value()
	typ := original.Type(sql.NewEmptyContext())

	if stringValue, ok := value.(string); ok && types.IsBinaryType(typ) {
		actual := requireSQLValue(t, parsed, sqlparser.HexNum)
		decoded, err := hex.DecodeString(strings.TrimPrefix(string(actual.Val), "0x"))
		require.NoError(t, err)
		require.Equal(t, []byte(stringValue), decoded)
		return
	}
	switch value := value.(type) {
	case time.Time, types.Timespan:
		actual := requireSQLValue(t, parsed, sqlparser.StrVal)
		converted, _, err := typ.Convert(context.Background(), string(actual.Val))
		require.NoError(t, err)
		comparison, err := typ.Compare(context.Background(), value, converted)
		require.NoError(t, err)
		require.Zero(t, comparison)
	case types.JSONDocument:
		actual, ok := parsed.(*sqlparser.ConvertExpr)
		require.Truef(t, ok, "expected JSON cast, found %T", parsed)
		require.Equal(t, "cast", strings.ToLower(actual.Name))
		require.Equal(t, "json", strings.ToLower(actual.Type.Type))
		jsonText := requireSQLValue(t, actual.Expr, sqlparser.StrVal)
		converted, _, err := types.JSON.Convert(context.Background(), string(jsonText.Val))
		require.NoError(t, err)
		comparison, err := value.Compare(context.Background(), converted.(sql.JSONWrapper))
		require.NoError(t, err)
		require.Zero(t, comparison)
	default:
		assertLiteral(t, parsed, value)
	}
}

// AssertBindVariableRoundTrip compares the parser bind-variable token to the original bind variable name.
func AssertBindVariableRoundTrip(t testing.TB, original fmt.Stringer, name string) {
	t.Helper()
	parsed := requireSQLValue(t, RequireExpression(t, ParseExpression(t, original)), sqlparser.ValArg)
	require.Equal(t, ":"+name, string(parsed.Val))
}

// AssertStarRoundTrip compares a parsed star qualifier to the original table qualifier.
func AssertStarRoundTrip(t testing.TB, original sql.Expression, table string) {
	t.Helper()
	parsed, ok := ParseExpression(t, original).(*sqlparser.StarExpr)
	require.Truef(t, ok, "expected star expression, found %T", parsed)
	require.Equal(t, table, parsed.TableName.Name.String())
}

func assertFunction(t testing.TB, parsed *sqlparser.FuncExpr, original sql.FunctionExpression, parsedName string) {
	t.Helper()
	require.Equal(t, strings.ToLower(parsedName), parsed.Name.Lowered())
	require.True(t, parsed.Qualifier.IsEmpty())
	children := original.Children()
	if windowed, ok := original.(sql.WindowAdaptableExpression); ok && windowed.Window() != nil && len(children) != len(parsed.Exprs) {
		require.Equal(t, len(parsed.Exprs)+windowed.Window().ExpressionsLen(), len(children))
		children = children[:len(parsed.Exprs)]
	}
	require.Equal(t, len(children), len(parsed.Exprs))
	for i, child := range children {
		if table, ok := starTable(child); ok {
			star, ok := parsed.Exprs[i].(*sqlparser.StarExpr)
			require.Truef(t, ok, "expected star function argument, found %T", parsed.Exprs[i])
			require.Equal(t, table, star.TableName.Name.String())
			continue
		}
		AssertExpressionValue(t, RequireFunctionArgument(t, parsed, i), child)
	}

	windowed, ok := original.(sql.WindowAdaptableExpression)
	if !ok || windowed.Window() == nil {
		require.Nil(t, parsed.Over)
		return
	}
	assertWindow(t, parsed.Over, windowed.Window())
}

func assertWindow(t testing.TB, parsed *sqlparser.Over, original *sql.WindowDefinition) {
	t.Helper()
	require.NotNil(t, parsed)
	require.Equal(t, original.Ref, parsed.NameRef.String())
	require.Len(t, parsed.PartitionBy, len(original.PartitionBy))
	for i, partition := range original.PartitionBy {
		AssertExpressionValue(t, parsed.PartitionBy[i], partition)
	}
	require.Len(t, parsed.OrderBy, len(original.OrderBy))
	for i, order := range original.OrderBy {
		AssertExpressionValue(t, parsed.OrderBy[i].Expr, order.Expr)
		require.Equal(t, strings.ToLower(order.Order.String()), parsed.OrderBy[i].Direction)
	}
	// WindowFrame does not expose whether it represents ROWS or RANGE. Framed-window tests must inspect Frame directly.
	require.Nil(t, original.Frame)
	require.Nil(t, parsed.Frame)
}

// AssertExpressionValue compares a parsed AST expression to the corresponding original GMS expression.
func AssertExpressionValue(t testing.TB, parsed sqlparser.Expr, original sql.Expression) {
	t.Helper()

	if valueExpression, ok := original.(interface{ Value() interface{} }); ok {
		assertLiteral(t, parsed, valueExpression.Value())
		return
	}
	if function, ok := original.(sql.FunctionExpression); ok {
		parsedFunction, ok := parsed.(*sqlparser.FuncExpr)
		require.Truef(t, ok, "expected function expression, found %T", parsed)
		assertFunction(t, parsedFunction, function, function.FunctionName())
		return
	}
	if nameable, ok := original.(sql.Nameable); ok {
		column, ok := parsed.(*sqlparser.ColName)
		require.Truef(t, ok, "expected column name, found %T", parsed)
		require.Equal(t, nameable.Name(), column.Name.String())
		if tableable, ok := original.(sql.Tableable); ok {
			require.Equal(t, tableable.Table(), column.Qualifier.Name.String())
		}
		return
	}

	require.Failf(t, "unsupported original expression", "%T", original)
}

func starTable(original sql.Expression) (string, bool) {
	value := reflect.ValueOf(original)
	if value.Kind() != reflect.Ptr || !value.Elem().IsValid() || value.Elem().Type().Name() != "Star" {
		return "", false
	}
	field := value.Elem().FieldByName("Table")
	return field.String(), true
}

func assertLiteral(t testing.TB, parsed sqlparser.Expr, original interface{}) {
	t.Helper()
	switch value := original.(type) {
	case nil:
		require.IsType(t, &sqlparser.NullVal{}, parsed)
	case bool:
		actual, ok := parsed.(sqlparser.BoolVal)
		require.Truef(t, ok, "expected boolean literal, found %T", parsed)
		require.Equal(t, value, bool(actual))
	case string:
		actual := requireSQLValue(t, parsed, sqlparser.StrVal)
		require.Equal(t, value, string(actual.Val))
	case []byte:
		actual := requireSQLValue(t, parsed, sqlparser.HexNum)
		decoded, err := hex.DecodeString(strings.TrimPrefix(string(actual.Val), "0x"))
		require.NoError(t, err)
		require.Equal(t, value, decoded)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		actual := requireSQLValue(t, parsed, sqlparser.IntVal)
		require.Equal(t, fmt.Sprint(value), string(actual.Val))
	case float32, float64:
		actual := requireSQLValue(t, parsed, sqlparser.FloatVal)
		require.Equal(t, fmt.Sprint(value), string(actual.Val))
	case types.GeometryValue:
		actual, ok := parsed.(*sqlparser.FuncExpr)
		require.Truef(t, ok, "expected geometry constructor, found %T", parsed)
		require.Equal(t, "st_geomfromwkb", actual.Name.Lowered())
		require.Len(t, actual.Exprs, 2)
		binary := requireSQLValue(t, RequireFunctionArgument(t, actual, 0), sqlparser.HexNum)
		decoded, err := hex.DecodeString(strings.TrimPrefix(string(binary.Val), "0x"))
		require.NoError(t, err)
		require.Equal(t, value.Serialize()[types.SRIDSize:], decoded)
		srid := requireSQLValue(t, RequireFunctionArgument(t, actual, 1), sqlparser.IntVal)
		require.Equal(t, fmt.Sprint(value.GetSRID()), string(srid.Val))
	default:
		require.Failf(t, "unsupported literal value", "%T", original)
	}
}

func requireSQLValue(t testing.TB, parsed sqlparser.Expr, valueType sqlparser.ValType) *sqlparser.SQLVal {
	t.Helper()
	value, ok := parsed.(*sqlparser.SQLVal)
	require.Truef(t, ok, "expected SQL value, found %T", parsed)
	require.Equal(t, valueType, value.Type)
	return value
}
