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

package expression_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// These tests keep expression SQL strings and their parsed values together across expression packages.
func TestAliasReferenceString(t *testing.T) {
	expr := expression.NewAliasReference("alias_name")
	require.Equal(t, "alias_name", expr.String())
	assertColumnRoundTrip(t, expr)
}

func TestAliasString(t *testing.T) {
	expr := expression.NewAlias(sql.NewEmptyContext(), "alias name", expression.NewLiteral(42, types.Int64))
	require.Equal(t, "42 as `alias name`", expr.String())
	assertAliasRoundTrip(t, expr)
}

func TestAnyValueString(t *testing.T) {
	expr := aggregation.NewAnyValue(expression.NewGetField(0, types.Int64, "value", false))
	require.Equal(t, "ANY_VALUE(value)", expr.String())
	assertFunctionRoundTripAs(t, expr, "ANY_VALUE")
}

func TestAsWKTString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := spatial.NewAsWKT(ctx, expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
	require.Equal(t, "st_aswkt(ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040, 0))", f.String())
	assertFunctionRoundTrip(t, f.(sql.FunctionExpression))
}

func TestAtanString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr, err := function.NewAtan(
		ctx,
		expression.NewLiteral(1, types.Int64),
		expression.NewLiteral(2, types.Int64),
	)
	require.NoError(t, err)
	require.Equal(t, "atan(1, 2)", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestAvgString(t *testing.T) {
	require := require.New(t)

	avg := aggregation.NewAvg(expression.NewGetField(0, types.Int32, "col1", true))
	require.Equal("AVG(col1)", avg.String())

	windowed := avg.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{
		PartitionBy: []sql.Expression{expression.NewGetField(1, types.Int32, "col2", true)},
	})
	require.Equal("AVG(col1) over ( partition by col2)", windowed.String())
	assertFunctionRoundTrip(t, windowed.(sql.FunctionExpression))
}

func TestBindVarString(t *testing.T) {
	expr := expression.NewBindVar("arg1")
	require.Equal(t, ":arg1", expr.String())
	assertBindVariableRoundTrip(t, expr, expr.Name)
}

func TestBitAndString(t *testing.T) {
	assert := require.New(t)
	m := aggregation.NewBitAnd(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BIT_AND(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("BIT_AND(field) over ()", windowed.String())
	assertFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "BIT_AND")
}

func TestBitOrString(t *testing.T) {
	assert := require.New(t)
	m := aggregation.NewBitOr(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BIT_OR(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("BIT_OR(field) over ()", windowed.String())
	assertFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "BIT_OR")
}

func TestBitXorString(t *testing.T) {
	assert := require.New(t)
	m := aggregation.NewBitXor(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("BIT_XOR(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("BIT_XOR(field) over ()", windowed.String())
	assertFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "BIT_XOR")
}

func TestCharString(t *testing.T) {
	expr, err := function.NewChar(sql.NewEmptyContext(), expression.NewLiteral(65, types.Int64))
	require.NoError(t, err)
	charExpr := expr.(*function.Char)
	charExpr.Collation = sql.Collation_utf8mb4_0900_ai_ci

	require.Equal(t, "char(65 USING utf8mb4)", charExpr.String())
	parsed, ok := requireExpression(t, parseExpression(t, charExpr)).(*sqlparser.CharExpr)
	require.True(t, ok)
	require.Equal(t, charExpr.Collation.CharacterSet().Name(), parsed.Type)
	require.Len(t, parsed.Exprs, len(charExpr.Children()))
	for i, child := range charExpr.Children() {
		assertExpressionValue(t, parsed.Exprs[i].(*sqlparser.AliasedExpr).Expr, child)
	}
}

func TestColDefaultExpressionString(t *testing.T) {
	expr := plan.ColDefaultExpression{Column: &sql.Column{Name: "odd` name", Type: types.Int64}}
	require.Equal(t, "DEFAULT(`odd`` name`)", expr.String())
	parsed, ok := requireExpression(t, parseExpression(t, expr)).(*sqlparser.Default)
	require.True(t, ok)
	require.Equal(t, expr.Column.Name, parsed.ColName)
}

func TestColumnDefaultValueString(t *testing.T) {
	var implicitDefault *sql.ColumnDefaultValue
	require.Equal(t, "", implicitDefault.String())

	explicitNull, err := sql.NewColumnDefaultValue(sql.UnresolvedColumnDefault{ExprString: "NULL"}, nil, true, false, true)
	require.NoError(t, err)
	require.Equal(t, "NULL", explicitNull.String())
	statement, err := sqlparser.Parse("SELECT " + explicitNull.String())
	require.NoError(t, err)
	parsed := statement.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	require.IsType(t, &sqlparser.NullVal{}, parsed)
	require.Equal(t, "NULL", explicitNull.Expr.(sql.UnresolvedColumnDefault).ExprString)
}

func TestCountDistinctString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	count := aggregation.NewCountDistinct(
		expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewGetField(1, types.Int64, "bar", false),
	)
	require.Equal(t, "COUNT(DISTINCT foo, bar)", count.String())
	assertDistinctFunctionRoundTripAs(t, count, "COUNT")

	window := sql.NewWindowDefinition(
		[]sql.Expression{expression.NewGetField(2, types.Int64, "baz", false)},
		nil, nil, "", "",
	)
	windowed := count.WithWindow(ctx, window)
	require.Equal(t, "COUNT(DISTINCT foo, bar) over ( partition by baz)", windowed.String())
	assertDistinctFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "COUNT")
}

func TestCountString(t *testing.T) {
	count := aggregation.NewCount(expression.NewStar()).WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "COUNT(*) over ()", count.String())
	assertFunctionRoundTrip(t, count.(sql.FunctionExpression))
}

func TestFindInSetString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := function.NewFindInSet(
		ctx,
		expression.NewLiteral("needle", types.Text),
		expression.NewLiteral("haystack,needle", types.Text),
	)
	require.Equal(t, "find_in_set('needle', 'haystack,needle')", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestFirstString(t *testing.T) {
	expr := aggregation.NewFirst(expression.NewGetField(0, types.Text, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "FIRST_VALUE(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "FIRST_VALUE")
}

func TestFormatString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	twoArgs, err := function.NewFormat(ctx, expression.NewLiteral(1234, types.Int64), expression.NewLiteral(2, types.Int64))
	require.NoError(t, err)
	require.Equal(t, "format(1234,2)", twoArgs.String())
	assertFunctionRoundTrip(t, twoArgs.(sql.FunctionExpression))

	threeArgs, err := function.NewFormat(ctx, expression.NewLiteral(1234, types.Int64), expression.NewLiteral(2, types.Int64), expression.NewLiteral("de_DE", types.Text))
	require.NoError(t, err)
	require.Equal(t, "format(1234,2,'de_DE')", threeArgs.String())
	assertFunctionRoundTrip(t, threeArgs.(sql.FunctionExpression))
}

func TestGeomCollFromTextString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	arg := expression.NewLiteral("GEOMETRYCOLLECTION(POINT(1 2))", types.Text)
	expr, err := spatial.NewGeomCollFromText(ctx, arg)
	require.NoError(t, err)
	require.IsType(t, &spatial.GeomCollFromText{}, expr)
	require.Equal(t, "st_geomcollfromtext('GEOMETRYCOLLECTION(POINT(1 2))')", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))

	cloned, err := expr.WithChildren(ctx, arg)
	require.NoError(t, err)
	require.IsType(t, &spatial.GeomCollFromText{}, cloned)
}

func TestGeomCollFromWKBString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	arg := expression.NewLiteral([]byte{1, 7, 0, 0, 0, 0, 0, 0, 0}, types.Blob)
	expr, err := spatial.NewGeomCollFromWKB(ctx, arg)
	require.NoError(t, err)
	require.IsType(t, &spatial.GeomCollFromWKB{}, expr)
	require.Equal(t, "st_geomcollfromwkb(0x010700000000000000)", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))

	cloned, err := expr.WithChildren(ctx, arg)
	require.NoError(t, err)
	require.IsType(t, &spatial.GeomCollFromWKB{}, cloned)
}

func TestGetFieldDescribeUsesSeparateDebugName(t *testing.T) {
	expr := expression.NewGetField(2, types.Int64, "sum(x) over ()", false).WithDebugName("sum\n └─ x\n")

	require.Equal(t, "sum(x) over ()", expr.String())
	require.Equal(t, "sum(x) over ()", sql.Describe(nil, expr, sql.DescribeOptions{Estimates: true}))
	require.Equal(t, "sum\n └─ x\n:2!null", sql.Describe(nil, expr, sql.DescribeOptions{Debug: true}))
}

func TestGetFieldString(t *testing.T) {
	tests := []struct {
		expr     *expression.GetField
		expected string
	}{
		{expression.NewGetField(0, types.Int64, "normal_name", false), "normal_name"},
		{expression.NewGetFieldWithTable(0, 0, types.Int64, "", "table_name", "column_name", false), "table_name.column_name"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		assertColumnRoundTrip(t, test.expr)
	}
}

func TestGroupConcatString(t *testing.T) {
	assert := require.New(t)

	m := aggregation.NewGroupConcat("distinct ", nil, ",", []sql.Expression{expression.NewUnresolvedColumn("field")}, 1024)

	assert.Equal("group_concat(distinct field separator ',')", m.String())

	m = aggregation.NewGroupConcat("distinct ", nil, "-", []sql.Expression{expression.NewUnresolvedColumn("field")}, 1024)

	assert.Equal("group_concat(distinct field separator '-')", m.String())

	sc := sql.SortConditions{
		{Expr: expression.NewUnresolvedColumn("field"), Order: sql.Ascending},
		{Expr: expression.NewUnresolvedColumn("field2"), Order: sql.Descending},
	}

	outputs := []sql.Expression{expression.NewUnresolvedColumn("field")}
	separator := "a'b\\c"
	m = aggregation.NewGroupConcat("distinct ", sc, separator, outputs, 1024)

	assert.Equal("group_concat(distinct field order by field ASC, field2 DESC separator 'a''b\\\\c')", m.String())
	parsed, ok := requireExpression(t, parseExpression(t, m)).(*sqlparser.GroupConcatExpr)
	require.True(t, ok)
	require.Equal(t, "distinct ", parsed.Distinct)
	require.Len(t, parsed.Exprs, len(outputs))
	for i, output := range outputs {
		assertExpressionValue(t, parsed.Exprs[i].(*sqlparser.AliasedExpr).Expr, output)
	}
	require.Len(t, parsed.OrderBy, len(sc))
	for i, order := range sc {
		assertExpressionValue(t, parsed.OrderBy[i].Expr, order.Expr)
		expectedDirection := sqlparser.AscScr
		if order.Order == sql.Descending {
			expectedDirection = sqlparser.DescScr
		}
		require.Equal(t, expectedDirection, parsed.OrderBy[i].Direction)
	}
	require.Equal(t, separator, parsed.Separator.SeparatorString)
	require.False(t, parsed.Separator.DefaultSeparator)

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("group_concat(distinct field order by field ASC, field2 DESC separator 'a''b\\\\c') over ()", windowed.String())
}

func TestInSubqueryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := plan.NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), plan.NewSubquery(nil, "select j from t"))
	require.Equal(t, "(i IN (select j from t))", expr.String())
	assertParsedInSubquery(t, requireExpression(t, parseExpression(t, expr)), expr)
}

func TestJSONObjectAggString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := aggregation.NewJSONObjectAgg(
		ctx,
		expression.NewGetField(0, types.Text, "k", false),
		expression.NewGetField(1, types.Int64, "v", false),
	).(sql.WindowAdaptableExpression).WithWindow(
		ctx,
		sql.NewWindowDefinition(
			[]sql.Expression{expression.NewGetField(2, types.Int64, "g", false)},
			nil, nil, "", "",
		),
	)
	require.Equal(t, "JSON_OBJECTAGG(k, v) over ( partition by g)", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestJSONSearchString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	for _, argc := range []int{3, 4, 5, 6} {
		t.Run(fmt.Sprint(argc), func(t *testing.T) {
			args := make([]sql.Expression, argc)
			names := make([]string, argc)
			for i := range args {
				names[i] = fmt.Sprintf("arg%d", i)
				args[i] = expression.NewGetField(i, types.LongText, names[i], true)
			}
			expr, err := json.NewJSONSearch(ctx, args...)
			require.NoError(t, err)
			require.Equal(t, "json_search("+strings.Join(names, ", ")+")", expr.String())
			assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
		})
	}
}

func TestJsonArrayAggString(t *testing.T) {
	assert := require.New(t)

	m := aggregation.NewJsonArray(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("JSON_ARRAYAGG(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("JSON_ARRAYAGG(field) over ()", windowed.String())
	assertFunctionRoundTripAs(t, windowed.(sql.FunctionExpression), "JSON_ARRAYAGG")
}

func TestJsonLengthString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr, err := json.NewJsonLength(
		ctx,
		expression.NewGetField(0, types.JSON, "doc", false),
		expression.NewLiteral("$.items", types.Text),
	)
	require.NoError(t, err)
	require.Equal(t, "json_length(doc, '$.items')", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestJsonValueString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	doc := expression.NewGetField(0, types.JSON, "doc", false)
	path := expression.NewLiteral("$.a", types.Text)

	signed, err := json.NewJsonValue(ctx, doc, path, expression.NewLiteral(int64(0), types.Int64))
	require.NoError(t, err)
	require.Equal(t, "json_value(doc, '$.a', 'signed')", signed.String())
	parsedSigned := requireFunction(t, parseExpression(t, signed))
	signedValue := signed.(*json.JsonValue)
	require.Equal(t, signedValue.FunctionName(), parsedSigned.Name.Lowered())
	require.Len(t, parsedSigned.Exprs, 3)
	assertExpressionValue(t, requireFunctionArgument(t, parsedSigned, 0), signedValue.JSON)
	assertExpressionValue(t, requireFunctionArgument(t, parsedSigned, 1), signedValue.Path)
	parsedType := requireFunctionArgument(t, parsedSigned, 2).(*sqlparser.SQLVal)
	require.Equal(t, sqlparser.StrVal, parsedType.Type)
	require.True(t, types.IsSigned(signedValue.Typ))
	require.Equal(t, "signed", string(parsedType.Val))

	defaultType, err := json.NewJsonValue(ctx, doc, path)
	require.NoError(t, err)
	require.Equal(t, "json_value(doc, '$.a')", defaultType.String())
	parsedDefault := requireFunction(t, parseExpression(t, defaultType))
	defaultValue := defaultType.(*json.JsonValue)
	require.Equal(t, defaultValue.FunctionName(), parsedDefault.Name.Lowered())
	require.Len(t, parsedDefault.Exprs, 2)
	assertExpressionValue(t, requireFunctionArgument(t, parsedDefault, 0), defaultValue.JSON)
	assertExpressionValue(t, requireFunctionArgument(t, parsedDefault, 1), defaultValue.Path)
}

func TestLastInsertIdString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	withoutArg, err := function.NewLastInsertId(ctx)
	require.NoError(t, err)
	require.Equal(t, "last_insert_id()", withoutArg.String())
	assertFunctionRoundTrip(t, withoutArg.(sql.FunctionExpression))

	withArg, err := function.NewLastInsertId(ctx, expression.NewLiteral(42, types.Int64))
	require.NoError(t, err)
	require.Equal(t, "last_insert_id(42)", withArg.String())
	assertFunctionRoundTrip(t, withArg.(sql.FunctionExpression))
}

func TestLastString(t *testing.T) {
	expr := aggregation.NewLast(expression.NewGetField(0, types.Text, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "LAST_VALUE(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "LAST_VALUE")
}

func TestLiteralStringRoundTrips(t *testing.T) {
	tests := []struct {
		name     string
		literal  *expression.Literal
		expected string
	}{
		{"binary string", expression.NewLiteral("a\x00", types.MustCreateBinary(query.Type_VARBINARY, 2)), "0x6100"},
		{"date", expression.NewLiteral(time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC), types.Date), "'2026-09-04'"},
		{"datetime", expression.NewLiteral(time.Date(2026, time.September, 4, 1, 2, 3, 456000000, time.UTC), types.DatetimeMaxPrecision), "'2026-09-04 01:02:03.456'"},
		{"time", expression.NewLiteral(types.Timespan(45_296_123_456), types.Time), "'12:34:56.123456'"},
		{"json", expression.NewLiteral(types.MustJSON(`{"a": 1}`), types.JSON), `CAST('{"a": 1}' AS JSON)`},
		{"geometry", expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}), "ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040, 4326)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.literal.String())
			assertLiteralRoundTrip(t, tt.literal)
		})
	}
}

func TestMaxString(t *testing.T) {
	assert := require.New(t)
	m := aggregation.NewMax(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("MAX(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("MAX(field) over ()", windowed.String())
	assertFunctionRoundTrip(t, windowed.(sql.FunctionExpression))
}

func TestMinString(t *testing.T) {
	assert := require.New(t)

	m := aggregation.NewMin(expression.NewGetField(0, types.Int32, "field", true))
	assert.Equal("MIN(field)", m.String())

	windowed := m.WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	assert.Equal("MIN(field) over ()", windowed.String())
	assertFunctionRoundTrip(t, windowed.(sql.FunctionExpression))
}

func TestMinuteString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := function.NewMinute(ctx, expression.NewGetField(0, types.LongText, "foo", false))
	require.Equal(t, "minute(foo)", f.String())
	assertFunctionRoundTrip(t, f.(sql.FunctionExpression))
}

func TestMultiLineStringFromTextRegistryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	reg := function.NewRegistry()
	reg.Register(function.BuiltIns...)
	fn, ok := reg.Function(ctx, "", "st_multilinestringfromtext")
	require.True(t, ok)
	expr, err := fn.NewInstance(ctx, []sql.Expression{
		expression.NewLiteral("MULTILINESTRING((1 2, 3 4))", types.Text),
	})
	require.NoError(t, err)
	require.IsType(t, &spatial.MLineFromText{}, expr)
	require.Equal(t, "st_mlinefromtext('MULTILINESTRING((1 2, 3 4))')", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestNTileString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	buckets := expression.NewLiteral(2, types.Int64)
	expr := window.NewNTile(ctx, buckets)
	expr = expr.(sql.WindowAdaptableExpression).WithWindow(ctx, sql.NewWindowDefinition(nil, nil, nil, "", ""))
	require.Equal(t, "ntile(2) over ()", expr.String())
	parsed := requireFunction(t, parseExpression(t, expr))
	require.Equal(t, strings.ToLower(expr.(sql.FunctionExpression).FunctionName()), parsed.Name.Lowered())
	require.Len(t, parsed.Exprs, 1)
	assertExpressionValue(t, requireFunctionArgument(t, parsed, 0), buckets)
	require.NotNil(t, parsed.Over)
	require.Empty(t, parsed.Over.PartitionBy)
	require.Empty(t, parsed.Over.OrderBy)
	require.Nil(t, parsed.Over.Frame)
}

func TestNestedInSubqueryDescription(t *testing.T) {
	ctx := sql.NewEmptyContext()
	subquery := plan.NewSubquery(NewUnresolvedTable("t", ""), "select j from t")
	inSubquery := plan.NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), subquery)
	expr := expression.NewAnd(expression.NewLiteral(true, types.Boolean), inSubquery)

	description := sql.Describe(ctx, expr, sql.DescribeOptions{Estimates: true})
	require.Contains(t, description, "InSubquery")
	require.Contains(t, description, "right: Subquery")
	require.Contains(t, description, "UnresolvedTable(t)")

	require.Equal(t, "(true AND (i IN (select j from t)))", expr.String())
	parsed := requireExpression(t, parseExpression(t, expr)).(*sqlparser.ParenExpr)
	parsedAnd := parsed.Expr.(*sqlparser.AndExpr)
	assertExpressionValue(t, parsedAnd.Left, expr.Children()[0])
	assertParsedInSubquery(t, parsedAnd.Right, inSubquery)
}

func TestNowString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	precision := expression.NewLiteral(3, types.Int64)

	now, err := function.NewNow(ctx, precision)
	require.NoError(t, err)
	require.Equal(t, "NOW(3)", now.String())
	assertFunctionRoundTrip(t, now.(sql.FunctionExpression))

	sysdate, err := function.NewSysdate(ctx, precision)
	require.NoError(t, err)
	require.Equal(t, "SYSDATE(3)", sysdate.String())
	assertFunctionRoundTripAs(t, sysdate.(sql.FunctionExpression), "SYSDATE")

	cloned, err := sysdate.WithChildren(ctx, sysdate.Children()...)
	require.NoError(t, err)
	require.Equal(t, "SYSDATE(3)", cloned.String())
}

func TestProcedureParamString(t *testing.T) {
	expr := expression.NewProcedureParam("param name", types.Int64)
	require.Equal(t, "`param name`", expr.String())
	assertColumnRoundTrip(t, expr)
}

func TestInTupleStrings(t *testing.T) {
	assert.Equal(t, "(foo IN (foo, 2))", expression.NewInTuple(expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewTuple(
			expression.NewGetField(0, types.Int64, "foo", false),
			expression.NewLiteral(int64(2), types.Int64),
		)).String())
	hit, err := expression.NewHashInTuple(nil, expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(2), types.Int64),
		))
	assert.NoError(t, err)
	assert.Equal(t, "(foo IN (2))", hit.String())
	parsedParen := requireExpression(t, parseExpression(t, hit)).(*sqlparser.ParenExpr)
	parsedIn := parsedParen.Expr.(*sqlparser.ComparisonExpr)
	assert.Equal(t, sqlparser.InStr, parsedIn.Operator)
	assertExpressionValue(t, parsedIn.Left, hit.Left())
	parsedTuple := parsedIn.Right.(sqlparser.ValTuple)
	originalTuple := hit.Right().(expression.Tuple)
	assert.Len(t, parsedTuple, len(originalTuple))
	for i, value := range originalTuple {
		assertExpressionValue(t, parsedTuple[i], value)
	}
	assert.Equal(t, "(foo HASH IN (2))", sql.Describe(nil, hit, sql.DescribeOptions{Estimates: true}))
	nested := expression.NewAnd(expression.NewLiteral(true, types.Boolean), hit)
	assert.Equal(t, "(true AND (foo HASH IN (2)))", sql.Describe(nil, nested, sql.DescribeOptions{Estimates: true}))
}

func TestSTEqualsRegistryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	reg := function.NewRegistry()
	reg.Register(function.BuiltIns...)
	fn, ok := reg.Function(ctx, "", "st_equals")
	require.True(t, ok)
	point := expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{})
	expr, err := fn.NewInstance(ctx, []sql.Expression{point, point})
	require.NoError(t, err)
	require.IsType(t, &spatial.STEquals{}, expr)
	require.Equal(t, "ST_EQUALS(ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040, 0), ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040, 0))", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestStarString(t *testing.T) {
	tests := []struct {
		expr     *expression.Star
		expected string
	}{
		{expression.NewStar(), "*"},
		{expression.NewQualifiedStar("normal_name"), "normal_name.*"},
		{expression.NewQualifiedStar("table name"), "`table name`.*"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		assertStarRoundTrip(t, test.expr, test.expr.Table)
	}
}

func TestStdDevPopString(t *testing.T) {
	expr := aggregation.NewStdDevPop(expression.NewGetField(0, nil, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "STDDEV_POP(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "STDDEV_POP")
}

func TestStdDevSampString(t *testing.T) {
	expr := aggregation.NewStdDevSamp(expression.NewGetField(0, nil, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "STDDEV_SAMP(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "STDDEV_SAMP")
}

func TestSubqueryString(t *testing.T) {
	expr := plan.NewSubquery(nil, "select 1")
	require.Equal(t, "(select 1)", expr.String())
	parsed := requireExpression(t, parseExpression(t, expr)).(*sqlparser.Subquery)
	assertParsedSubquery(t, parsed, expr)
}

func TestSumString(t *testing.T) {
	expr := aggregation.NewSum(expression.NewGetField(0, nil, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "SUM(value) over ()", expr.String())
	assertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}

func TestSystemVarString(t *testing.T) {
	tests := []struct {
		scope    string
		expected string
	}{
		{"", "@@unregistered_system_variable"},
		{"session", "@@session.unregistered_system_variable"},
	}
	for _, tt := range tests {
		expr := expression.NewSystemVar("unregistered_system_variable", sql.GetMysqlScope(sql.SystemVariableScope_Session), tt.scope)
		require.Equal(t, tt.expected, expr.String())
		parsed := requireColumn(t, parseExpression(t, expr))
		parts := strings.Split(strings.TrimPrefix(parsed.Name.String(), "@@"), ".")
		if expr.SpecifiedScope == "" {
			require.Equal(t, []string{expr.Name}, parts)
		} else {
			require.Equal(t, []string{expr.SpecifiedScope, expr.Name}, parts)
		}
	}
}

func TestUncompressedLengthString(t *testing.T) {
	stringExpr := function.NewUncompressedLength(sql.NewEmptyContext(), expression.NewLiteral("value", types.Text))
	require.Equal(t, "uncompressed_length('value')", stringExpr.String())
	assertFunctionRoundTrip(t, stringExpr.(sql.FunctionExpression))
}

func TestUnresolvedColumnString(t *testing.T) {
	tests := []struct {
		expr     *expression.UnresolvedColumn
		expected string
	}{
		{expression.NewUnresolvedColumn("normal_name"), "normal_name"},
		{expression.NewUnresolvedQualifiedColumn("table_name", "column_name"), "table_name.column_name"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		assertColumnRoundTrip(t, test.expr)
	}
}

func TestUnresolvedFunctionString(t *testing.T) {
	expr := expression.NewUnresolvedFunction("function name", false, nil)
	require.Equal(t, "`function name`()", expr.String())
	parsed := requireFunction(t, parseExpression(t, expr))
	require.Equal(t, expr.Name(), parsed.Name.String())
	require.Equal(t, len(expr.Arguments), len(parsed.Exprs))
	require.Nil(t, parsed.Over)
}

func TestUnresolvedProcedureParamString(t *testing.T) {
	expr := expression.NewUnresolvedProcedureParam("param name")
	require.Equal(t, "`param name`", expr.String())
	assertColumnRoundTrip(t, expr)
}

func TestValidatePasswordStrengthString(t *testing.T) {
	stringExpr := function.NewValidatePasswordStrength(sql.NewEmptyContext(), expression.NewLiteral("value", types.Text))
	require.Equal(t, "validate_password_strength('value')", stringExpr.String())
	assertFunctionRoundTrip(t, stringExpr.(sql.FunctionExpression))
}

func TestVarPopString(t *testing.T) {
	expr := aggregation.NewVarPop(expression.NewGetField(0, nil, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "VAR_POP(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "VAR_POP")
}

func TestVarSampString(t *testing.T) {
	expr := aggregation.NewVarSamp(expression.NewGetField(0, nil, "value", false)).
		WithWindow(sql.NewEmptyContext(), &sql.WindowDefinition{})
	require.Equal(t, "VAR_SAMP(value) over ()", expr.String())
	assertFunctionRoundTripAs(t, expr.(sql.FunctionExpression), "VAR_SAMP")
}

func TestWrapperString(t *testing.T) {
	tests := []struct {
		expr     *expression.Wrapper
		expected string
	}{
		{expression.WrapExpression(nil), "NULL"},
		{expression.WrapExpression(expression.NewLiteral(42, types.Int64)), "(42)"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		parsed := requireExpression(t, parseExpression(t, test.expr))
		if test.expr.Unwrap() == nil {
			require.IsType(t, &sqlparser.NullVal{}, parsed)
		} else {
			parenthesized, ok := parsed.(*sqlparser.ParenExpr)
			require.True(t, ok)
			assertExpressionValue(t, parenthesized.Expr, test.expr.Unwrap())
		}
	}
}

func TestYearWeekString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f, err := function.NewYearWeek(ctx, expression.NewGetField(0, types.LongText, "foo", false))
	require.NoError(t, err)
	require.Equal(t, "YEARWEEK(foo, 0)", f.String())
	assertFunctionRoundTrip(t, f.(sql.FunctionExpression))

	explicitMode, err := function.NewYearWeek(ctx, expression.NewGetField(0, types.LongText, "foo", false), expression.NewLiteral(1, types.Int64))
	require.NoError(t, err)
	require.Equal(t, "YEARWEEK(foo, 1)", explicitMode.String())
	assertFunctionRoundTrip(t, explicitMode.(sql.FunctionExpression))
}

// parseExpression parses the String representation of original as exactly one SELECT expression. Callers must inspect
// the returned AST fields and compare them to original; successfully parsing alone is not a round-trip assertion.
func parseExpression(t testing.TB, original fmt.Stringer) sqlparser.SelectExpr {
	t.Helper()
	statement, err := sqlparser.Parse("SELECT " + original.String())
	require.NoError(t, err)

	selectStatement, ok := statement.(*sqlparser.Select)
	require.Truef(t, ok, "expected SELECT statement, found %T", statement)
	require.Len(t, selectStatement.SelectExprs, 1)
	return selectStatement.SelectExprs[0]
}

// requireExpression returns the expression contained by a parsed SELECT expression.
func requireExpression(t testing.TB, parsed sqlparser.SelectExpr) sqlparser.Expr {
	t.Helper()
	aliased, ok := parsed.(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased expression, found %T", parsed)
	return aliased.Expr
}

// requireFunction returns a parsed function expression.
func requireFunction(t testing.TB, parsed sqlparser.SelectExpr) *sqlparser.FuncExpr {
	t.Helper()
	expression := requireExpression(t, parsed)
	function, ok := expression.(*sqlparser.FuncExpr)
	require.Truef(t, ok, "expected function expression, found %T", expression)
	return function
}

// requireColumn returns a parsed column expression.
func requireColumn(t testing.TB, parsed sqlparser.SelectExpr) *sqlparser.ColName {
	t.Helper()
	expression := requireExpression(t, parsed)
	column, ok := expression.(*sqlparser.ColName)
	require.Truef(t, ok, "expected column expression, found %T", expression)
	return column
}

// requireFunctionArgument returns a parsed function argument.
func requireFunctionArgument(t testing.TB, function *sqlparser.FuncExpr, index int) sqlparser.Expr {
	t.Helper()
	require.Greater(t, len(function.Exprs), index)
	argument, ok := function.Exprs[index].(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased function argument, found %T", function.Exprs[index])
	return argument.Expr
}

// assertFunctionRoundTrip compares the semantic fields parsed from original.String() to the fields of original.
func assertFunctionRoundTrip(t testing.TB, original sql.FunctionExpression) {
	t.Helper()
	assertFunctionRoundTripAs(t, original, original.FunctionName())
}

// assertFunctionRoundTripAs is assertFunctionRoundTrip for expressions whose parser-visible name differs from their
// registry name.
func assertFunctionRoundTripAs(t testing.TB, original sql.FunctionExpression, parsedName string) {
	t.Helper()
	parsed := requireFunction(t, parseExpression(t, original))
	require.False(t, parsed.Distinct)
	assertFunction(t, parsed, original, parsedName)
}

// AssertDistinctFunctionRoundTrip additionally verifies the DISTINCT modifier on a function expression.
func assertDistinctFunctionRoundTripAs(t testing.TB, original sql.FunctionExpression, parsedName string) {
	t.Helper()
	parsed := requireFunction(t, parseExpression(t, original))
	require.True(t, parsed.Distinct)
	assertFunction(t, parsed, original, parsedName)
}

// assertColumnRoundTrip compares a parsed column name and qualifier to the original expression.
func assertColumnRoundTrip(t testing.TB, original sql.Expression) {
	t.Helper()
	assertExpressionValue(t, requireExpression(t, parseExpression(t, original)), original)
}

// assertAliasRoundTrip compares both the parsed alias and its value to the original expression.
func assertAliasRoundTrip(t testing.TB, original interface {
	sql.Expression
	sql.Nameable
}) {
	t.Helper()
	parsed, ok := parseExpression(t, original).(*sqlparser.AliasedExpr)
	require.Truef(t, ok, "expected aliased expression, found %T", parsed)
	require.Equal(t, original.Name(), parsed.As.String())
	require.Len(t, original.Children(), 1)
	assertExpressionValue(t, parsed.Expr, original.Children()[0])
}

// assertLiteralRoundTrip compares the parsed literal token and value to the original literal value.
func assertLiteralRoundTrip(t testing.TB, original sql.Expression) {
	t.Helper()
	parsed := requireExpression(t, parseExpression(t, original))
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

// assertBindVariableRoundTrip compares the parser bind-variable token to the original bind variable name.
func assertBindVariableRoundTrip(t testing.TB, original fmt.Stringer, name string) {
	t.Helper()
	parsed := requireSQLValue(t, requireExpression(t, parseExpression(t, original)), sqlparser.ValArg)
	require.Equal(t, ":"+name, string(parsed.Val))
}

// assertStarRoundTrip compares a parsed star qualifier to the original table qualifier.
func assertStarRoundTrip(t testing.TB, original sql.Expression, table string) {
	t.Helper()
	parsed, ok := parseExpression(t, original).(*sqlparser.StarExpr)
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
		assertExpressionValue(t, requireFunctionArgument(t, parsed, i), child)
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
		assertExpressionValue(t, parsed.PartitionBy[i], partition)
	}
	require.Len(t, parsed.OrderBy, len(original.OrderBy))
	for i, order := range original.OrderBy {
		assertExpressionValue(t, parsed.OrderBy[i].Expr, order.Expr)
		require.Equal(t, strings.ToLower(order.Order.String()), parsed.OrderBy[i].Direction)
	}
	// WindowFrame does not expose whether it represents ROWS or RANGE. Framed-window tests must inspect Frame directly.
	require.Nil(t, original.Frame)
	require.Nil(t, parsed.Frame)
}

// assertExpressionValue compares a parsed AST expression to the corresponding original GMS expression.
func assertExpressionValue(t testing.TB, parsed sqlparser.Expr, original sql.Expression) {
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
		binary := requireSQLValue(t, requireFunctionArgument(t, actual, 0), sqlparser.HexNum)
		decoded, err := hex.DecodeString(strings.TrimPrefix(string(binary.Val), "0x"))
		require.NoError(t, err)
		require.Equal(t, value.Serialize()[types.SRIDSize:], decoded)
		srid := requireSQLValue(t, requireFunctionArgument(t, actual, 1), sqlparser.IntVal)
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

func assertParsedInSubquery(t *testing.T, parsed sqlparser.Expr, original *plan.InSubquery) {
	t.Helper()
	parenthesized := parsed.(*sqlparser.ParenExpr)
	comparison := parenthesized.Expr.(*sqlparser.ComparisonExpr)
	require.Equal(t, sqlparser.InStr, comparison.Operator)
	assertExpressionValue(t, comparison.Left, original.Left())
	assertParsedSubquery(t, comparison.Right.(*sqlparser.Subquery), original.Right().(*plan.Subquery))
}

func assertParsedSubquery(t *testing.T, parsed *sqlparser.Subquery, original *plan.Subquery) {
	t.Helper()
	expectedStatement, err := sqlparser.Parse(original.QueryString)
	require.NoError(t, err)
	expected := expectedStatement.(*sqlparser.Select)
	actual := parsed.Select.(*sqlparser.Select)
	require.Len(t, actual.SelectExprs, len(expected.SelectExprs))
	for i := range expected.SelectExprs {
		expectedExpr := expected.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
		actualExpr := actual.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
		require.Equal(t, reflect.TypeOf(expectedExpr), reflect.TypeOf(actualExpr))
		switch expectedExpr := expectedExpr.(type) {
		case *sqlparser.SQLVal:
			actualValue := actualExpr.(*sqlparser.SQLVal)
			require.Equal(t, expectedExpr.Type, actualValue.Type)
			require.Equal(t, expectedExpr.Val, actualValue.Val)
		case *sqlparser.ColName:
			actualColumn := actualExpr.(*sqlparser.ColName)
			require.Equal(t, expectedExpr.Name.String(), actualColumn.Name.String())
			require.Equal(t, expectedExpr.Qualifier.Name.String(), actualColumn.Qualifier.Name.String())
		default:
			require.Failf(t, "unsupported subquery select expression", "%T", expectedExpr)
		}
	}
	require.Len(t, actual.From, len(expected.From))
	for i := range expected.From {
		expectedTable := expected.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
		actualTable := actual.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
		require.Equal(t, expectedTable.Name.String(), actualTable.Name.String())
		require.Equal(t, expectedTable.DbQualifier.String(), actualTable.DbQualifier.String())
	}
}
