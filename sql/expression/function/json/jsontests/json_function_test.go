// Copyright 2023 Dolthub, Inc.
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

package jsontests

import (
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonInsert(t *testing.T) {
	_, err := json.NewJSONInsert(sql.NewEmptyContext())
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonInsertTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonRemove(t *testing.T) {
	_, err := json.NewJSONRemove(sql.NewEmptyContext())
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonRemoveTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonReplace(t *testing.T) {
	_, err := json.NewJSONRemove(sql.NewEmptyContext())
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonReplaceTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonSet(t *testing.T) {
	ctx := sql.NewEmptyContext()
	_, err := json.NewJSONSet(ctx)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONSet(
		ctx,
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONSet(
		ctx,
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonSetTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonExtract(t *testing.T) {
	_, err := json.NewJSONExtract(sql.NewEmptyContext())
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonExtractTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
			testJSONExtractAsterisk(t, format.prepareFunc)
		})
	}
}

func TestJsonValue(t *testing.T) {
	_, err := json.NewJSONExtract(sql.NewEmptyContext())
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			RunJsonValueTests(t, format.prepareFunc)
		})
	}
}

func TestJsonValueString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	doc := expression.NewGetField(0, types.JSON, "doc", false)
	path := expression.NewLiteral("$.a", types.Text)

	signed, err := json.NewJsonValue(ctx, doc, path, expression.NewLiteral(int64(0), types.Int64))
	require.NoError(t, err)
	require.Equal(t, "json_value(doc, '$.a', 'signed')", signed.String())
	parsedSigned := exprtest.RequireFunction(t, exprtest.ParseExpression(t, signed))
	signedValue := signed.(*json.JsonValue)
	require.Equal(t, signedValue.FunctionName(), parsedSigned.Name.Lowered())
	require.Len(t, parsedSigned.Exprs, 3)
	exprtest.AssertExpressionValue(t, exprtest.RequireFunctionArgument(t, parsedSigned, 0), signedValue.JSON)
	exprtest.AssertExpressionValue(t, exprtest.RequireFunctionArgument(t, parsedSigned, 1), signedValue.Path)
	parsedType := exprtest.RequireFunctionArgument(t, parsedSigned, 2).(*sqlparser.SQLVal)
	require.Equal(t, sqlparser.StrVal, parsedType.Type)
	require.True(t, types.IsSigned(signedValue.Typ))
	require.Equal(t, "signed", string(parsedType.Val))

	defaultType, err := json.NewJsonValue(ctx, doc, path)
	require.NoError(t, err)
	require.Equal(t, "json_value(doc, '$.a')", defaultType.String())
	parsedDefault := exprtest.RequireFunction(t, exprtest.ParseExpression(t, defaultType))
	defaultValue := defaultType.(*json.JsonValue)
	require.Equal(t, defaultValue.FunctionName(), parsedDefault.Name.Lowered())
	require.Len(t, parsedDefault.Exprs, 2)
	exprtest.AssertExpressionValue(t, exprtest.RequireFunctionArgument(t, parsedDefault, 0), defaultValue.JSON)
	exprtest.AssertExpressionValue(t, exprtest.RequireFunctionArgument(t, parsedDefault, 1), defaultValue.Path)
}

func TestJsonContainsPath(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// Verify arg count 3 or more.
	_, err := json.NewJSONContainsPath(ctx)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONContainsPath(
		ctx,
		expression.NewGetField(0, types.JSON, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONContainsPath(
		ctx,
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonContainsPathTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}
