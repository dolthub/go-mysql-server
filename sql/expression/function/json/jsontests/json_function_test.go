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

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonInsert(t *testing.T) {
	_, err := json.NewJSONInsert()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonInsertTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonRemove(t *testing.T) {
	_, err := json.NewJSONRemove()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonRemoveTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonReplace(t *testing.T) {
	_, err := json.NewJSONRemove()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonReplaceTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}

func TestJsonSet(t *testing.T) {
	_, err := json.NewJSONSet()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONSet(
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONSet(
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
	_, err := json.NewJSONExtract()
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
	_, err := json.NewJSONExtract()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			RunJsonValueTests(t, format.prepareFunc)
		})
	}
}

func TestJsonContainsPath(t *testing.T) {
	// Verify arg count 3 or more.
	_, err := json.NewJSONContainsPath()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
	)
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	_, err = json.NewJSONContainsPath(
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
