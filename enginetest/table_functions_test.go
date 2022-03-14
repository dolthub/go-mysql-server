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

package enginetest_test

import (
	"io"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableFunction = (*SimpleTableFunction)(nil)

func TestTableFunctions(t *testing.T) {
	var tableFunctionScriptTests = []enginetest.ScriptTest{
		{
			Name:        "undefined table function",
			Query:       "SELECT * from does_not_exist('q', 123);",
			ExpectedErr: sql.ErrTableFunctionNotFound,
		},
		{
			Name:     "basic table function",
			Query:    "SELECT * from simple_table_function(123);",
			Expected: []sql.Row{{"foo", 123}},
		},
		{
			Name:     "basic table function",
			Query:    "SELECT * from simple_TABLE_function(123);",
			Expected: []sql.Row{{"foo", 123}},
		},
		{
			Name:     "aggregate function applied to a table function",
			Query:    "SELECT count(*) from simple_TABLE_function(123);",
			Expected: []sql.Row{{1}},
		},
		{
			Name:     "projection of table function",
			Query:    "SELECT one from simple_TABLE_function(123);",
			Expected: []sql.Row{{"foo"}},
		},
		{
			Name:     "nested expressions in table function arguments",
			Query:    "SELECT * from simple_TABLE_function(concat('f', 'o', 'o'));",
			Expected: []sql.Row{{"foo", 123}},
		},
	}

	harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
	db := harness.NewDatabase("mydb")
	databaseProvider := harness.NewDatabaseProvider(db)
	testDatabaseProvider := NewTestProvider(&databaseProvider, SimpleTableFunction{})
	engine := enginetest.NewEngineWithProvider(t, harness, testDatabaseProvider)
	for _, test := range tableFunctionScriptTests {
		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

// SimpleTableFunction an extremely simple implementation of TableFunction for testing.
// When evaluated, returns a single row: {"foo", 123}
type SimpleTableFunction struct {
	returnedResults bool
}

func (s SimpleTableFunction) Resolved() bool {
	return true
}

func (s SimpleTableFunction) String() string {
	return "SimpleTableFunction"
}

func (s SimpleTableFunction) Schema() sql.Schema {
	schema := []*sql.Column{
		&sql.Column{
			Name: "one",
			Type: sql.TinyText,
		},
		&sql.Column{
			Name: "two",
			Type: sql.Int64,
		},
	}

	return schema
}

func (s SimpleTableFunction) Children() []sql.Node {
	return []sql.Node{}
}

func (s SimpleTableFunction) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if s.returnedResults == true {
		return nil, io.EOF
	}

	s.returnedResults = true
	rowIter := &SimpleTableFunctionRowIter{}
	return rowIter, nil
}

func (s SimpleTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

func (s SimpleTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{}
}

func (s SimpleTableFunction) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) Database() sql.Database {
	return nil
}

func (s SimpleTableFunction) WithDatabase(_ sql.Database) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) TableFunctionName() string {
	return "simple_table_function"
}

func (s SimpleTableFunction) Description() string {
	return "SimpleTableFunction"
}

var _ sql.RowIter = (*SimpleTableFunctionRowIter)(nil)

type SimpleTableFunctionRowIter struct {
	returnedResults bool
}

func (itr *SimpleTableFunctionRowIter) Next(_ *sql.Context) (sql.Row, error) {
	if itr.returnedResults {
		return nil, io.EOF
	}

	itr.returnedResults = true
	return sql.Row{"foo", 123}, nil
}

func (itr *SimpleTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}

var _ sql.FunctionProvider = (*TestProvider)(nil)

type TestProvider struct {
	sql.MutableDatabaseProvider
	tableFunctions map[string]sql.TableFunction
}

func NewTestProvider(dbProvider *sql.MutableDatabaseProvider, tf sql.TableFunction) *TestProvider {
	return &TestProvider{
		*dbProvider,
		map[string]sql.TableFunction{strings.ToLower(tf.TableFunctionName()): tf},
	}
}

func (t TestProvider) Function(ctx *sql.Context, name string) (sql.Function, error) {
	return nil, sql.ErrFunctionNotFound.New(name)
}

func (t TestProvider) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, error) {
	if tf, ok := t.tableFunctions[strings.ToLower(name)]; ok {
		return tf, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
}
