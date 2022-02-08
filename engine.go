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

package sqle

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// IsReadOnly sets the engine to disallow modification queries.
	IsReadOnly bool
	// IncludeRootAccount adds the root account (with no password) to the list of accounts, and also enables
	// authentication.
	IncludeRootAccount bool
	// TemporaryUsers adds any users that should be included when the engine is created. By default, authentication is
	// disabled, and including any users here will enable authentication. All users in this list will have full access.
	// This field is only temporary, and will be removed as development on users and authentication continues.
	TemporaryUsers []TemporaryUser
}

// TemporaryUser is a user that will be added to the engine. This is for temporary use while the remaining features
// are implemented. Replaces the old "auth.New..." functions for adding a user.
type TemporaryUser struct {
	Username string
	Password string
}

// Engine is a SQL engine.
type Engine struct {
	Analyzer          *analyzer.Analyzer
	LS                *sql.LockSubsystem
	ProcessList       sql.ProcessList
	MemoryManager     *sql.MemoryManager
	BackgroundThreads *sql.BackgroundThreads
	IsReadOnly        bool
}

type ColumnWithRawDefault struct {
	SqlColumn *sql.Column
	Default   string
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`. Should call Engine.Close() to finalize
// dependency lifecycles.
func New(a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	var isReadOnly bool
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
		isReadOnly = cfg.IsReadOnly
		if cfg.IncludeRootAccount {
			a.Catalog.GrantTables.AddRootAccount()
		}
		for _, tempUser := range cfg.TemporaryUsers {
			a.Catalog.GrantTables.AddSuperUser(tempUser.Username, tempUser.Password)
		}
	}

	ls := sql.NewLockSubsystem()

	a.Catalog.RegisterFunction(
		sql.FunctionN{
			Name: "version",
			Fn:   function.NewVersion(versionPostfix),
		})
	a.Catalog.RegisterFunction(function.GetLockingFuncs(ls)...)

	return &Engine{
		Analyzer:          a,
		MemoryManager:     sql.NewMemoryManager(sql.ProcessMemory),
		ProcessList:       NewProcessList(),
		LS:                ls,
		BackgroundThreads: sql.NewBackgroundThreads(),
		IsReadOnly:        isReadOnly,
	}
}

// NewDefault creates a new default Engine.
func NewDefault(pro sql.DatabaseProvider) *Engine {
	a := analyzer.NewDefault(pro)
	return New(a, nil)
}

// AnalyzeQuery analyzes a query and returns its Schema.
func (e *Engine) AnalyzeQuery(
	ctx *sql.Context,
	query string,
) (sql.Schema, error) {
	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, err
	}

	analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, err
	}

	return analyzed.Schema(), nil
}

// Query executes a query. If parsed is non-nil, it will be used instead of parsing the query from text.
func (e *Engine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return e.QueryWithBindings(ctx, query, nil)
}

// QueryWithBindings executes the query given with the bindings provided
func (e *Engine) QueryWithBindings(
	ctx *sql.Context,
	query string,
	bindings map[string]sql.Expression,
) (sql.Schema, sql.RowIter, error) {
	return e.QueryNodeWithBindings(ctx, query, nil, bindings)
}

// QueryNodeWithBindings executes the query given with the bindings provided. If parsed is non-nil, it will be used
// instead of parsing the query from text.
func (e *Engine) QueryNodeWithBindings(
	ctx *sql.Context,
	query string,
	parsed sql.Node,
	bindings map[string]sql.Expression,
) (sql.Schema, sql.RowIter, error) {
	var (
		analyzed sql.Node
		iter     sql.RowIter
		err      error
	)

	if parsed == nil {
		parsed, err = parse.Parse(ctx, query)
		if err != nil {
			return nil, nil, err
		}
	}

	err = e.readOnlyCheck(parsed)
	if err != nil {
		return nil, nil, err
	}

	if len(bindings) > 0 {
		parsed, err = plan.ApplyBindings(ctx, parsed, bindings)
		if err != nil {
			return nil, nil, err
		}
	}

	analyzed, err = e.Analyzer.Analyze(ctx, parsed, nil)
	if err != nil {
		return nil, nil, err
	}

	iter, err = analyzed.RowIter(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

func (e *Engine) Close() error {
	for _, p := range e.ProcessList.Processes() {
		e.ProcessList.Kill(p.Connection)
	}
	return e.BackgroundThreads.Shutdown()
}

func (e *Engine) WithBackgroundThreads(b *sql.BackgroundThreads) *Engine {
	e.BackgroundThreads = b
	return e
}

// readOnlyCheck checks to see if the query is valid with the modification setting of the engine.
func (e *Engine) readOnlyCheck(node sql.Node) error {
	if plan.IsDDLNode(node) && e.IsReadOnly {
		return sql.ErrNotAuthorized.New()
	}
	switch node.(type) {
	case
		*plan.DeleteFrom, *plan.InsertInto, *plan.Update, *plan.LockTables, *plan.UnlockTables:
		if e.IsReadOnly {
			return sql.ErrNotAuthorized.New()
		}
	}
	return nil
}

// ResolveDefaults takes in a schema, along with each column's default value in a string form, and returns the schema
// with the default values parsed and resolved.
func ResolveDefaults(tableName string, schema []*ColumnWithRawDefault) (sql.Schema, error) {
	// todo: change this function or thread a context
	ctx := sql.NewEmptyContext()
	db := plan.NewDummyResolvedDB("temporary")
	e := NewDefault(memory.NewMemoryDBProvider(db))
	defer e.Close()

	unresolvedSchema := make(sql.Schema, len(schema))
	defaultCount := 0
	for i, col := range schema {
		unresolvedSchema[i] = col.SqlColumn
		if col.Default != "" {
			var err error
			unresolvedSchema[i].Default, err = parse.StringToColumnDefaultValue(ctx, col.Default)
			if err != nil {
				return nil, err
			}
			defaultCount++
		}
	}
	// if all defaults are nil, we can skip the rest of this
	if defaultCount == 0 {
		return unresolvedSchema, nil
	}

	// *plan.CreateTable properly handles resolving default values, so we hijack it
	createTable := plan.NewCreateTable(db, tableName, false, false, &plan.TableSpec{Schema: sql.NewPrimaryKeySchema(unresolvedSchema)})

	analyzed, err := e.Analyzer.Analyze(ctx, createTable, nil)
	if err != nil {
		return nil, err
	}

	analyzedQueryProcess, ok := analyzed.(*plan.QueryProcess)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown analyzed result type `%T`", analyzed)
	}

	analyzedCreateTable, ok := analyzedQueryProcess.Child.(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown query process child type `%T`", analyzedQueryProcess)
	}

	return analyzedCreateTable.CreateSchema.Schema, nil
}
