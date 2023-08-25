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

package enginetest

import (
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

type QueryEngine interface {
	PrepareQuery(
		ctx *sql.Context,
		query string,
	) (sql.Node, error)
	Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error)
	// TODO: get rid of this, should not be exposed to engine tests
	EngineAnalyzer() *analyzer.Analyzer
	// TODO: get rid of this, should not be exposed to engine tests
	EnginePreparedDataCache() *sqle.PreparedDataCache
	QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]*query.BindVariable) (sql.Schema, sql.RowIter, error)
	CloseSession(connID uint32)
	Close() error
}

var _ QueryEngine = (*sqle.Engine)(nil)
