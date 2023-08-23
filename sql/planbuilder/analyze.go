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

package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	names := make([]sql.DbTable, len(n.Tables))
	defaultDb := b.ctx.GetCurrentDatabase()
	for i, table := range n.Tables {
		dbName := table.Qualifier.String()
		if dbName == "" {
			if defaultDb == "" {
				err := sql.ErrNoDatabaseSelected.New()
				b.handleErr(err)
			}
			dbName = defaultDb
		}
		names[i] = sql.DbTable{Db: dbName, Table: strings.ToLower(table.Name.String())}
	}
	analyze := plan.NewAnalyze(names)

	stats, err := b.cat.Statistics(b.ctx)
	if err != nil {
		b.handleErr(err)
	}

	outScope.node = analyze.WithDb(defaultDb).WithStats(stats)
	return
}
