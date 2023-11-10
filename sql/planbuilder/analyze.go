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
	"encoding/json"
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/stats"
)

func (b *Builder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	defaultDb := b.ctx.GetCurrentDatabase()

	if n.Action == "" {
		return b.buildAnalyzeTables(inScope, n, query)
	}

	// table and columns
	if len(n.Tables) != 1 {
		err := fmt.Errorf("ANALYZE %s expected 1 table name, found %d", n.Action, len(n.Tables))
		b.handleErr(err)
	}

	dbName := strings.ToLower(n.Tables[0].Qualifier.String())
	if dbName == "" {
		if defaultDb == "" {
			err := sql.ErrNoDatabaseSelected.New()
			b.handleErr(err)
		}
		dbName = defaultDb
	}
	tableName := strings.ToLower(n.Tables[0].Name.String())

	tableScope, ok := b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		err := sql.ErrTableNotFound.New(tableName)
		b.handleErr(err)
	}
	_, ok = tableScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("can only update statistics for base tables, found %s: %s", tableName, tableScope.node)
		b.handleErr(err)
	}

	columns := make([]string, len(n.Columns))
	types := make([]sql.Type, len(n.Columns))
	for i, c := range n.Columns {
		col, ok := tableScope.resolveColumn(dbName, tableName, c.Lowered(), false)
		if !ok {
			err := sql.ErrTableColumnNotFound.New(tableName, c.Lowered())
			b.handleErr(err)
		}
		columns[i] = col.col
		types[i] = col.typ
	}

	switch n.Action {
	case ast.UpdateStr:
		sch := tableScope.node.Schema()
		return b.buildAnalyzeUpdate(inScope, n, dbName, tableName, sch, columns, types)
	case ast.DropStr:
		outScope = inScope.push()
		outScope.node = plan.NewDropHistogram(dbName, tableName, columns).WithProvider(b.cat)
	default:
		err := fmt.Errorf("invalid ANALYZE action: %s, expected UPDATE or DROP", n.Action)
		b.handleErr(err)
	}
	return
}

func (b *Builder) buildAnalyzeTables(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	defaultDb := b.ctx.GetCurrentDatabase()
	tables := make([]sql.Table, len(n.Tables))
	for i, table := range n.Tables {
		dbName := table.Qualifier.String()
		if dbName == "" {
			if defaultDb == "" {
				err := sql.ErrNoDatabaseSelected.New()
				b.handleErr(err)
			}
			dbName = defaultDb
		}
		tableName := strings.ToLower(table.Name.String())
		tableScope, ok := b.buildTablescan(inScope, dbName, tableName, nil)
		if !ok {
			err := sql.ErrTableNotFound.New(tableName)
			b.handleErr(err)
		}
		rt, ok := tableScope.node.(*plan.ResolvedTable)
		if !ok {
			err := fmt.Errorf("can only update statistics for base tables, found %s: %s", tableName, tableScope.node)
			b.handleErr(err)
		}

		tables[i] = rt.Table
	}
	analyze := plan.NewAnalyze(tables)
	outScope.node = analyze.WithDb(defaultDb).WithStats(b.cat)
	return
}

func (b *Builder) buildAnalyzeUpdate(inScope *scope, n *ast.Analyze, dbName, tableName string, sch sql.Schema, columns []string, types []sql.Type) (outScope *scope) {
	outScope = inScope.push()
	statistic := new(stats.Statistic)
	using := b.buildScalar(inScope, n.Using)
	if l, ok := using.(*expression.Literal); ok {
		if typ, ok := l.Type().(sql.StringType); ok {
			val, _, err := typ.Convert(l.Value())
			if err != nil {
				b.handleErr(err)
			}
			if str, ok := val.(string); ok {
				err := json.Unmarshal([]byte(str), statistic)
				if err != nil {
					err = ErrFailedToParseStats.New(err.Error())
					b.handleErr(err)
				}
			}

		}
	}
	if statistic == nil {
		err := fmt.Errorf("no statistics found for update")
		b.handleErr(err)
	}
	indexName := statistic.Qual.Idx
	if indexName == "" {
		indexName = "primary"
	}
	statistic.SetQualifier(sql.NewStatQualifier(dbName, tableName, indexName))
	statistic.SetColumns(columns)
	statistic.SetTypes(types)

	statCols := sql.NewFastIntSet()
	for _, c := range columns {
		i := sch.IndexOfColName(c)
		statCols.Add(i + 1)
	}
	allCols := sql.NewFastIntSet()
	allCols.AddRange(1, len(sch)+1)
	statColset := sql.NewColSetFromIntSet(statCols)
	allColset := sql.NewColSetFromIntSet(allCols)
	// TODO find if underlying index has strict/lax key
	fds := sql.NewTablescanFDs(allColset, nil, nil, allColset)
	updatedStat := statistic.WithColSet(statColset).WithFuncDeps(fds)
	updatedStat = stats.UpdateCounts(updatedStat)

	outScope.node = plan.NewUpdateHistogram(dbName, tableName, indexName, columns, updatedStat).WithProvider(b.cat)
	return outScope
}
