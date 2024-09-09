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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/expression"
"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildLoad(inScope *scope, d *ast.Load) (outScope *scope) {
	dbName := strings.ToLower(d.Table.DbQualifier.String())
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}

	destScope, ok := b.buildResolvedTableForTablename(inScope, d.Table, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(d.Table.Name.String()))
	}
	var db sql.Database
	var rt *plan.ResolvedTable
	switch n := destScope.node.(type) {
	case *plan.ResolvedTable:
		rt = n
		db = rt.Database()
	case *plan.UnresolvedTable:
		db = n.Database()
	default:
		b.handleErr(fmt.Errorf("expected insert destination to be resolved or unresolved table"))
	}
	if rt == nil {
		if b.TriggerCtx().Active && !b.TriggerCtx().Call {
			b.TriggerCtx().UnresolvedTables = append(b.TriggerCtx().UnresolvedTables, d.Table.Name.String())
		} else {
			err := fmt.Errorf("expected resolved table: %s", d.Table.Name.String())
			b.handleErr(err)
		}
	}

	var ignoreNumVal int64 = 0
	if d.IgnoreNum != nil {
		ignoreNumVal = b.getInt64Value(inScope, d.IgnoreNum, "Cannot parse ignore Value")
	}

	dest := destScope.node
	sch := dest.Schema()
	if rt != nil {
		sch = b.resolveSchemaDefaults(destScope, rt.Schema())
	}

	ld := plan.NewLoadData(bool(d.Local), d.Infile, sch, columnsToStrings(d.Columns), ignoreNumVal, d.IgnoreOrReplace)
	if d.Charset != "" {
		// TODO: deal with charset; ignore for now
		ld.Charset = d.Charset
	}

	if d.Fields != nil {
		if d.Fields.TerminatedBy != nil && len(d.Fields.TerminatedBy.Val) != 0 {
			ld.FieldsTerminatedBy = string(d.Fields.TerminatedBy.Val)
		}

		if d.Fields.EnclosedBy != nil {
			ld.FieldsEnclosedBy = string(d.Fields.EnclosedBy.Delim.Val)
			if len(ld.FieldsEnclosedBy) > 1 {
				b.handleErr(sql.ErrUnexpectedSeparator.New())
			}
			if d.Fields.EnclosedBy.Optionally {
				ld.FieldsEnclosedByOpt = true
			}
		}

		if d.Fields.EscapedBy != nil {
			ld.FieldsEscapedBy = string(d.Fields.EscapedBy.Val)
			if len(ld.FieldsEscapedBy) > 1 {
				b.handleErr(sql.ErrUnexpectedSeparator.New())
			}
		}
	}

	if d.Lines != nil {
		if d.Lines.StartingBy != nil {
			ld.LinesStartingBy = string(d.Lines.StartingBy.Val)
		}
		if d.Lines.TerminatedBy != nil {
			ld.LinesTerminatedBy = string(d.Lines.TerminatedBy.Val)
		}
	}

	if d.SetExprs != nil {
		ld.SetExprs = make([]sql.Expression, len(sch))
		for _, expr := range d.SetExprs {
			col := b.buildScalar(destScope, expr.Name)
			gf, isGf := col.(*expression.GetField)
			if !isGf {
				continue
			}
			colName := gf.Name()
			idx := sch.IndexOfColName(colName)
			if idx == -1 {
				b.handleErr(fmt.Errorf("column not found"))
			}
			ld.SetExprs[idx] = b.buildScalar(destScope, expr.Expr)

			// Add set column name to ld.ColumnNames (if not empty or already present), so it's not trimmed from projection
			if len(ld.ColumnNames) != 0 {
				exists := false
				for _, name := range ld.ColumnNames {
					if strings.EqualFold(name, colName) {
						exists = true
						break
					}
				}
				if !exists {
					ld.ColumnNames = append(ld.ColumnNames, colName)
				}
			}
		}
	}

	outScope = inScope.push()
	ins := plan.NewInsertInto(db, plan.NewInsertDestination(sch, dest), ld, ld.IsReplace, ld.ColumnNames, nil, ld.IsIgnore)
	b.validateInsert(ins)
	outScope.node = ins
	if rt != nil {
		checks := b.loadChecksFromTable(destScope, rt.Table)
		outScope.node = ins.WithChecks(checks)
	}
	return outScope
}
