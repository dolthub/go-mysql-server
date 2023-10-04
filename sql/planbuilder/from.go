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
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TODO outScope will be populated with a source node and column sets
func (b *Builder) buildFrom(inScope *scope, te ast.TableExprs) (outScope *scope) {
	if len(te) == 0 {
		outScope = inScope.push()
		outScope.ast = te
		outScope.node = plan.NewResolvedDualTable()
		return
	}

	if len(te) > 1 {
		cj := &ast.JoinTableExpr{
			LeftExpr:  te[0],
			RightExpr: te[1],
			Join:      ast.JoinStr,
			Condition: ast.JoinCondition{On: ast.BoolVal(true)},
		}
		for _, t := range te[2:] {
			cj = &ast.JoinTableExpr{
				LeftExpr:  cj,
				RightExpr: t,
				Join:      ast.JoinStr,
				Condition: ast.JoinCondition{On: ast.BoolVal(true)},
			}
		}
		return b.buildJoin(inScope, cj)
	}
	return b.buildDataSource(inScope, te[0])
}

func (b *Builder) validateJoinTableNames(leftScope, rightScope *scope) {
	for t, _ := range leftScope.tables {
		if _, ok := rightScope.tables[t]; ok {
			err := sql.ErrDuplicateAliasOrTable.New(t)
			b.handleErr(err)
		}
	}
}

func (b *Builder) isLateral(te ast.TableExpr) bool {
	switch t := te.(type) {
	case *ast.JSONTableExpr:
		return true
	case *ast.AliasedTableExpr:
		return t.Lateral
	default:
		return false
	}
}

func (b *Builder) isUsingJoin(te *ast.JoinTableExpr) bool {
	return te.Condition.Using != nil ||
		strings.EqualFold(te.Join, ast.NaturalJoinStr) ||
		strings.EqualFold(te.Join, ast.NaturalLeftJoinStr) ||
		strings.EqualFold(te.Join, ast.NaturalRightJoinStr)
}

func (b *Builder) buildJoin(inScope *scope, te *ast.JoinTableExpr) (outScope *scope) {
	//TODO build individual table expressions
	// collect column  definitions
	leftScope := b.buildDataSource(inScope, te.LeftExpr)

	// TODO lateral join right will see left outputs
	rightInScope := inScope
	if b.isLateral(te.RightExpr) && te.Join != ast.RightJoinStr {
		rightInScope = leftScope
	}
	rightScope := b.buildDataSource(rightInScope, te.RightExpr)

	b.validateJoinTableNames(leftScope, rightScope)

	if b.isUsingJoin(te) {
		return b.buildUsingJoin(inScope, leftScope, rightScope, te)
	}

	outScope = inScope.push()
	outScope.appendColumnsFromScope(leftScope)
	outScope.appendColumnsFromScope(rightScope)

	// cross join
	if (te.Condition.On == nil || te.Condition.On == ast.BoolVal(true)) && te.Condition.Using == nil {
		if rast, ok := te.RightExpr.(*ast.AliasedTableExpr); ok && rast.Lateral {
			var err error
			outScope.node, err = b.f.buildJoin(leftScope.node, rightScope.node, plan.JoinTypeLateralCross, expression.NewLiteral(true, types.Boolean))
			if err != nil {
				b.handleErr(err)
			}
		} else if b.isLateral(te.RightExpr) {
			outScope.node = plan.NewJoin(leftScope.node, rightScope.node, plan.JoinTypeLateralCross, nil)
		} else {
			outScope.node = plan.NewCrossJoin(leftScope.node, rightScope.node)
		}
		return
	}

	var filter sql.Expression
	if te.Condition.On != nil {
		filter = b.buildScalar(outScope, te.Condition.On)
	}

	var op plan.JoinType
	switch strings.ToLower(te.Join) {
	case ast.JoinStr:
		if b.isLateral(te.RightExpr) {
			op = plan.JoinTypeLateralInner
		} else {
			op = plan.JoinTypeInner
		}
	case ast.LeftJoinStr:
		if b.isLateral(te.RightExpr) {
			op = plan.JoinTypeLateralLeft
		} else {
			op = plan.JoinTypeLeftOuter
		}
	case ast.RightJoinStr:
		if b.isLateral(te.RightExpr) {
			op = plan.JoinTypeLateralRight
		} else {
			op = plan.JoinTypeRightOuter
		}
	case ast.FullOuterJoinStr:
		op = plan.JoinTypeFullOuter
	default:
		b.handleErr(fmt.Errorf("unknown join type: %s", te.Join))
	}
	var err error
	outScope.node, err = b.f.buildJoin(leftScope.node, rightScope.node, op, filter)
	if err != nil {
		b.handleErr(err)
	}

	return outScope
}

// buildUsingJoin converts a JOIN with a USING clause into an INNER JOIN, LEFT JOIN, or RIGHT JOIN; NATURAL JOINs are a
// subset of USING joins.
// The scope of these join must contain all the qualified columns from both left and right tables. The columns listed
// in the USING clause must be in both left and right tables, and will be redirected to
// either the left or right table.
// An equality filter is created with columns in the USING list. Columns in the USING
// list are de-duplicated and listed first (in the order they appear in the left table), followed by the remaining
// columns from the left table, followed by the remaining columns from the right table.
// NATURAL_JOIN(t1, t2)       => PROJ(t1.a1, ...,t1.aN) -> INNER_JOIN(t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
// NATURAL_LEFT_JOIN(t1, t2)  => PROJ(t1.a1, ...,t1.aN) -> LEFT_JOIN (t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
// NATURAL_RIGHT_JOIN(t1, t2) => PROJ(t1.a1, ...,t1.aN) -> RIGHT_JOIN(t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
// USING_JOIN(t1, t2)         => PROJ(t1.a1, ...,t1.aN) -> INNER_JOIN(t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
// USING_LEFT_JOIN(t1, t2)    => PROJ(t1.a1, ...,t1.aN) -> LEFT_JOIN (t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
// USING_RIGHT_JOIN(t1, t2)   => PROJ(t1.a1, ...,t1.aN) -> RIGHT_JOIN(t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
func (b *Builder) buildUsingJoin(inScope, leftScope, rightScope *scope, te *ast.JoinTableExpr) (outScope *scope) {
	outScope = inScope.push()

	// Fill in USING columns for NATURAL JOINs
	if len(te.Condition.Using) == 0 {
		for _, lCol := range leftScope.cols {
			for _, rCol := range rightScope.cols {
				if strings.EqualFold(lCol.col, rCol.col) {
					te.Condition.Using = append(te.Condition.Using, ast.NewColIdent(lCol.col))
					break
				}
			}
		}
	}

	// Right joins swap left and right scopes.
	var left, right *scope
	if te.Join == ast.RightJoinStr || te.Join == ast.NaturalRightJoinStr {
		left, right = rightScope, leftScope
	} else {
		left, right = leftScope, rightScope
	}

	// Add columns in common
	var filter sql.Expression
	usingCols := map[string]struct{}{}
	for _, col := range te.Condition.Using {
		colName := col.String()
		// Every column in the USING clause must be in both tables.
		lCol, ok := left.resolveColumn("", "", colName, false)
		if !ok {
			b.handleErr(sql.ErrUnknownColumn.New(colName, "from clause"))
		}
		rCol, ok := right.resolveColumn("", "", colName, false)
		if !ok {
			b.handleErr(sql.ErrUnknownColumn.New(colName, "from clause"))
		}
		f := expression.NewEquals(lCol.scalarGf(), rCol.scalarGf())
		if filter == nil {
			filter = f
		} else {
			filter = expression.NewAnd(filter, f)
		}
		usingCols[colName] = struct{}{}
		outScope.redirect(scopeColumn{col: rCol.col}, lCol)
	}

	// Add common columns first, then left, then right.
	// The order of columns for the common section must match left table
	for _, lCol := range left.cols {
		if _, ok := usingCols[lCol.col]; ok {
			outScope.addColumn(lCol)
		}
	}
	for _, rCol := range right.cols {
		if _, ok := usingCols[rCol.col]; ok {
			outScope.addColumn(rCol)
		}
	}
	for _, lCol := range left.cols {
		if _, ok := usingCols[lCol.col]; !ok {
			outScope.addColumn(lCol)
		}
	}
	for _, rCol := range right.cols {
		if _, ok := usingCols[rCol.col]; !ok {
			outScope.addColumn(rCol)
		}
	}

	// joining two tables with no common columns is just cross join
	if len(te.Condition.Using) == 0 {
		if b.isLateral(te.RightExpr) {
			outScope.node = plan.NewJoin(leftScope.node, rightScope.node, plan.JoinTypeLateralCross, nil)
		} else {
			outScope.node = plan.NewCrossJoin(leftScope.node, rightScope.node)
		}
		return outScope
	}

	switch strings.ToLower(te.Join) {
	case ast.JoinStr, ast.NaturalJoinStr:
		outScope.node = plan.NewInnerJoin(leftScope.node, rightScope.node, filter)
	case ast.LeftJoinStr, ast.NaturalLeftJoinStr:
		outScope.node = plan.NewLeftOuterJoin(leftScope.node, rightScope.node, filter)
	case ast.RightJoinStr, ast.NaturalRightJoinStr:
		outScope.node = plan.NewLeftOuterJoin(rightScope.node, leftScope.node, filter)
	default:
		b.handleErr(fmt.Errorf("unknown using join type: %s", te.Join))
	}
	return outScope
}

func (b *Builder) buildDataSource(inScope *scope, te ast.TableExpr) (outScope *scope) {
	outScope = inScope.push()
	outScope.ast = te

	// build individual table, collect column definitions
	switch t := (te).(type) {
	case *ast.AliasedTableExpr:
		switch e := t.Expr.(type) {
		case ast.TableName:
			tableName := strings.ToLower(e.Name.String())
			if cteScope := inScope.getCte(tableName); cteScope != nil {
				outScope = cteScope.copy()
				outScope.parent = inScope
			} else {
				var ok bool
				outScope, ok = b.buildTablescan(inScope, e.Qualifier.String(), tableName, t.AsOf)
				if !ok {
					b.handleErr(sql.ErrTableNotFound.New(tableName))
				}
			}
			if t.As.String() != "" {
				tAlias := strings.ToLower(t.As.String())
				outScope.setTableAlias(tAlias)
				var err error
				outScope.node, err = b.f.buildTableAlias(tAlias, outScope.node)
				if err != nil {
					b.handleErr(err)
				}
			}
		case *ast.Subquery:
			if t.As.IsEmpty() {
				// This should be caught by the parser, but here just in case
				b.handleErr(sql.ErrUnsupportedFeature.New("subquery without alias"))
			}

			sqScope := inScope.pushSubquery()
			fromScope := b.buildSelectStmt(sqScope, e.Select)
			alias := strings.ToLower(t.As.String())
			sq := plan.NewSubqueryAlias(alias, ast.String(e.Select), fromScope.node)
			sq = sq.WithCorrelated(sqScope.correlated())
			sq = sq.WithVolatile(sqScope.volatile())
			sq.IsLateral = t.Lateral

			var renameCols []string
			if len(e.Columns) > 0 {
				renameCols = columnsToStrings(e.Columns)
				sq = sq.WithColumns(renameCols)
			}

			if len(renameCols) > 0 && len(fromScope.cols) != len(renameCols) {
				err := sql.ErrColumnCountMismatch.New()
				b.handleErr(err)
			}

			outScope = inScope.push()
			scopeMapping := make(map[sql.ColumnId]sql.Expression)
			for i, c := range fromScope.cols {
				col := c.col
				if len(renameCols) > 0 {
					col = renameCols[i]
				}
				toId := outScope.newColumn(scopeColumn{
					db:       c.db,
					table:    alias,
					col:      col,
					id:       0,
					typ:      c.typ,
					nullable: c.nullable,
				})
				scopeMapping[sql.ColumnId(toId)] = c.scalarGf()
			}
			outScope.node = sq.WithScopeMapping(scopeMapping)
			return
		case *ast.ValuesStatement:
			if t.As.IsEmpty() {
				// Parser should enforce this, but just to be safe
				b.handleErr(sql.ErrUnsupportedSyntax.New("every derived table must have an alias"))
			}
			exprTuples := make([][]sql.Expression, len(e.Rows))
			for i, vt := range e.Rows {
				exprs := make([]sql.Expression, len(vt))
				exprTuples[i] = exprs
				for j, e := range vt {
					exprs[j] = b.buildScalar(inScope, e)
				}
			}

			outScope = inScope.push()
			vdt := plan.NewValueDerivedTable(plan.NewValues(exprTuples), t.As.String())
			for _, c := range vdt.Schema() {
				outScope.newColumn(scopeColumn{col: c.Name, table: c.Source, typ: c.Type, nullable: c.Nullable})
			}
			var renameCols []string
			if len(e.Columns) > 0 {
				renameCols = columnsToStrings(e.Columns)
				vdt = vdt.WithColumns(renameCols)
			}
			b.renameSource(outScope, t.As.String(), renameCols)
			outScope.node = vdt
			return
		default:
			b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(te)))
		}

	case *ast.TableFuncExpr:
		return b.buildTableFunc(inScope, t)

	case *ast.JoinTableExpr:
		return b.buildJoin(inScope, t)

	case *ast.JSONTableExpr:
		return b.buildJSONTable(inScope, t)

	case *ast.ParenTableExpr:
		if len(t.Exprs) == 1 {
			switch j := t.Exprs[0].(type) {
			case *ast.JoinTableExpr:
				return b.buildJoin(inScope, j)
			default:
				b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(t)))
			}
		} else {
			b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(t)))
		}
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(te)))
	}
	return
}

func columnsToStrings(cols ast.Columns) []string {
	if len(cols) == 0 {
		return nil
	}
	res := make([]string, len(cols))
	for i, c := range cols {
		res[i] = c.String()
	}

	return res
}

func (b *Builder) resolveTable(tab, db string, asOf interface{}) *plan.ResolvedTable {
	var table sql.Table
	var database sql.Database
	var err error
	if asOf != nil {
		table, database, err = b.cat.TableAsOf(b.ctx, db, tab, asOf)
	} else {
		table, database, err = b.cat.Table(b.ctx, db, tab)
	}
	if sql.ErrAsOfNotSupported.Is(err) {
		if asOf != nil {
			b.handleErr(err)
		}
		table, database, err = b.cat.Table(b.ctx, db, tab)
	}
	if err != nil {
		b.handleErr(err)
	}

	if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privilegedDatabase.Unwrap()
	}
	return plan.NewResolvedTable(table, database, asOf)
}

func (b *Builder) buildTableFunc(inScope *scope, t *ast.TableFuncExpr) (outScope *scope) {
	//TODO what are valid mysql table arguments
	args := make([]sql.Expression, 0, len(t.Exprs))
	for _, e := range t.Exprs {
		switch e := e.(type) {
		case *ast.AliasedExpr:
			expr := b.buildScalar(inScope, e.Expr)

			if !e.As.IsEmpty() {
				b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
			}

			if selectExprNeedsAlias(e, expr) {
				b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
			}

			args = append(args, expr)
		default:
			b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
		}
	}

	utf := expression.NewUnresolvedTableFunction(t.Name, args)

	tableFunction, err := b.cat.TableFunction(b.ctx, utf.Name())
	if err != nil {
		b.handleErr(err)
	}

	database := b.currentDb()

	var hasBindVarArgs bool
	for _, arg := range utf.Arguments {
		if _, ok := arg.(*expression.BindVar); ok {
			hasBindVarArgs = true
			break
		}
	}

	outScope = inScope.push()
	outScope.ast = t
	if hasBindVarArgs {
		// TODO deferred tableFunction
	}

	newInstance, err := tableFunction.NewInstance(b.ctx, database, utf.Arguments)
	if err != nil {
		b.handleErr(err)
	}

	if ctf, isCTF := newInstance.(sql.CatalogTableFunction); isCTF {
		newInstance, err = ctf.WithCatalog(b.cat)
		if err != nil {
			b.handleErr(err)
		}
	}

	// Table Function must always have an alias, pick function name as alias if none is provided
	var name string
	var newAlias sql.Node
	if t.Alias.IsEmpty() {
		name = t.Name
		newAlias = plan.NewTableAlias(name, newInstance)
	} else {
		name = t.Alias.String()
		newAlias, err = b.f.buildTableAlias(name, newInstance)
		if err != nil {
			b.handleErr(err)
		}
	}

	outScope.node = newAlias
	for _, c := range newAlias.Schema() {
		outScope.newColumn(scopeColumn{
			db:    database.Name(),
			table: name,
			col:   c.Name,
			typ:   c.Type,
		})
	}
	return
}

func (b *Builder) buildJSONTableCols(inScope *scope, jtSpec *ast.JSONTableSpec) []plan.JSONTableCol {
	var cols []plan.JSONTableCol
	for _, jtColDef := range jtSpec.Columns {
		// nested col defs need to be flattened into multiple colOpts with all paths appended
		if jtColDef.Spec != nil {
			nestedCols := b.buildJSONTableCols(inScope, jtColDef.Spec)
			col := plan.JSONTableCol{
				Path:       jtColDef.Spec.Path,
				NestedCols: nestedCols,
			}
			cols = append(cols, col)
			continue
		}

		typ, err := types.ColumnTypeToType(&jtColDef.Type)
		if err != nil {
			b.handleErr(err)
		}

		var defEmptyVal, defErrorVal sql.Expression
		if jtColDef.Opts.ValOnEmpty == nil {
			defEmptyVal = expression.NewLiteral(nil, types.Null)
		} else {
			defEmptyVal = b.buildScalar(inScope, jtColDef.Opts.ValOnEmpty)
		}

		if jtColDef.Opts.ValOnError == nil {
			defErrorVal = expression.NewLiteral(nil, types.Null)
		} else {
			defErrorVal = b.buildScalar(inScope, jtColDef.Opts.ValOnError)
		}

		col := plan.JSONTableCol{
			Path: jtColDef.Opts.Path,
			Opts: &plan.JSONTableColOpts{
				Name:         jtColDef.Name.String(),
				Type:         typ,
				ForOrd:       bool(jtColDef.Type.Autoincrement),
				Exists:       jtColDef.Opts.Exists,
				DefEmptyVal:  defEmptyVal,
				DefErrorVal:  defErrorVal,
				ErrorOnEmpty: jtColDef.Opts.ErrorOnEmpty,
				ErrorOnError: jtColDef.Opts.ErrorOnError,
			},
		}
		cols = append(cols, col)
	}
	return cols
}

func (b *Builder) buildJSONTable(inScope *scope, t *ast.JSONTableExpr) (outScope *scope) {
	data := b.buildScalar(inScope, t.Data)
	if _, ok := data.(*plan.Subquery); ok {
		b.handleErr(sql.ErrInvalidArgument.New("JSON_TABLE"))
	}

	outScope = inScope.push()
	outScope.ast = t

	alias := t.Alias.String()
	cols := b.buildJSONTableCols(inScope, t.Spec)
	var recFlatten func(col plan.JSONTableCol)
	recFlatten = func(col plan.JSONTableCol) {
		for _, col := range col.NestedCols {
			recFlatten(col)
		}
		if col.Opts != nil {
			outScope.newColumn(scopeColumn{
				table: strings.ToLower(alias),
				col:   col.Opts.Name,
				typ:   col.Opts.Type,
			})
		}
	}
	for _, col := range cols {
		recFlatten(col)
	}

	var err error
	if outScope.node, err = plan.NewJSONTable(data, t.Spec.Path, alias, cols); err != nil {
		b.handleErr(err)
	}
	return outScope
}

func (b *Builder) buildTablescan(inScope *scope, db, name string, asof *ast.AsOf) (outScope *scope, ok bool) {
	outScope = inScope.push()

	if b.ViewCtx().DbName != "" {
		db = b.ViewCtx().DbName
	} else if db == "" {
		db = b.ctx.GetCurrentDatabase()
	}

	var asOfLit interface{}
	if asof != nil {
		asOfLit = b.buildAsOfLit(inScope, asof.Time)
	} else if asof := b.ViewCtx().AsOf; asof != nil {
		asOfLit = asof
	} else if asof := b.ProcCtx().AsOf; asof != nil {
		asOfLit = asof
	}

	var tab sql.Table
	var database sql.Database
	var err error
	database, err = b.cat.Database(b.ctx, db)
	if err != nil {
		b.handleErr(err)
	}

	if view := b.resolveView(name, database, asOfLit); view != nil {
		outScope.node = view
		for _, c := range view.Schema() {
			outScope.newColumn(scopeColumn{
				db:          strings.ToLower(db),
				table:       strings.ToLower(name),
				col:         strings.ToLower(c.Name),
				originalCol: c.Name,
				typ:         c.Type,
				nullable:    c.Nullable,
			})
		}
		return outScope, true
	}

	if asOfLit != nil {
		tab, database, err = b.cat.TableAsOf(b.ctx, db, name, asOfLit)
	} else {
		tab, _, err = database.GetTableInsensitive(b.ctx, name)
	}
	if err != nil {
		if sql.ErrDatabaseNotFound.Is(err) {
			if db == "" {
				err = sql.ErrNoDatabaseSelected.New()
			}
		}
		b.handleErr(err)
	} else if tab == nil {
		if b.TriggerCtx().Active && !b.TriggerCtx().Call {
			outScope.node = plan.NewUnresolvedTable(name, db)
			b.TriggerCtx().UnresolvedTables = append(b.TriggerCtx().UnresolvedTables, name)
			return outScope, true
		}
		return outScope, false
	}

	rt := plan.NewResolvedTable(tab, database, asOfLit)
	ct, ok := rt.Table.(sql.CatalogTable)
	if ok {
		rt.Table = ct.AssignCatalog(b.cat)
	}
	outScope.node = rt

	for _, c := range tab.Schema() {
		outScope.newColumn(scopeColumn{
			db:          strings.ToLower(db),
			table:       strings.ToLower(tab.Name()),
			col:         strings.ToLower(c.Name),
			originalCol: c.Name,
			typ:         c.Type,
			nullable:    c.Nullable,
		})
	}

	if dt, _ := rt.Table.(sql.DynamicColumnsTable); dt != nil {
		// the columns table has to resolve all columns in every table
		sch, err := dt.AllColumns(b.ctx)
		if err != nil {
			b.handleErr(err)
		}

		var newSch sql.Schema
		startSource := sch[0].Source
		tmpScope := inScope.push()
		for i, c := range sch {
			// bucket schema fragments into colsets for resolving defaults
			newCol := scopeColumn{
				db:          strings.ToLower(db),
				table:       strings.ToLower(c.Source),
				col:         strings.ToLower(c.Name),
				originalCol: c.Name,
				typ:         c.Type,
				nullable:    c.Nullable,
			}
			if !strings.EqualFold(c.Source, startSource) {
				startSource = c.Source
				tmpSch := b.resolveSchemaDefaults(tmpScope, sch[i-len(tmpScope.cols):i])
				newSch = append(newSch, tmpSch...)
				tmpScope = inScope.push()
			}
			tmpScope.newColumn(newCol)
		}
		if len(tmpScope.cols) > 0 {
			tmpSch := b.resolveSchemaDefaults(tmpScope, sch[len(sch)-len(tmpScope.cols):len(sch)])
			newSch = append(newSch, tmpSch...)
		}
		rt.Table, err = dt.WithDefaultsSchema(newSch)
		if err != nil {
			b.handleErr(err)
		}
	}

	return outScope, true
}

func (b *Builder) resolveView(name string, database sql.Database, asOf interface{}) sql.Node {
	var view *sql.View

	if vdb, vok := database.(sql.ViewDatabase); vok {
		viewDef, vdok, err := vdb.GetViewDefinition(b.ctx, name)
		if err != nil {
			b.handleErr(err)
		}
		oldOpts := b.parserOpts
		defer func() {
			b.parserOpts = oldOpts
		}()
		if vdok {
			outerAsOf := b.ViewCtx().AsOf
			outerDb := b.ViewCtx().DbName
			b.ViewCtx().AsOf = asOf
			b.ViewCtx().DbName = database.Name()
			defer func() {
				b.ViewCtx().AsOf = outerAsOf
				b.ViewCtx().DbName = outerDb
			}()
			b.parserOpts = sql.NewSqlModeFromString(viewDef.SqlMode).ParserOptions()
			node, _, _, err := b.Parse(viewDef.TextDefinition, false)
			if err != nil {
				// TODO: Need to account for non-existing functions or
				//  users without appropriate privilege to the referenced table/column/function.
				if sql.ErrTableNotFound.Is(err) || sql.ErrColumnNotFound.Is(err) {
					// TODO: ALTER VIEW should not return this error
					err = sql.ErrInvalidRefInView.New(database.Name(), name)
				}
				b.handleErr(err)
			}
			view = plan.NewSubqueryAlias(name, viewDef.TextDefinition, node).AsView(viewDef.CreateViewStatement)

		}
	}
	// If we didn't find the view from the database directly, use the in-session registry
	if view == nil {
		view, _ = b.ctx.GetViewRegistry().View(database.Name(), name)
		if view != nil {
			def, _, _ := transform.NodeWithOpaque(view.Definition(), func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
				// TODO this is a hack because the test registry setup is busted, these should always be resolved
				if urt, ok := n.(*plan.UnresolvedTable); ok {
					return b.resolveTable(urt.Name(), urt.Database().Name(), urt.AsOf()), transform.NewTree, nil
				}
				return n, transform.SameTree, nil
			})
			view = view.WithDefinition(def)
		}
	}

	if view == nil {
		return nil
	}

	query := view.Definition().Children()[0]
	n, err := view.Definition().WithChildren(query)
	if err != nil {
		b.handleErr(err)
	}
	return n
}
