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
	// TODO validateUniqueTableNames is redundant
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

	if strings.EqualFold(te.Join, ast.NaturalJoinStr) {
		// TODO inline resolve natural join
		return b.buildNaturalJoin(inScope, leftScope, rightScope)
	}

	outScope = inScope.push()
	outScope.appendColumnsFromScope(leftScope)
	outScope.appendColumnsFromScope(rightScope)

	// cross join
	if te.Condition.On == nil || te.Condition.On == ast.BoolVal(true) {
		if rast, ok := te.RightExpr.(*ast.AliasedTableExpr); ok && rast.Lateral {
			outScope.node = plan.NewJoin(leftScope.node, rightScope.node, plan.JoinTypeLateralCross, expression.NewLiteral(true, types.Boolean))
		} else {
			outScope.node = plan.NewCrossJoin(leftScope.node, rightScope.node)
		}
		return
	}

	filter := b.buildScalar(outScope, te.Condition.On)

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
	outScope.node = plan.NewJoin(leftScope.node, rightScope.node, op, filter)

	return outScope
}

// buildNaturalJoin logically converts a NATURAL_JOIN to an INNER_JOIN.
// All column names shared by the two tables are used as equality filters
// in the join. The intersection of all unique columns is projected.
// Common table attributes are rewritten to reference the left definitions
// //.
// NATURAL_JOIN(t1, t2)
// =>
// PROJ(t1.a1, ...,t1.aN) -> INNER_JOIN(t1, t2, [t1.a1=t2.a1,..., t1.aN=t2.aN])
func (b *Builder) buildNaturalJoin(inScope, leftScope, rightScope *scope) (outScope *scope) {
	outScope = inScope.push()
	var proj []sql.Expression
	for _, lCol := range leftScope.cols {
		outScope.addColumn(lCol)
		proj = append(proj, lCol.scalarGf())
	}

	var filter sql.Expression
	for _, rCol := range rightScope.cols {
		var matched scopeColumn
		for _, lCol := range leftScope.cols {
			if lCol.col == rCol.col {
				matched = lCol
				break
			}
		}
		if !matched.empty() {
			outScope.redirect(rCol, matched)
		} else {
			proj = append(proj, rCol.scalarGf())
			outScope.addColumn(rCol)
			continue
		}

		f := expression.NewEquals(matched.scalarGf(), rCol.scalarGf())
		if filter == nil {
			filter = f
		} else {
			filter = expression.NewAnd(filter, f)
		}
	}
	if filter == nil {
		outScope.node = plan.NewCrossJoin(leftScope.node, rightScope.node)
		return
	}

	jn := plan.NewJoin(leftScope.node, rightScope.node, plan.JoinTypeInner, filter)
	outScope.node = jn
	return
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
				outScope.node = plan.NewTableAlias(tAlias, outScope.node)
			}
		case *ast.Subquery:
			if t.As.IsEmpty() {
				// This should be caught by the parser, but here just in case
				b.handleErr(sql.ErrUnsupportedFeature.New("subquery without alias"))
			}

			sqScope := inScope.push()
			fromScope := b.buildSelectStmt(sqScope, e.Select)
			alias := strings.ToLower(t.As.String())
			sq := plan.NewSubqueryAlias(alias, ast.String(e.Select), fromScope.node)
			sq.IsLateral = t.Lateral

			var renameCols []string
			if len(e.Columns) > 0 {
				renameCols = columnsToStrings(e.Columns)
				sq = sq.WithColumns(renameCols)
			}

			outScope = inScope.push()
			outScope.node = sq

			if len(renameCols) > 0 && len(fromScope.cols) != len(renameCols) {
				err := sql.ErrColumnCountMismatch.New()
				b.handleErr(err)
			}
			for i, c := range fromScope.cols {
				col := c.col
				if len(renameCols) > 0 {
					col = renameCols[i]
				}
				outScope.newColumn(scopeColumn{
					db:       c.db,
					table:    alias,
					col:      col,
					id:       0,
					typ:      c.typ,
					nullable: c.nullable,
				})
			}
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
	table, _, err := b.cat.TableAsOf(b.ctx, db, tab, asOf)
	if err != nil {
		b.handleErr(err)
	}
	database, err := b.cat.Database(b.ctx, b.ctx.GetCurrentDatabase())
	if err != nil {
		b.handleErr(err)
	}

	if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privilegedDatabase.Unwrap()
	}
	return plan.NewResolvedTable(table, database, asOf)
}

func (b *Builder) buildUnion(inScope *scope, u *ast.Union) (outScope *scope) {
	leftScope := b.buildSelectStmt(inScope, u.Left)
	rightScope := b.buildSelectStmt(inScope, u.Right)

	distinct := u.Type != ast.UnionAllStr
	limit := b.buildLimit(inScope, u.Limit)
	offset := b.buildOffset(inScope, u.Limit)

	// mysql errors for order by right projection
	orderByScope := b.analyzeOrderBy(leftScope, leftScope, u.OrderBy)

	var sortFields sql.SortFields
	for _, c := range orderByScope.cols {
		so := sql.Ascending
		if c.descending {
			so = sql.Descending
		}
		scalar := c.scalar
		if scalar == nil {
			scalar = c.scalarGf()
		}
		// Unions pass order bys to the top scope, where the original
		// order by get field may not longer be accessible. Here it is
		// safe to assume the alias has already been computed.
		scalar, _, _ = transform.Expr(scalar, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.Alias:
				return expression.NewGetField(int(c.id), e.Type(), e.Name(), e.IsNullable()), transform.NewTree, nil
			default:
				return e, transform.SameTree, nil
			}
		})
		sf := sql.SortField{
			Column: scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}

	n, ok := leftScope.node.(*plan.Union)
	if ok {
		if len(n.SortFields) > 0 {
			if len(sortFields) > 0 {
				err := sql.ErrConflictingExternalQuery.New()
				b.handleErr(err)
			}
			sortFields = n.SortFields
		}
		if n.Limit != nil {
			if limit != nil {
				err := fmt.Errorf("conflicing external LIMIT")
				b.handleErr(err)
			}
			limit = n.Limit
		}
		if n.Offset != nil {
			if offset != nil {
				err := fmt.Errorf("conflicing external OFFSET")
				b.handleErr(err)
			}
			offset = n.Offset
		}
		leftScope.node = plan.NewUnion(n.Left(), n.Right(), n.Distinct, nil, nil, nil)
	}

	ret := plan.NewUnion(leftScope.node, rightScope.node, distinct, limit, offset, sortFields)
	outScope = leftScope
	outScope.node = ret
	return
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

	// Table Function must always have an alias, pick function name as alias if none is provided
	var newAlias *plan.TableAlias
	if t.Alias.IsEmpty() {
		newAlias = plan.NewTableAlias(t.Name, newInstance)
	} else {
		newAlias = plan.NewTableAlias(t.Alias.String(), newInstance)
	}

	outScope.node = newAlias
	for _, c := range newAlias.Schema() {
		outScope.newColumn(scopeColumn{
			db:    database.Name(),
			table: newAlias.Name(),
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

	// lookup table in catalog
	// Special handling for asOf w/ prepared statement bindvar
	if db == "" {
		db = b.ctx.GetCurrentDatabase()
	}

	var asOfLit interface{}
	if asof != nil {
		asOfLit = b.buildAsOfLit(inScope, asof.Time)
	} else if asof := b.ViewCtx().AsOf; asof != nil {
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
				db:       strings.ToLower(db),
				table:    strings.ToLower(name),
				col:      strings.ToLower(c.Name),
				typ:      c.Type,
				nullable: c.Nullable,
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
			db:       strings.ToLower(db),
			table:    strings.ToLower(tab.Name()),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	if dt, _ := rt.Table.(sql.DynamicColumnsTable); dt != nil {
		// the columns table schema is dynamic
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
				db:       strings.ToLower(db),
				table:    strings.ToLower(c.Source),
				col:      strings.ToLower(c.Name),
				typ:      c.Type,
				nullable: c.Nullable,
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
		if vdok {
			outerAsOf := b.ViewCtx().AsOf
			outerDb := b.currentDatabase
			b.ViewCtx().AsOf = asOf
			b.currentDatabase = database
			defer func() {
				b.ViewCtx().AsOf = outerAsOf
				b.currentDatabase = outerDb
			}()
			node, _, _, err := b.Parse(viewDef.TextDefinition, false)
			if err != nil {
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
