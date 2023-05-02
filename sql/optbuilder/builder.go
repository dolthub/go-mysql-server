package optbuilder

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type scope struct {
	b      *PlanBuilder
	parent *scope
	ast    ast.SQLNode
	node   sql.Node

	cols      []scopeColumn
	extraCols []scopeColumn
	ctes      map[string]*scope
	groupBy   *groupBy
	exprs     map[string]columnId
	colId     columnId
}

func (s *scope) getExpr(name string) (columnId, bool) {
	n := strings.ToLower(name)
	id, ok := s.exprs[n]
	if !ok && s.groupBy != nil {
		id, ok = s.groupBy.outScope.getExpr(n)
	}
	return id, ok
}

func (s *scope) initGroupBy() {
	s.groupBy = &groupBy{outScope: s.replace()}
}

func (s *scope) outerScopeLen() int {
	var cnt int
	sco := s.parent
	for sco != nil {
		cnt += len(sco.cols)
		sco = sco.parent
	}
	return cnt
}

func (s *scope) setTableAlias(t string) {
	for i := range s.cols {
		beforeColStr := s.cols[i].String()
		s.cols[i].table = t
		id, ok := s.getExpr(beforeColStr)
		if !ok {
			err := sql.ErrColumnNotFound.New(beforeColStr)
			s.b.handleErr(err)
		}
		delete(s.exprs, beforeColStr)
		s.exprs[s.cols[i].String()] = id
	}
}

func (s *scope) setColAlias(cols []string) {
	if len(cols) != len(s.cols) {
		s.b.handleErr(fmt.Errorf("invalid column number for column alias"))
	}
	for i := range s.cols {
		s.cols[i].col = cols[i]
	}
}

func (s *scope) push() *scope {
	return &scope{
		parent: s,
	}
}

func (s *scope) replace() *scope {
	if s == nil {
		return &scope{}
	}
	return &scope{
		parent: s.parent,
		colId:  s.colId,
	}
}

func (s *scope) addCte(name string, cteScope *scope) {
	if s.ctes == nil {
		s.ctes = make(map[string]*scope)
	}
	s.ctes[name] = cteScope
}

func (s *scope) getCte(name string) *scope {
	if s == nil || s.ctes == nil {
		return nil
	}
	return s.ctes[strings.ToLower(name)]
}

func (s *scope) addColumn(col scopeColumn) columnId {
	s.colId++
	col.id = s.colId
	s.cols = append(s.cols, col)
	if s.exprs == nil {
		s.exprs = make(map[string]columnId)
	}
	if col.table != "" {
		s.exprs[fmt.Sprintf("%s.%s", strings.ToLower(col.table), strings.ToLower(col.col))] = s.colId
	} else {
		s.exprs[col.col] = s.colId
	}
	return s.colId
}

func (s *scope) addExtraColumn(col scopeColumn) {
	s.extraCols = append(s.extraCols, col)
}

func (s *scope) addColumns(cols []scopeColumn) {
	s.cols = append(s.cols, cols...)
}

func (s *scope) appendColumnsFromScope(src *scope) {
	for _, c := range src.cols {
		s.addColumn(c)
	}
	// these become pass-through columns in the new scope.
	for i := len(src.cols); i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

type columnId uint16

type scopeColumn struct {
	db         string
	table      string
	col        string
	id         columnId
	typ        sql.Type
	scalar     sql.Expression
	nullable   bool
	descending bool
}

func (c scopeColumn) String() string {
	if c.table == "" {
		return c.col
	} else {
		return fmt.Sprintf("%s.%s", c.table, c.col)
	}
}

type PlanBuilder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	currentDatabase sql.Database
}

func (b *PlanBuilder) newScope() *scope {
	return &scope{}
}

func (b *PlanBuilder) buildSelectStmt(inScope *scope, s ast.SelectStatement) (outScope *scope) {
	switch s := s.(type) {
	case *ast.Select:
		if s.With != nil {
			cteScope := b.buildWith(inScope, s.With)
			return b.buildSelect(cteScope, s)
		}
		return b.buildSelect(inScope, s)
	case *ast.ParenSelect:
		return b.buildParenSelect(inScope, s)
	case *ast.Union:
		return b.buildUnion(inScope, s)
	default:
		b.handleErr(fmt.Errorf("unknown select statement %T", s))
	}
	return
}

func (b *PlanBuilder) buildUnion(inScope *scope, s *ast.Union) (outScope *scope) {
	panic("todo")
}

func (b *PlanBuilder) buildParenSelect(inScope *scope, s *ast.ParenSelect) (outScope *scope) {
	panic("todo")
}

func (b *PlanBuilder) buildSelect(inScope *scope, s *ast.Select) (outScope *scope) {
	fromScope := b.buildFrom(inScope, s.From)
	// TODO windows
	b.buildWhere(fromScope, s.Where)
	projScope := fromScope.replace()

	// create SELECT list
	// aggregates in select list added to fromScope.groupBy.outCols
	// args to aggregates added to fromScope.groupBy.inCols
	// select gets ref of agg output
	b.analyzeProjectionList(fromScope, projScope, s.SelectExprs)

	// find aggregations in order by
	orderByScope := b.analyzeOrderBy(fromScope, projScope, s.OrderBy)
	// TODO having, noop for now
	// find aggregations in having
	b.analyzeHaving(fromScope, s.Having)

	// collect:
	// - group by expressions
	// - agg in, out, proj
	// then build
	needsAgg := b.needsAggregation(fromScope, s)
	if needsAgg {
		groupingCols := b.buildGroupingCols(fromScope, projScope, s.GroupBy, s.SelectExprs)
		having := b.buildHaving(fromScope, projScope, s.Having)
		// make Project -> group by
		outScope = b.buildAggregation(fromScope, projScope, having, groupingCols)
	} else {
		outScope = fromScope
	}

	b.buildOrderBy(outScope, orderByScope)
	b.buildProjection(outScope, projScope)
	outScope = projScope
	return
}

func (b *PlanBuilder) analyzeProjectionList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	b.analyzeSelectList(inScope, outScope, selectExprs)
}

func (b *PlanBuilder) analyzeSelectList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	// use inScope to construct projections for projScope
	var exprs []sql.Expression
	//outerLen := inScope.outerScopeLen()
	for _, se := range selectExprs {
		pe := b.selectExprToExpression(inScope, se)
		switch e := pe.(type) {
		case *expression.GetField:
			gf := expression.NewGetFieldWithTable(e.Index(), e.Type(), e.Table(), e.Name(), e.IsNullable())
			exprs = append(exprs, gf)
			id, ok := inScope.getExpr(gf.String())
			if !ok {
				err := sql.ErrColumnNotFound.New(gf.String())
				b.handleErr(err)
			}
			gf = gf.WithIndex(int(id)).(*expression.GetField)
			outScope.addColumn(scopeColumn{table: gf.Table(), col: gf.Name(), scalar: gf, typ: gf.Type(), nullable: gf.IsNullable(), id: id})
		case *expression.Star:
			for _, c := range inScope.cols {
				if c.table == e.Table || e.Table == "" {
					gf := expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
					exprs = append(exprs, gf)
					id, ok := inScope.getExpr(gf.String())
					if !ok {
						err := sql.ErrColumnNotFound.New(gf.String())
						b.handleErr(err)
					}
					outScope.addColumn(scopeColumn{table: c.table, col: c.col, scalar: gf, typ: gf.Type(), nullable: gf.IsNullable(), id: id})
				}
			}
		case *expression.Alias:
			if gf, ok := e.Child.(*expression.GetField); ok {
				id, ok := inScope.getExpr(gf.String())
				if !ok {
					err := sql.ErrColumnNotFound.New(gf.String())
					b.handleErr(err)
				}
				col := scopeColumn{id: id, table: "", col: e.Name(), scalar: e, typ: gf.Type(), nullable: gf.IsNullable()}
				outScope.addColumn(col)
			} else {
				col := scopeColumn{col: pe.String(), scalar: pe, typ: pe.Type(), nullable: pe.IsNullable()}
				outScope.addColumn(col)
			}
			exprs = append(exprs, e)
		default:
			exprs = append(exprs, pe)
			col := scopeColumn{col: pe.String(), scalar: pe, typ: pe.Type()}
			outScope.addColumn(col)
		}
	}
}

func (b *PlanBuilder) buildProjection(inScope, outScope *scope) {
	projections := make([]sql.Expression, len(outScope.cols))
	for i, sc := range outScope.cols {
		projections[i] = sc.scalar
	}
	outScope.node = plan.NewProject(projections, inScope.node)
}

// TODO outScope will be populated with a source node and column sets
func (b *PlanBuilder) buildFrom(inScope *scope, te ast.TableExprs) (outScope *scope) {
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
	return
}

func (b *PlanBuilder) validateJoinTableNames(leftScope, rightScope *scope) {
}

func (b *PlanBuilder) isLateral(te ast.TableExpr) bool {
	_, ok := te.(*ast.JSONTableExpr)
	return ok
}

func (b *PlanBuilder) buildJoin(inScope *scope, te *ast.JoinTableExpr) (outScope *scope) {
	//TODO build individual table expressions
	// collect column  definitions
	leftScope := b.buildDataSource(inScope, te.LeftExpr)

	// TODO lateral join right will see left outputs
	rightInScope := inScope
	if b.isLateral(te.RightExpr) {
		rightInScope = leftScope
	}
	rightScope := b.buildDataSource(rightInScope, te.RightExpr)

	b.validateJoinTableNames(leftScope, rightScope)

	outScope = inScope.push()
	outScope.appendColumnsFromScope(leftScope)
	outScope.appendColumnsFromScope(rightScope)

	if strings.EqualFold(te.Join, ast.NaturalJoinStr) {
		// TODO inline resolve natural join
		outScope.node = plan.NewNaturalJoin(leftScope.node, rightScope.node)
		return
	}

	// cross join
	if te.Condition.On == nil {
		outScope.node = plan.NewCrossJoin(leftScope.node, rightScope.node)
		return
	}

	filter := b.buildScalar(outScope, te.Condition.On)

	var op plan.JoinType
	switch strings.ToLower(te.Join) {
	case ast.JoinStr:
		op = plan.JoinTypeInner
	case ast.LeftJoinStr:
		op = plan.JoinTypeLeftOuter
	case ast.RightJoinStr:
		op = plan.JoinTypeRightOuter
	case ast.FullOuterJoinStr:
		op = plan.JoinTypeFullOuter
	default:
		b.handleErr(fmt.Errorf("unknown join type: %s", te.Join))
	}
	outScope.node = plan.NewJoin(leftScope.node, rightScope.node, op, filter)
	return outScope
}

func (b *PlanBuilder) buildDataSource(inScope *scope, te ast.TableExpr) (outScope *scope) {
	outScope = inScope.push()
	outScope.ast = te

	// build individual table, collect column definitions
	switch t := (te).(type) {
	case *ast.AliasedTableExpr:
		// TODO: Add support for qualifier.
		switch e := t.Expr.(type) {
		case ast.TableName:
			// TODO this can be a CTE
			if cteScope := inScope.getCte(e.Name.String()); cteScope != nil {
				outScope = cteScope
				return
			}
			outScope = b.buildTablescan(inScope, e.Qualifier.String(), e.Name.String(), t.AsOf)
			if t.As.String() != "" {
				outScope.setTableAlias(t.As.String())
				outScope.node = plan.NewTableAlias(t.As.String(), outScope.node)
			}
		case *ast.Subquery:
			if t.As.IsEmpty() {
				// This should be caught by the parser, but here just in case
				b.handleErr(sql.ErrUnsupportedFeature.New("subquery without alias"))
			}

			outScope = b.buildSelectStmt(inScope, e.Select)
			sq := plan.NewSubqueryAlias(t.As.String(), ast.String(e.Select), outScope.node)
			var renameCols []string
			if len(e.Columns) > 0 {
				renameCols = columnsToStrings(e.Columns)
				sq = sq.WithColumns(renameCols)
			}
			b.renameSource(outScope, t.As.String(), renameCols)
			outScope.node = sq
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
		return b.buildJsonTable(inScope, t)

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

func (b *PlanBuilder) buildTableFunc(inScope *scope, t *ast.TableFuncExpr) (outScope *scope) {
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
	outScope.node = newInstance
	for _, c := range newInstance.Schema() {
		outScope.addColumn(scopeColumn{
			db:    database.Name(),
			table: "",
			col:   c.Name,
			typ:   c.Type,
		})
	}
	return
}

func (b *PlanBuilder) currentDb() sql.Database {
	if b.currentDatabase == nil {
		database, err := b.cat.Database(b.ctx, b.ctx.GetCurrentDatabase())
		if err != nil {
			b.handleErr(err)
		}

		if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
			database = privilegedDatabase.Unwrap()
		}
		b.currentDatabase = database
	}
	return b.currentDatabase
}
func (b *PlanBuilder) selectExprToExpression(inScope *scope, se ast.SelectExpr) sql.Expression {
	switch e := se.(type) {
	case *ast.StarExpr:
		if e.TableName.IsEmpty() {
			// TODO all columns from inscope
			return expression.NewStar()
		}
		// TODO lookup table's columns
		return expression.NewQualifiedStar(e.TableName.Name.String())
	case *ast.AliasedExpr:
		expr := b.buildScalar(inScope, e.Expr)

		if !e.As.IsEmpty() {
			return expression.NewAlias(e.As.String(), expr)
		}

		if selectExprNeedsAlias(e, expr) {
			return expression.NewAlias(e.InputExpression, expr)
		}

		return expr
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
	}
	return nil
}

func (b *PlanBuilder) buildJsonTable(inScope *scope, t *ast.JSONTableExpr) (outScope *scope) {
	data := b.buildScalar(inScope, t.Data)
	if _, ok := data.(*plan.Subquery); ok {
		b.handleErr(sql.ErrInvalidArgument.New("JSON_TABLE"))
	}

	paths := make([]string, len(t.Spec.Columns))
	for i, col := range t.Spec.Columns {
		paths[i] = col.Type.Path
	}

	sch, _ := b.tableSpecToSchema(inScope, t.Spec, false)

	outScope = inScope.push()
	outScope.ast = t
	for _, col := range sch.Schema {
		col.Source = strings.ToLower(t.Alias.String())
		outScope.addColumn(scopeColumn{
			db:    "",
			table: col.Source,
			col:   col.Name,
			typ:   col.Type,
		})
	}
	outScope.node = &plan.JSONTable{
		TableName: t.Alias.String(),
		DataExpr:  data,
		Path:      t.Path,
		Sch:       sch,
		ColPaths:  paths,
	}
	return outScope
}

func (b *PlanBuilder) renameSource(scope *scope, table string, cols []string) {
	if table != "" {
		scope.setTableAlias(table)
	}
	if len(cols) > 0 {
		scope.setColAlias(cols)
	}
}

func (b *PlanBuilder) buildTablescan(inScope *scope, db, name string, asof *ast.AsOf) (outScope *scope) {
	outScope = inScope.push()

	// lookup table in catalog
	// Special handling for asOf w/ prepared statement bindvar
	if db == "" {
		db = b.ctx.GetCurrentDatabase()
	}

	var asOfExpr sql.Expression
	var asOfLit interface{}
	var asofBindVar bool
	if asof != nil {
		asOfExpr = b.buildScalar(inScope, asof.Time)
		asofBindVar = transform.InspectExpr(asOfExpr, func(expr sql.Expression) bool {
			_, ok := expr.(*expression.BindVar)
			return ok
		})
		if !asofBindVar {
			//TODO what does this mean?
			// special case for AsOf's that use naked identifiers; they are interpreted as UnresolvedColumns
			if col, ok := asOfExpr.(*expression.UnresolvedColumn); ok {
				asOfExpr = expression.NewLiteral(col.String(), types.LongText)
			}

			var err error
			asOfLit, err = asOfExpr.Eval(b.ctx, nil)
			if err != nil {
				b.handleErr(err)
			}
		}
	}
	tab, database, err := b.cat.Table(b.ctx, db, name)
	if err != nil {
		if sql.ErrDatabaseNotFound.Is(err) {
			if db == "" {
				err = sql.ErrNoDatabaseSelected.New()
			}
		}
		b.handleErr(err)
	} else if tab == nil {
		b.handleErr(sql.ErrTableNotFound.New(name))
	}

	rt := plan.NewResolvedTable(tab, database, asOfLit)
	outScope.node = rt
	if asofBindVar {
		outScope.node = plan.NewDeferredAsOfTable(rt, asOfExpr)
	}

	for _, c := range tab.Schema() {
		outScope.addColumn(scopeColumn{
			db:       strings.ToLower(db),
			table:    strings.ToLower(tab.Name()),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}
	return outScope
}

func (b *PlanBuilder) buildScalar(inScope *scope, e ast.Expr) sql.Expression {
	switch v := e.(type) {
	case *ast.Default:
		return expression.NewDefaultColumn(v.ColName)
	case *ast.SubstrExpr:
		var name sql.Expression
		if v.Name != nil {
			name = b.buildScalar(inScope, v.Name)
		} else {
			name = b.buildScalar(inScope, v.StrVal)
		}
		start := b.buildScalar(inScope, v.From)

		if v.To == nil {
			return &function.Substring{Str: name, Start: start}
		}
		len := b.buildScalar(inScope, v.To)
		return &function.Substring{Str: name, Start: start, Len: len}
	case *ast.CurTimeFuncExpr:
		fsp := b.buildScalar(inScope, v.Fsp)
		return &function.CurrTimestamp{[]sql.Expression{fsp}}
	case *ast.TrimExpr:
		pat := b.buildScalar(inScope, v.Pattern)
		str := b.buildScalar(inScope, v.Str)
		function.NewTrim(str, pat, v.Dir)
	case *ast.ComparisonExpr:
		return b.buildComparison(inScope, v)
	case *ast.IsExpr:
		return b.buildScalar(inScope, v)
	case *ast.NotExpr:
		c := b.buildScalar(inScope, v.Expr)
		return expression.NewNot(c)
	case *ast.SQLVal:
		return b.convertVal(b.ctx, v)
	case ast.BoolVal:
		return expression.NewLiteral(bool(v), types.Boolean)
	case *ast.NullVal:
		return expression.NewLiteral(nil, types.Null)
	case *ast.ColName:
		checkScope := inScope
		for checkScope != nil {
			c, idx := b.resolveColumn(checkScope, v)
			if idx >= 0 {
				return expression.NewGetFieldWithTable(checkScope.outerScopeLen()+idx+1, c.typ, c.table, c.col, c.nullable)
			}
			checkScope = checkScope.parent
		}
		b.handleErr(sql.ErrColumnNotFound.New(v))
	case *ast.FuncExpr:
		name := v.Name.Lowered()
		if isAggregateFunc(name) {
			// TODO this assumes aggregate is in the same scope
			// also need to avoid nested aggregates
			return b.buildAggregateFunc(inScope, name, v)
		} else if isWindowFunc(name) {
			panic("todo window funcs")
		}

		f, err := b.cat.Function(b.ctx, name)
		if err != nil {
			b.handleErr(err)
		}

		args := make([]sql.Expression, len(v.Exprs))
		for i, e := range v.Exprs {
			args[i] = b.selectExprToExpression(inScope, e)
		}

		rf, err := f.NewInstance(args)
		if err != nil {
			b.handleErr(err)
		}

		// NOTE: The count distinct expressions work differently due to the * syntax. eg. COUNT(*)
		if v.Distinct && v.Name.Lowered() == "count" {
			panic("preprocess aggregates into aggInfo")
			return aggregation.NewCountDistinct(args...)
		}

		// NOTE: Not all aggregate functions support DISTINCT. Fortunately, the vitess parser will throw
		// errors for when DISTINCT is used on aggregate functions that don't support DISTINCT.
		if v.Distinct {
			if len(args) != 1 {
				return nil
			}

			args[0] = expression.NewDistinctExpression(args[0])
		}
		if v.Over != nil {
			panic("todo preprocess window functions int windowInfo")
		}

		return rf

	case *ast.GroupConcatExpr:
		// TODO this is an aggregation
		//return b.buildAggregateFunc(inScope, "group_concat", v)
		panic("todo should have been processed into an aggInfo")
	case *ast.ParenExpr:
		return b.buildScalar(inScope, v.Expr)
	case *ast.AndExpr:
		lhs := b.buildScalar(inScope, v.Left)

		rhs := b.buildScalar(inScope, v.Right)

		return expression.NewAnd(lhs, rhs)
	case *ast.OrExpr:
		lhs := b.buildScalar(inScope, v.Left)

		rhs := b.buildScalar(inScope, v.Right)

		return expression.NewOr(lhs, rhs)
	case *ast.XorExpr:
		lhs := b.buildScalar(inScope, v.Left)

		rhs := b.buildScalar(inScope, v.Right)

		return expression.NewXor(lhs, rhs)
	case *ast.ConvertExpr:
		expr := b.buildScalar(inScope, v.Expr)

		return expression.NewConvert(expr, v.Type.Type)
	case *ast.RangeCond:
		val := b.buildScalar(inScope, v.Left)

		lower := b.buildScalar(inScope, v.From)

		upper := b.buildScalar(inScope, v.To)

		switch strings.ToLower(v.Operator) {
		case ast.BetweenStr:
			return expression.NewBetween(val, lower, upper)
		case ast.NotBetweenStr:
			return expression.NewNot(expression.NewBetween(val, lower, upper))
		default:
			return nil
		}
	case ast.ValTuple:
		var exprs = make([]sql.Expression, len(v))
		for i, e := range v {
			expr := b.buildScalar(inScope, e)
			exprs[i] = expr
		}
		return expression.NewTuple(exprs...)

	case *ast.BinaryExpr:
		return b.buildBinaryScalar(inScope, v)
	case *ast.UnaryExpr:
		return b.buildScalar(inScope, v)
	case *ast.Subquery:
		//node, err := convert(ctx, v.Select, "")
		//if err != nil {
		//	return nil
		//}
		selScope := b.buildSelectStmt(inScope, v.Select)

		//b.renameSource(selScope, "", v.Columns)
		// TODO: get the original select statement, not the reconstruction
		selectString := ast.String(v.Select)
		return plan.NewSubquery(selScope.node, selectString)
	case *ast.CaseExpr:
		return b.buildScalar(inScope, v)
	case *ast.IntervalExpr:
		return b.buildScalar(inScope, v)
	case *ast.CollateExpr:
		// handleCollateExpr is meant to handle generic text-returning expressions that should be reinterpreted as a different collation.
		innerExpr := b.buildScalar(inScope, v.Expr)
		//TODO: rename this from Charset to Collation
		collation, err := sql.ParseCollation(nil, &v.Charset, false)
		if err != nil {
			b.handleErr(err)
		}
		// If we're collating a string literal, we check that the charset and collation match now. Other string sources
		// (such as from tables) will have their own charset, which we won't know until after the parsing stage.
		charSet := b.ctx.GetCharacterSet()
		if _, isLiteral := innerExpr.(*expression.Literal); isLiteral && collation.CharacterSet() != charSet {
			b.handleErr(sql.ErrCollationInvalidForCharSet.New(collation.Name(), charSet.Name()))
		}
		expression.NewCollatedExpression(innerExpr, collation)
	case *ast.ValuesFuncExpr:
		col := b.buildScalar(inScope, v.Name)
		fn, err := b.cat.Function(b.ctx, "values")
		if err != nil {
			b.handleErr(err)
		}
		values, err := fn.NewInstance([]sql.Expression{col})
		if err != nil {
			b.handleErr(err)
		}
		return values
	case *ast.ExistsExpr:
		selScope := b.buildSelectStmt(inScope, v.Subquery.Select)
		// rebuild subquery
		// renameColumns
		selectString := ast.String(v.Subquery.Select)
		sq := plan.NewSubquery(selScope.node, selectString)
		return plan.NewExistsSubquery(sq)
	case *ast.TimestampFuncExpr:
		var (
			unit  sql.Expression
			expr1 sql.Expression
			expr2 sql.Expression
		)

		unit = expression.NewLiteral(v.Unit, types.LongText)
		expr1 = b.buildScalar(inScope, v.Expr1)
		expr2 = b.buildScalar(inScope, v.Expr2)

		if v.Name == "timestampdiff" {
			return function.NewTimestampDiff(unit, expr1, expr2)
		} else if v.Name == "timestampadd" {
			return nil
		}
		return nil
	case *ast.ExtractFuncExpr:
		var unit sql.Expression = expression.NewLiteral(strings.ToUpper(v.Unit), types.LongText)
		expr := b.buildScalar(inScope, v.Expr)
		return function.NewExtract(unit, expr)
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
	}
	return nil
}

func (b *PlanBuilder) buildGetField(inScope *scope, v *ast.ColName) *expression.GetField {
	table := strings.ToLower(v.Qualifier.String())
	col := strings.ToLower(v.Name.String())
	checkScope := inScope
	outerLen := inScope.outerScopeLen()
	for checkScope != nil {
		for i, c := range checkScope.cols {
			if c.col == col && (c.table == table || table == "") {
				return expression.NewGetFieldWithTable(outerLen+i, c.typ, c.table, c.col, c.nullable)
			}
		}
		checkScope = checkScope.parent
	}
	b.handleErr(sql.ErrColumnNotFound.New(v))
	return nil
}

func (b *PlanBuilder) resolveColumn(inScope *scope, v *ast.ColName) (scopeColumn, int) {
	table := strings.ToLower(v.Qualifier.String())
	col := strings.ToLower(v.Name.String())
	for i, c := range inScope.cols {
		if c.col == col && (c.table == table || table == "") {
			return c, i
		}
	}
	return scopeColumn{}, -1
}

func (b *PlanBuilder) buildUnaryScalar(inScope *scope, e *ast.UnaryExpr) sql.Expression {
	switch strings.ToLower(e.Operator) {
	case ast.MinusStr:
		expr := b.buildScalar(inScope, e.Expr)
		return expression.NewUnaryMinus(expr)
	case ast.PlusStr:
		// Unary plus expressions do nothing (do not turn the expression positive). Just return the underlying expressio return b.buildScalar(inScope, e.Expr)
		expr := b.buildScalar(inScope, e.Expr)
		return expression.NewBinary(expr)
	case ast.BangStr:
		c := b.buildScalar(inScope, e.Expr)
		return expression.NewNot(c)
	default:
		lowerOperator := strings.TrimSpace(strings.ToLower(e.Operator))
		if strings.HasPrefix(lowerOperator, "_") {
			// This is a character set introducer, so we need to decode the string to our internal encoding (`utf8mb4`)
			charSet, err := sql.ParseCharacterSet(lowerOperator[1:])
			if err != nil {
				b.handleErr(err)
			}
			if charSet.Encoder() == nil {
				err := sql.ErrUnsupportedFeature.New("unsupported character set: " + charSet.Name())
				b.handleErr(err)
			}

			// Due to how vitess orders expressions, COLLATE is a child rather than a parent, so we need to handle it in a special way
			collation := charSet.DefaultCollation()
			if collateExpr, ok := e.Expr.(*ast.CollateExpr); ok {
				// We extract the expression out of CollateExpr as we're only concerned about the collation string
				e.Expr = collateExpr.Expr
				// TODO: rename this from Charset to Collation
				collation, err = sql.ParseCollation(nil, &collateExpr.Charset, false)
				if err != nil {
					b.handleErr(err)
				}
				if collation.CharacterSet() != charSet {
					err := sql.ErrCollationInvalidForCharSet.New(collation.Name(), charSet.Name())
					b.handleErr(err)
				}
			}

			// Character set introducers only work on string literals
			expr := b.buildScalar(inScope, e.Expr)
			if _, ok := expr.(*expression.Literal); !ok || !types.IsText(expr.Type()) {
				err := sql.ErrCharSetIntroducer.New()
				b.handleErr(err)
			}
			literal, _ := expr.Eval(b.ctx, nil)

			// Internally all strings are `utf8mb4`, so we need to decode the string (which applies the introducer)
			if strLiteral, ok := literal.(string); ok {
				decodedLiteral, ok := charSet.Encoder().Decode(encodings.StringToBytes(strLiteral))
				if !ok {
					err := sql.ErrCharSetInvalidString.New(charSet.Name(), strLiteral)
					b.handleErr(err)
				}
				return expression.NewLiteral(encodings.BytesToString(decodedLiteral), types.CreateLongText(collation))
			} else if byteLiteral, ok := literal.([]byte); ok {
				decodedLiteral, ok := charSet.Encoder().Decode(byteLiteral)
				if !ok {
					err := sql.ErrCharSetInvalidString.New(charSet.Name(), strLiteral)
					b.handleErr(err)
				}
				return expression.NewLiteral(decodedLiteral, types.CreateLongText(collation))
			} else {
				// Should not be possible
				err := fmt.Errorf("expression literal returned type `%s` but literal value had type `%T`",
					expr.Type().String(), literal)
				b.handleErr(err)
			}
		}
		err := sql.ErrUnsupportedFeature.New("unary operator: " + e.Operator)
		b.handleErr(err)
	}
	return nil
}

func (b *PlanBuilder) buildBinaryScalar(inScope *scope, be *ast.BinaryExpr) sql.Expression {
	switch strings.ToLower(be.Operator) {
	case
		ast.PlusStr,
		ast.MinusStr,
		ast.MultStr,
		ast.DivStr,
		ast.ShiftLeftStr,
		ast.ShiftRightStr,
		ast.BitAndStr,
		ast.BitOrStr,
		ast.BitXorStr,
		ast.IntDivStr,
		ast.ModStr:

		l := b.buildScalar(inScope, be.Left)

		r := b.buildScalar(inScope, be.Right)

		_, lok := l.(*expression.Interval)
		_, rok := r.(*expression.Interval)
		if lok && be.Operator == "-" {
			err := sql.ErrUnsupportedSyntax.New("subtracting from an interval")
			b.handleErr(err)
		} else if (lok || rok) && be.Operator != "+" && be.Operator != "-" {
			err := sql.ErrUnsupportedSyntax.New("only + and - can be used to add or subtract intervals from dates")
			b.handleErr(err)
		} else if lok && rok {
			err := sql.ErrUnsupportedSyntax.New("intervals cannot be added or subtracted from other intervals")
			b.handleErr(err)
		}

		switch strings.ToLower(be.Operator) {
		case ast.DivStr:
			return expression.NewDiv(l, r)
		case ast.ModStr:
			return expression.NewMod(l, r)
		case ast.BitAndStr, ast.BitOrStr, ast.BitXorStr, ast.ShiftRightStr, ast.ShiftLeftStr:
			return expression.NewBitOp(l, r, be.Operator)
		case ast.IntDivStr:
			return expression.NewIntDiv(l, r)
		default:
			return expression.NewArithmetic(l, r, be.Operator)
		}
	case
		ast.JSONExtractOp,
		ast.JSONUnquoteExtractOp:
		err := sql.ErrUnsupportedFeature.New(fmt.Sprintf("(%s) JSON operators not supported", be.Operator))
		b.handleErr(err)

	default:
		err := sql.ErrUnsupportedFeature.New(be.Operator)
		b.handleErr(err)
	}
	return nil
}

func (b *PlanBuilder) buildLiteral(inScope *scope, v *ast.SQLVal) sql.Expression {
	switch v.Type {
	case ast.StrVal:
		return expression.NewLiteral(string(v.Val), types.CreateLongText(b.ctx.GetCollation()))
	case ast.IntVal:
		return b.convertInt(string(v.Val), 10)
	case ast.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			b.handleErr(err)
		}

		// use the value as string format to keep precision and scale as defined for DECIMAL data type to avoid rounded up float64 value
		if ps := strings.Split(string(v.Val), "."); len(ps) == 2 {
			ogVal := string(v.Val)
			floatVal := fmt.Sprintf("%v", val)
			if len(ogVal) >= len(floatVal) && ogVal != floatVal {
				p, s := expression.GetDecimalPrecisionAndScale(ogVal)
				dt, err := types.CreateDecimalType(p, s)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(b.ctx.GetCollation()))
				}
				dVal, _, err := dt.Convert(ogVal)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(b.ctx.GetCollation()))
				}
				return expression.NewLiteral(dVal, dt)
			}
		}

		return expression.NewLiteral(val, types.Float64)
	case ast.HexNum:
		//TODO: binary collation?
		v := strings.ToLower(string(v.Val))
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
		} else if strings.HasPrefix(v, "x") {
			v = strings.Trim(v[1:], "'")
		}

		valBytes := []byte(v)
		dst := make([]byte, hex.DecodedLen(len(valBytes)))
		_, err := hex.Decode(dst, valBytes)
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewLiteral(dst, types.LongBlob)
	case ast.HexVal:
		//TODO: binary collation?
		val, err := v.HexDecode()
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewLiteral(val, types.LongBlob)
	case ast.ValArg:
		return expression.NewBindVar(strings.TrimPrefix(string(v.Val), ":"))
	case ast.BitVal:
		if len(v.Val) == 0 {
			return expression.NewLiteral(0, types.Uint64)
		}

		res, err := strconv.ParseUint(string(v.Val), 2, 64)
		if err != nil {
			b.handleErr(err)
		}

		return expression.NewLiteral(res, types.Uint64)
	}

	b.handleErr(sql.ErrInvalidSQLValType.New(v.Type))
	return nil
}

func (b *PlanBuilder) buildFilter() {

}

func (b *PlanBuilder) analyzeAggregation() {

}

func (b *PlanBuilder) buildWhere(inScope *scope, where *ast.Where) {
	if where == nil {
		return
	}
	filter := b.buildScalar(inScope, where.Expr)
	filterNode := plan.NewFilter(filter, inScope.node)
	inScope.node = filterNode
}

func (b *PlanBuilder) analyzeOrderBy(fromScope, projScope *scope, order ast.OrderBy) (outScope *scope) {
	// - regular col
	// - ordinal into proj
	// - getfield output of proj

	// if regular col, make sure in aggOut or add
	// (sort before projecting final group by result)

	// if ordinal into proj
	// get the reference to the i'th output
	outScope = fromScope.replace()
	//outerLen := fromScope.outerScopeLen()

	for _, o := range order {
		var descending bool
		switch strings.ToLower(o.Direction) {
		default:
			err := errInvalidSortOrder.New(o.Direction)
			b.handleErr(err)
		case ast.AscScr:
			descending = false
		case ast.DescScr:
			descending = true
		}

		var expr sql.Expression
		switch e := o.Expr.(type) {
		case *ast.ColName:
			// add to extra cols
			c, idx := b.resolveColumn(fromScope, e)
			if idx == -1 {
				err := sql.ErrColumnNotFound.New(e.Name)
				b.handleErr(err)
			}
			c.descending = descending
			c.scalar = expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
			outScope.addColumn(c)
			fromScope.addExtraColumn(c)
		case *ast.SQLVal:
			// integer literal into projScope
			// else throw away
			if e.Type == ast.IntVal {
				lit := b.convertInt(string(e.Val), 10)
				idx, _, err := types.Int64.Convert(lit.Value())
				if err != nil {
					b.handleErr(err)
				}
				intIdx, ok := idx.(int64)
				if !ok {
					b.handleErr(fmt.Errorf("expected integer order by literal"))
				}
				if intIdx < 1 {
					b.handleErr(fmt.Errorf("expected positive integer order by literal"))
				}
				target := projScope.cols[intIdx-1]
				var gf *expression.GetField
				if target.scalar != nil {
					gf = expression.NewGetFieldWithTable(int(target.id), target.typ, "", target.scalar.String(), target.nullable)
				} else {
					gf = expression.NewGetFieldWithTable(int(target.id), target.typ, target.table, target.col, target.nullable)
				}
				outScope.addColumn(scopeColumn{
					table:      gf.Table(),
					col:        gf.Name(),
					scalar:     gf,
					typ:        gf.Type(),
					nullable:   gf.IsNullable(),
					descending: descending,
					id:         target.id,
				})
				expr = gf
			}
		default:
			// we could add to aggregates here, ref GF in aggOut
			expr = b.buildScalar(fromScope, e)
			col := scopeColumn{
				table:      "",
				col:        expr.String(),
				scalar:     expr,
				typ:        expr.Type(),
				nullable:   expr.IsNullable(),
				descending: descending,
			}
			_, ok := outScope.getExpr(expr.String())
			if !ok {
				outScope.addColumn(col)
			}
		}
	}
	return
}

func (b *PlanBuilder) buildLimit(inScope *scope, limit *ast.Limit) sql.Expression {
	// Limit must wrap offset, and not vice-versa, so that skipped rows don't count toward the returned row count.
	if limit != nil && limit.Offset != nil {
		return b.buildScalar(inScope, limit.Offset)
	} else if limit != nil {
		return b.buildScalar(inScope, limit.Rowcount)
	}
	return nil
}

func (b *PlanBuilder) buildOrderBy(inScope, orderByScope *scope) {
	// TODO build Sort node over input
	if len(orderByScope.cols) == 0 {
		return
	}
	var sortFields sql.SortFields
	for _, c := range orderByScope.cols {
		so := sql.Ascending
		if c.descending {
			so = sql.Descending
		}
		sf := sql.SortField{
			Column: c.scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}
	sort := plan.NewSort(sortFields, inScope.node)
	inScope.node = sort
	return
}

type parseErr struct {
	err error
}

func (b *PlanBuilder) handleErr(err error) {
	panic(parseErr{err})
}
