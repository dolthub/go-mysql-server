package optbuilder

import (
	"encoding/hex"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strconv"
	"strings"
)

type scope struct {
	b      *PlanBuilder
	parent *scope
	ast    ast.SQLNode
	node   sql.Node

	cols []scopeColumn
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
		s.cols[i].table = t
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
	return &scope{
		parent: s.parent,
	}
}

func (s *scope) addColumn(col scopeColumn) {
	s.cols = append(s.cols, col)
}

func (s *scope) addColumns(cols []scopeColumn) {
	s.cols = append(s.cols, cols...)
}

func (s *scope) appendColumnsFromScope(src *scope) {
	l := len(s.cols)
	s.cols = append(s.cols, src.cols...)
	// these become pass-through columns in the new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

type columnId uint16

type scopeColumn struct {
	db     string
	table  string
	col    string
	id     columnId
	typ    sql.Type
	scalar sql.Expression
}

type PlanBuilder struct {
	ctx             *sql.Context
	cat             sql.Catalog
	tabId           uint16
	colI            uint16
	currentDatabase sql.Database
}

func (b *PlanBuilder) newScope() *scope {
	return &scope{}
}

func (b *PlanBuilder) buildSelectStmt(inScope *scope, s ast.SelectStatement) (outScope *scope) {
	switch s := s.(type) {
	case *ast.Select:
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
	// TODO CTEs
	fromScope := b.buildFrom(inScope, s.From)
	// TODO windows
	b.buildWhere(fromScope, s.Where)
	//TODO aggregation will split scopes
	// fromScope (into agg) -> projection (out of agg)
	projScope := fromScope.replace()

	b.analyzeProjectionList(fromScope, projScope, s.SelectExprs)
	b.buildProjection(fromScope, projScope)
	outScope = projScope
	return
}

func (b *PlanBuilder) analyzeProjectionList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	b.analyzeSelectList(inScope, outScope, selectExprs)
}

func (b *PlanBuilder) analyzeSelectList(inScope, outScope *scope, selectExprs ast.SelectExprs) {
	// use inScope to construct projections for projScope
	var exprs []sql.Expression
	outerLen := inScope.outerScopeLen()
	for _, se := range selectExprs {
		pe := b.selectExprToExpression(inScope, se)
		if star, ok := pe.(*expression.Star); ok {
			for i, c := range inScope.cols {
				if c.table == star.Table || star.Table == "" {
					gf := expression.NewGetFieldWithTable(outerLen+i, c.typ, c.table, c.col, true)
					exprs = append(exprs, gf)
					outScope.addColumn(scopeColumn{table: c.table, col: c.col, scalar: gf, typ: gf.Type()})
				}
			}
		} else {
			exprs = append(exprs, pe)
			outScope.addColumn(scopeColumn{col: pe.String(), scalar: pe, typ: pe.Type()})
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
	}

	rt := plan.NewResolvedTable(tab, database, asOfLit)
	outScope.node = rt
	if asofBindVar {
		outScope.node = plan.NewDeferredAsOfTable(rt, asOfExpr)
	}

	for _, c := range tab.Schema() {
		outScope.addColumn(scopeColumn{
			db:    strings.ToLower(db),
			table: strings.ToLower(tab.Name()),
			col:   strings.ToLower(c.Name),
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
		val, err := convertVal(b.ctx, v)
		if err != nil {
			b.handleErr(err)
		}
		return val
	case ast.BoolVal:
		return expression.NewLiteral(bool(v), types.Boolean)
	case *ast.NullVal:
		return expression.NewLiteral(nil, types.Null)
	case *ast.ColName:
		table := strings.ToLower(v.Qualifier.String())
		col := strings.ToLower(v.Name.String())
		checkScope := inScope
		outerLen := inScope.outerScopeLen()
		for checkScope != nil {
			for i, c := range checkScope.cols {
				if c.col == col && (c.table == table || table == "") {
					return expression.NewGetFieldWithTable(outerLen+i, c.typ, c.table, c.col, true)
				}
			}
			checkScope = checkScope.parent
		}
		b.handleErr(sql.ErrColumnNotFound.New(v))
	case *ast.FuncExpr:
		args := make([]sql.Expression, len(v.Exprs))
		for i, e := range v.Exprs {
			args[i] = b.selectExprToExpression(inScope, e)
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

		name := v.Name.Lowered()
		f, err := b.cat.Function(b.ctx, name)
		if err != nil {
			b.handleErr(err)
		}

		rf, err := f.NewInstance(args)
		if err != nil {
			b.handleErr(err)
		}

		return rf

	case *ast.GroupConcatExpr:
		// TODO this is an aggregation
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
		return b.buildScalar(inScope, v)
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
		return expression.NewUnresolvedFunction("values", false, nil, col)
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

func (b *PlanBuilder) buildLiteral(ctx *sql.Context, v *ast.SQLVal) (sql.Expression, error) {
	switch v.Type {
	case ast.StrVal:
		return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
	case ast.IntVal:
		return convertInt(string(v.Val), 10)
	case ast.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, err
		}

		// use the value as string format to keep precision and scale as defined for DECIMAL data type to avoid rounded up float64 value
		if ps := strings.Split(string(v.Val), "."); len(ps) == 2 {
			ogVal := string(v.Val)
			floatVal := fmt.Sprintf("%v", val)
			if len(ogVal) >= len(floatVal) && ogVal != floatVal {
				p, s := expression.GetDecimalPrecisionAndScale(ogVal)
				dt, err := types.CreateDecimalType(p, s)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
				}
				dVal, _, err := dt.Convert(ogVal)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
				}
				return expression.NewLiteral(dVal, dt), nil
			}
		}

		return expression.NewLiteral(val, types.Float64), nil
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
			return nil, err
		}
		return expression.NewLiteral(dst, types.LongBlob), nil
	case ast.HexVal:
		//TODO: binary collation?
		val, err := v.HexDecode()
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, types.LongBlob), nil
	case ast.ValArg:
		return expression.NewBindVar(strings.TrimPrefix(string(v.Val), ":")), nil
	case ast.BitVal:
		if len(v.Val) == 0 {
			return expression.NewLiteral(0, types.Uint64), nil
		}

		res, err := strconv.ParseUint(string(v.Val), 2, 64)
		if err != nil {
			return nil, err
		}

		return expression.NewLiteral(res, types.Uint64), nil
	}

	return nil, sql.ErrInvalidSQLValType.New(v.Type)
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

func (b *PlanBuilder) analyzeHaving() {

}

func (b *PlanBuilder) buildDistinct() {

}

func (b *PlanBuilder) buildOrderBy() {

}

type parseErr struct {
	err error
}

func (b *PlanBuilder) handleErr(err error) {
	panic(parseErr{err})
}
