package parse // import "gopkg.in/src-d/go-mysql-server.v0/sql/parse"

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
)

var (
	// ErrUnsupportedSyntax is thrown when a specific syntax is not already supported
	ErrUnsupportedSyntax = errors.NewKind("unsupported syntax: %#v")

	// ErrUnsupportedFeature is thrown when a feature is not already supported
	ErrUnsupportedFeature = errors.NewKind("unsupported feature: %s")

	// ErrInvalidSQLValType is returned when a SQLVal type is not valid.
	ErrInvalidSQLValType = errors.NewKind("invalid SQLVal of type: %d")

	// ErrInvalidSortOrder is returned when a sort order is not valid.
	ErrInvalidSortOrder = errors.NewKind("invalod sort order: %s")
)

var (
	describeTablesRegex = regexp.MustCompile(`^describe\s+table\s+(.*)`)
	createIndexRegex    = regexp.MustCompile(`^create\s+index\s+`)
	dropIndexRegex      = regexp.MustCompile(`^drop\s+index\s+`)
	showIndexRegex      = regexp.MustCompile(`^show\s+(index|indexes|keys)\s+(from|in)\s+\S+\s*`)
	describeRegex       = regexp.MustCompile(`^(describe|desc|explain)\s+(.*)\s+`)
)

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(ctx *sql.Context, s string) (sql.Node, error) {
	span, ctx := ctx.Span("parse", opentracing.Tag{Key: "query", Value: s})
	defer span.Finish()

	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, ";") {
		s = s[:len(s)-1]
	}

	lowerQuery := strings.ToLower(s)
	switch true {
	case describeTablesRegex.MatchString(lowerQuery):
		return parseDescribeTables(lowerQuery)
	case createIndexRegex.MatchString(lowerQuery):
		return parseCreateIndex(s)
	case dropIndexRegex.MatchString(lowerQuery):
		return parseDropIndex(s)
	case showIndexRegex.MatchString(lowerQuery):
		return parseShowIndex(s)
	case describeRegex.MatchString(lowerQuery):
		return parseDescribeQuery(ctx, s)
	}

	stmt, err := sqlparser.Parse(s)
	if err != nil {
		return nil, err
	}

	return convert(ctx, stmt, s)
}

func parseDescribeTables(s string) (sql.Node, error) {
	t := describeTablesRegex.FindStringSubmatch(s)
	if len(t) == 2 && t[1] != "" {
		return plan.NewDescribe(plan.NewUnresolvedTable(t[1])), nil
	}

	return nil, ErrUnsupportedSyntax.New(s)
}

func convert(ctx *sql.Context, stmt sqlparser.Statement, query string) (sql.Node, error) {
	switch n := stmt.(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(n)
	case *sqlparser.Show:
		return convertShow(n)
	case *sqlparser.Select:
		return convertSelect(ctx, n)
	case *sqlparser.Insert:
		return convertInsert(ctx, n)
	case *sqlparser.DDL:
		return convertDDL(n)
	}
}

func convertShow(s *sqlparser.Show) (sql.Node, error) {
	if s.Type != sqlparser.KeywordString(sqlparser.TABLES) {
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		return nil, ErrUnsupportedFeature.New(unsupportedShow)
	}

	return plan.NewShowTables(&sql.UnresolvedDatabase{}), nil
}

func convertSelect(ctx *sql.Context, s *sqlparser.Select) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, s.From)
	if err != nil {
		return nil, err
	}

	if s.Having != nil {
		return nil, ErrUnsupportedFeature.New("HAVING")
	}

	if s.Where != nil {
		node, err = whereToFilter(s.Where, node)
		if err != nil {
			return nil, err
		}
	}

	node, err = selectToProjectOrGroupBy(s.SelectExprs, s.GroupBy, node)
	if err != nil {
		return nil, err
	}

	if s.Distinct != "" {
		node = plan.NewDistinct(node)
	}

	if len(s.OrderBy) != 0 {
		node, err = orderByToSort(s.OrderBy, node)
		if err != nil {
			return nil, err
		}
	}

	if s.Limit != nil {
		node, err = limitToLimit(ctx, s.Limit.Rowcount, node)
		if err != nil {
			return nil, err
		}
	}

	if s.Limit != nil && s.Limit.Offset != nil {
		node, err = offsetToOffset(ctx, s.Limit.Offset, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func convertDDL(c *sqlparser.DDL) (sql.Node, error) {
	switch c.Action {
	case sqlparser.CreateStr:
		return convertCreateTable(c)
	default:
		return nil, ErrUnsupportedSyntax.New(c)
	}
}

func convertCreateTable(c *sqlparser.DDL) (sql.Node, error) {
	schema, err := columnDefinitionToSchema(c.TableSpec.Columns)
	if err != nil {
		return nil, err
	}

	return plan.NewCreateTable(
		&sql.UnresolvedDatabase{},
		c.NewName.Name.String(),
		schema,
	), nil
}

func convertInsert(ctx *sql.Context, i *sqlparser.Insert) (sql.Node, error) {
	if len(i.OnDup) > 0 {
		return nil, ErrUnsupportedFeature.New("ON DUPLICATE KEY")
	}

	if len(i.Ignore) > 0 {
		return nil, ErrUnsupportedSyntax.New(i)
	}

	src, err := insertRowsToNode(ctx, i.Rows)
	if err != nil {
		return nil, err
	}

	return plan.NewInsertInto(
		plan.NewUnresolvedTable(i.Table.Name.String()),
		src,
		columnsToStrings(i.Columns),
	), nil
}

func columnDefinitionToSchema(colDef []*sqlparser.ColumnDefinition) (sql.Schema, error) {
	var schema sql.Schema
	for _, cd := range colDef {
		typ := cd.Type
		internalTyp, err := sql.MysqlTypeToType(typ.SQLType())
		if err != nil {
			return nil, err
		}

		schema = append(schema, &sql.Column{
			Nullable: !bool(typ.NotNull),
			Type:     internalTyp,
			Name:     cd.Name.String(),
			// TODO
			Default: nil,
		})
	}

	return schema, nil
}

func columnsToStrings(cols sqlparser.Columns) []string {
	res := make([]string, len(cols))
	for i, c := range cols {
		res[i] = c.String()
	}

	return res
}

func insertRowsToNode(ctx *sql.Context, ir sqlparser.InsertRows) (sql.Node, error) {
	switch v := ir.(type) {
	case *sqlparser.Select:
		return convertSelect(ctx, v)
	case *sqlparser.Union:
		return nil, ErrUnsupportedFeature.New("UNION")
	case sqlparser.Values:
		return valuesToValues(v)
	default:
		return nil, ErrUnsupportedSyntax.New(ir)
	}
}

func valuesToValues(v sqlparser.Values) (sql.Node, error) {
	exprTuples := make([][]sql.Expression, len(v))
	for i, vt := range v {
		exprs := make([]sql.Expression, len(vt))
		exprTuples[i] = exprs
		for j, e := range vt {
			expr, err := exprToExpression(e)
			if err != nil {
				return nil, err
			}

			exprs[j] = expr
		}
	}

	return plan.NewValues(exprTuples), nil
}

func tableExprsToTable(
	ctx *sql.Context,
	te sqlparser.TableExprs,
) (sql.Node, error) {
	if len(te) == 0 {
		return nil, ErrUnsupportedFeature.New("zero tables in FROM")
	}

	var nodes []sql.Node
	for _, t := range te {
		n, err := tableExprToTable(ctx, t)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	if len(nodes) == 1 {
		return nodes[0], nil
	}

	join := plan.NewCrossJoin(nodes[0], nodes[1])
	for i := 2; i < len(nodes); i++ {
		join = plan.NewCrossJoin(join, nodes[i])
	}

	return join, nil
}

func tableExprToTable(
	ctx *sql.Context,
	te sqlparser.TableExpr,
) (sql.Node, error) {
	switch t := (te).(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(te)
	case *sqlparser.AliasedTableExpr:
		// TODO: Add support for qualifier.
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			if !e.Qualifier.IsEmpty() {
				return nil, ErrUnsupportedFeature.New("table name qualifiers")
			}

			node := plan.NewUnresolvedTable(e.Name.String())
			if !t.As.IsEmpty() {
				return plan.NewTableAlias(t.As.String(), node), nil
			}

			return node, nil
		case *sqlparser.Subquery:
			node, err := convert(ctx, e.Select, "")
			if err != nil {
				return nil, err
			}

			if t.As.IsEmpty() {
				return nil, ErrUnsupportedFeature.New("subquery without alias")
			}

			return plan.NewSubqueryAlias(t.As.String(), node), nil
		default:
			return nil, ErrUnsupportedSyntax.New(te)
		}
	case *sqlparser.JoinTableExpr:
		// TODO: add support for the rest of joins
		if t.Join != sqlparser.JoinStr && t.Join != sqlparser.NaturalJoinStr {
			return nil, ErrUnsupportedFeature.New(t.Join)
		}

		// TODO: add support for using, once we have proper table
		// qualification of fields
		if len(t.Condition.Using) > 0 {
			return nil, ErrUnsupportedFeature.New("using clause on join")
		}

		left, err := tableExprToTable(ctx, t.LeftExpr)
		if err != nil {
			return nil, err
		}

		right, err := tableExprToTable(ctx, t.RightExpr)
		if err != nil {
			return nil, err
		}

		if t.Join == sqlparser.NaturalJoinStr {
			return plan.NewNaturalJoin(left, right), nil
		}

		cond, err := exprToExpression(t.Condition.On)
		if err != nil {
			return nil, err
		}

		return plan.NewInnerJoin(left, right, cond), nil
	}
}

func whereToFilter(w *sqlparser.Where, child sql.Node) (*plan.Filter, error) {
	c, err := exprToExpression(w.Expr)
	if err != nil {
		return nil, err
	}

	return plan.NewFilter(c, child), nil
}

func orderByToSort(ob sqlparser.OrderBy, child sql.Node) (*plan.Sort, error) {
	var sortFields []plan.SortField
	for _, o := range ob {
		e, err := exprToExpression(o.Expr)
		if err != nil {
			return nil, err
		}

		var so plan.SortOrder
		switch o.Direction {
		default:
			return nil, ErrInvalidSortOrder.New(o.Direction)
		case sqlparser.AscScr:
			so = plan.Ascending
		case sqlparser.DescScr:
			so = plan.Descending
		}

		sf := plan.SortField{Column: e, Order: so}
		sortFields = append(sortFields, sf)
	}

	return plan.NewSort(sortFields, child), nil
}

func limitToLimit(
	ctx *sql.Context,
	limit sqlparser.Expr,
	child sql.Node,
) (*plan.Limit, error) {
	e, err := exprToExpression(limit)
	if err != nil {
		return nil, err
	}

	nl, ok := e.(*expression.Literal)
	if !ok || nl.Type() != sql.Int64 {
		return nil, ErrUnsupportedFeature.New("LIMIT with non-integer literal")
	}

	n, err := nl.Eval(ctx, nil)
	if err != nil {
		return nil, err
	}
	return plan.NewLimit(n.(int64), child), nil
}

func offsetToOffset(
	ctx *sql.Context,
	offset sqlparser.Expr,
	child sql.Node,
) (*plan.Offset, error) {
	e, err := exprToExpression(offset)
	if err != nil {
		return nil, err
	}

	nl, ok := e.(*expression.Literal)
	if !ok || nl.Type() != sql.Int64 {
		return nil, ErrUnsupportedFeature.New("OFFSET with non-integer literal")
	}

	n, err := nl.Eval(ctx, nil)
	if err != nil {
		return nil, err
	}
	return plan.NewOffset(n.(int64), child), nil
}

func isAggregate(e sql.Expression) bool {
	var isAgg bool
	expression.Inspect(e, func(e sql.Expression) bool {
		fn, ok := e.(*expression.UnresolvedFunction)
		if ok {
			isAgg = isAgg || fn.IsAggregate
		}

		return true
	})
	return isAgg
}

func selectToProjectOrGroupBy(se sqlparser.SelectExprs, g sqlparser.GroupBy, child sql.Node) (sql.Node, error) {
	selectExprs, err := selectExprsToExpressions(se)
	if err != nil {
		return nil, err
	}

	isAgg := len(g) > 0
	if !isAgg {
		for _, e := range selectExprs {
			if isAggregate(e) {
				isAgg = true
				break
			}
		}
	}

	if isAgg {
		groupingExprs, err := groupByToExpressions(g)
		if err != nil {
			return nil, err
		}

		return plan.NewGroupBy(selectExprs, groupingExprs, child), nil
	}

	return plan.NewProject(selectExprs, child), nil
}

func selectExprsToExpressions(se sqlparser.SelectExprs) ([]sql.Expression, error) {
	var exprs []sql.Expression
	for _, e := range se {
		pe, err := selectExprToExpression(e)
		if err != nil {
			return nil, err
		}

		exprs = append(exprs, pe)
	}

	return exprs, nil
}

func exprToExpression(e sqlparser.Expr) (sql.Expression, error) {
	switch v := e.(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(e)
	case *sqlparser.ComparisonExpr:
		return comparisonExprToExpression(v)
	case *sqlparser.IsExpr:
		return isExprToExpression(v)
	case *sqlparser.NotExpr:
		c, err := exprToExpression(v.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewNot(c), nil
	case *sqlparser.SQLVal:
		return convertVal(v)
	case sqlparser.BoolVal:
		return expression.NewLiteral(bool(v), sql.Boolean), nil
	case *sqlparser.NullVal:
		return expression.NewLiteral(nil, sql.Null), nil
	case *sqlparser.ColName:
		// TODO: add handling of case sensitiveness.
		if !v.Qualifier.IsEmpty() {
			return expression.NewUnresolvedQualifiedColumn(
				v.Qualifier.Name.String(),
				v.Name.Lowered(),
			), nil
		}
		return expression.NewUnresolvedColumn(v.Name.Lowered()), nil
	case *sqlparser.FuncExpr:
		exprs, err := selectExprsToExpressions(v.Exprs)
		if err != nil {
			return nil, err
		}

		return expression.NewUnresolvedFunction(v.Name.Lowered(),
			v.IsAggregate(), exprs...), nil
	case *sqlparser.ParenExpr:
		return exprToExpression(v.Expr)
	case *sqlparser.AndExpr:
		lhs, err := exprToExpression(v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := exprToExpression(v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewAnd(lhs, rhs), nil
	case *sqlparser.OrExpr:
		lhs, err := exprToExpression(v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := exprToExpression(v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewOr(lhs, rhs), nil
	case *sqlparser.ConvertExpr:
		expr, err := exprToExpression(v.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewConvert(expr, v.Type.Type), nil
	case *sqlparser.RangeCond:
		val, err := exprToExpression(v.Left)
		if err != nil {
			return nil, err
		}

		lower, err := exprToExpression(v.From)
		if err != nil {
			return nil, err
		}

		upper, err := exprToExpression(v.To)
		if err != nil {
			return nil, err
		}

		switch v.Operator {
		case sqlparser.BetweenStr:
			return expression.NewBetween(val, lower, upper), nil
		case sqlparser.NotBetweenStr:
			return expression.NewNot(expression.NewBetween(val, lower, upper)), nil
		default:
			return nil, ErrUnsupportedFeature.New(fmt.Sprintf("RangeCond with operator: %s", v.Operator))
		}
	case sqlparser.ValTuple:
		var exprs = make([]sql.Expression, len(v))
		for i, e := range v {
			expr, err := exprToExpression(e)
			if err != nil {
				return nil, err
			}
			exprs[i] = expr
		}
		return expression.NewTuple(exprs...), nil

	case *sqlparser.BinaryExpr:
		return binaryExprToExpression(v)
	}
}

func convertVal(v *sqlparser.SQLVal) (sql.Expression, error) {
	switch v.Type {
	case sqlparser.StrVal:
		return expression.NewLiteral(string(v.Val), sql.Text), nil
	case sqlparser.IntVal:
		//TODO: Use smallest integer representation and widen later.
		val, err := strconv.ParseInt(string(v.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.Int64), nil
	case sqlparser.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.Float64), nil
	case sqlparser.HexNum:
		v := strings.ToLower(string(v.Val))
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
		} else if strings.HasPrefix(v, "x") {
			v = strings.Trim(v[1:], "'")
		}

		val, err := strconv.ParseInt(v, 16, 64)
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.Int64), nil
	case sqlparser.HexVal:
		val, err := v.HexDecode()
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.Blob), nil
	case sqlparser.ValArg:
		return expression.NewLiteral(string(v.Val), sql.Text), nil
	case sqlparser.BitVal:
		return expression.NewLiteral(v.Val[0] == '1', sql.Boolean), nil
	}

	return nil, ErrInvalidSQLValType.New(v.Type)
}

func isExprToExpression(c *sqlparser.IsExpr) (sql.Expression, error) {
	e, err := exprToExpression(c.Expr)
	if err != nil {
		return nil, err
	}

	switch c.Operator {
	case sqlparser.IsNullStr:
		return expression.NewIsNull(e), nil
	case sqlparser.IsNotNullStr:
		return expression.NewNot(expression.NewIsNull(e)), nil
	default:
		return nil, ErrUnsupportedSyntax.New(c)
	}
}

func comparisonExprToExpression(c *sqlparser.ComparisonExpr) (sql.Expression, error) {
	left, err := exprToExpression(c.Left)
	if err != nil {
		return nil, err
	}

	right, err := exprToExpression(c.Right)
	if err != nil {
		return nil, err
	}

	switch c.Operator {
	default:
		return nil, ErrUnsupportedFeature.New(c.Operator)
	case sqlparser.RegexpStr:
		return expression.NewRegexp(left, right), nil
	case sqlparser.NotRegexpStr:
		return expression.NewNot(expression.NewRegexp(left, right)), nil
	case sqlparser.EqualStr:
		return expression.NewEquals(left, right), nil
	case sqlparser.LessThanStr:
		return expression.NewLessThan(left, right), nil
	case sqlparser.LessEqualStr:
		return expression.NewLessThanOrEqual(left, right), nil
	case sqlparser.GreaterThanStr:
		return expression.NewGreaterThan(left, right), nil
	case sqlparser.GreaterEqualStr:
		return expression.NewGreaterThanOrEqual(left, right), nil
	case sqlparser.NotEqualStr:
		return expression.NewNot(
			expression.NewEquals(left, right),
		), nil
	case sqlparser.InStr:
		return expression.NewIn(left, right), nil
	case sqlparser.NotInStr:
		return expression.NewNotIn(left, right), nil
	}
}

func groupByToExpressions(g sqlparser.GroupBy) ([]sql.Expression, error) {
	es := make([]sql.Expression, len(g))
	for i, ve := range g {
		e, err := exprToExpression(ve)
		if err != nil {
			return nil, err
		}

		es[i] = e
	}

	return es, nil
}

func selectExprToExpression(se sqlparser.SelectExpr) (sql.Expression, error) {
	switch e := se.(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(e)
	case *sqlparser.StarExpr:
		if e.TableName.IsEmpty() {
			return expression.NewStar(), nil
		}
		return expression.NewQualifiedStar(e.TableName.Name.String()), nil
	case *sqlparser.AliasedExpr:
		expr, err := exprToExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		if e.As.String() == "" {
			return expr, nil
		}

		// TODO: Handle case-sensitiveness when needed.
		return expression.NewAlias(expr, e.As.Lowered()), nil
	}
}

func binaryExprToExpression(be *sqlparser.BinaryExpr) (sql.Expression, error) {
	switch be.Operator {
	case
		sqlparser.PlusStr,
		sqlparser.MinusStr,
		sqlparser.MultStr,
		sqlparser.DivStr,
		sqlparser.ShiftLeftStr,
		sqlparser.ShiftRightStr,
		sqlparser.BitAndStr,
		sqlparser.BitOrStr,
		sqlparser.BitXorStr,
		sqlparser.IntDivStr,
		sqlparser.ModStr:

		l, err := exprToExpression(be.Left)
		if err != nil {
			return nil, err
		}

		r, err := exprToExpression(be.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewArithmetic(l, r, be.Operator), nil

	default:
		return nil, ErrUnsupportedFeature.New(be.Operator)
	}
}
