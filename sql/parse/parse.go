package parse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
)

func errUnsupported(n sqlparser.SQLNode) error {
	return fmt.Errorf("unsupported syntax: %#v", n)
}

func errUnsupportedFeature(feature string) error {
	return fmt.Errorf("unsupported feature: %s", feature)
}

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(s string) (sql.Node, error) {
	if strings.HasSuffix(s, ";") {
		s = s[:len(s)-1]
	}

	t := regexp.MustCompile(`^describe\s+table\s+(.*)`).FindStringSubmatch(strings.ToLower(s))
	if len(t) == 2 && t[1] != "" {
		return plan.NewDescribe(plan.NewUnresolvedTable(t[1])), nil
	}

	stmt, err := sqlparser.Parse(s)
	if err != nil {
		return nil, err
	}

	return convert(stmt)
}

func convert(stmt sqlparser.Statement) (sql.Node, error) {
	switch n := stmt.(type) {
	default:
		return nil, errUnsupported(n)
	case *sqlparser.Show:
		return convertShow(n)
	case *sqlparser.Select:
		return convertSelect(n)
	case *sqlparser.Insert:
		return convertInsert(n)
	}
}

func convertShow(s *sqlparser.Show) (sql.Node, error) {
	if s.Type != sqlparser.KeywordString(sqlparser.TABLES) {
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		return nil, errUnsupportedFeature(unsupportedShow)
	}

	return plan.NewShowTables(&sql.UnresolvedDatabase{}), nil
}

func convertSelect(s *sqlparser.Select) (sql.Node, error) {
	var node sql.Node

	node, err := tableExprsToTable(s.From)
	if err != nil {
		return nil, err
	}

	if s.Having != nil {
		return nil, errUnsupportedFeature("HAVING")
	}

	if s.Where != nil {
		node, err = whereToFilter(s.Where, node)
		if err != nil {
			return nil, err
		}
	}

	if len(s.OrderBy) != 0 {
		node, err = orderByToSort(s.OrderBy, node)
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

	if s.Limit != nil {
		node, err = limitToLimit(s.Limit.Rowcount, node)
		if err != nil {
			return nil, err
		}
	}

	if s.Limit != nil && s.Limit.Offset != nil {
		node, err = offsetToOffset(s.Limit.Offset, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func convertInsert(i *sqlparser.Insert) (sql.Node, error) {
	if len(i.OnDup) > 0 {
		return nil, errUnsupportedFeature("ON DUPLICATE KEY")
	}

	if len(i.Ignore) > 0 {
		return nil, errUnsupported(i)
	}

	src, err := insertRowsToNode(i.Rows)
	if err != nil {
		return nil, err
	}

	return plan.NewInsertInto(
		plan.NewUnresolvedTable(i.Table.Name.String()),
		src,
		columnsToStrings(i.Columns),
	), nil
}

func columnsToStrings(cols sqlparser.Columns) []string {
	res := make([]string, len(cols))
	for i, c := range cols {
		res[i] = c.String()
	}

	return res
}

func insertRowsToNode(ir sqlparser.InsertRows) (sql.Node, error) {
	switch v := ir.(type) {
	case *sqlparser.Select:
		return convertSelect(v)
	case *sqlparser.Union:
		return nil, errUnsupportedFeature("UNION")
	case sqlparser.Values:
		return valuesToValues(v)
	default:
		return nil, errUnsupported(ir)
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

func tableExprsToTable(te sqlparser.TableExprs) (sql.Node, error) {
	if len(te) == 0 {
		return nil, errUnsupportedFeature("zero tables in FROM")
	}

	var nodes []sql.Node
	for _, t := range te {
		n, err := tableExprToTable(t)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	if len(nodes) == 1 {
		return nodes[0], nil
	}

	if len(nodes) == 2 {
		return plan.NewCrossJoin(nodes[0], nodes[1]), nil
	}

	//TODO: Support N tables in JOIN.
	return nil, errUnsupportedFeature("more than 2 tables in JOIN")
}

func tableExprToTable(te sqlparser.TableExpr) (sql.Node, error) {
	switch t := (te).(type) {
	default:
		return nil, errUnsupported(te)
	case *sqlparser.AliasedTableExpr:
		//TODO: Add support for table alias.
		//TODO: Add support for qualifier.
		tn, ok := t.Expr.(sqlparser.TableName)
		if !ok {
			return nil, errUnsupportedFeature("non simple tables")
		}

		return plan.NewUnresolvedTable(tn.Name.String()), nil
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
			panic(fmt.Errorf("invalid sort order: %s", o.Direction))
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

func limitToLimit(limit sqlparser.Expr, child sql.Node) (*plan.Limit, error) {
	e, err := exprToExpression(limit)
	if err != nil {
		return nil, err
	}

	nl, ok := e.(*expression.Literal)
	if !ok || nl.Type() != sql.Int64 {
		return nil, errUnsupportedFeature("LIMIT with non-integer literal")
	}

	n, err := nl.Eval(nil)
	if err != nil {
		return nil, err
	}
	return plan.NewLimit(n.(int64), child), nil
}

func offsetToOffset(offset sqlparser.Expr, child sql.Node) (*plan.Offset, error) {
	e, err := exprToExpression(offset)
	if err != nil {
		return nil, err
	}

	nl, ok := e.(*expression.Literal)
	if !ok || nl.Type() != sql.Int64 {
		return nil, errUnsupportedFeature("OFFSET with non-integer literal")
	}

	n, err := nl.Eval(nil)
	if err != nil {
		return nil, err
	}
	return plan.NewOffset(n.(int64), child), nil
}

func isAggregate(e sql.Expression) bool {
	switch v := e.(type) {
	case *expression.UnresolvedFunction:
		return v.IsAggregate
	case *expression.Alias:
		return isAggregate(v.Child)
	default:
		return false
	}
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
		return nil, errUnsupported(e)
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
		switch v.Type {
		case sqlparser.StrVal:
			return expression.NewLiteral(string(v.Val), sql.Text), nil
		case sqlparser.IntVal:
			//TODO: Use smallest integer representation and widen later.
			n, _ := strconv.ParseInt(string(v.Val), 10, 64)
			return expression.NewLiteral(n, sql.Int64), nil
		case sqlparser.HexVal:
			//TODO
			return nil, errUnsupported(v)
		default:
			//TODO
			return nil, errUnsupported(v)
		}
	case sqlparser.BoolVal:
		return expression.NewLiteral(bool(v), sql.Boolean), nil
	case *sqlparser.NullVal:
		return expression.NewLiteral(nil, sql.Null), nil
	case *sqlparser.ColName:
		//TODO: add handling of case sensitiveness.
		return expression.NewUnresolvedColumn(v.Name.Lowered()), nil
	case *sqlparser.FuncExpr:
		exprs, err := selectExprsToExpressions(v.Exprs)
		if err != nil {
			return nil, err
		}

		return expression.NewUnresolvedFunction(v.Name.Lowered(),
			v.IsAggregate(), exprs...), nil
	}
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
		return nil, errUnsupported(c)
	}
}

func comparisonExprToExpression(c *sqlparser.ComparisonExpr) (sql.Expression,
	error) {

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
		return nil, errUnsupportedFeature(c.Operator)
	case sqlparser.RegexpStr:
		return expression.NewRegexp(left, right), nil
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
		return nil, errUnsupported(e)
	case *sqlparser.StarExpr:
		//TODO: Add support for qualified start.
		return expression.NewStar(), nil
	case *sqlparser.AliasedExpr:
		expr, err := exprToExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		if e.As.String() == "" {
			return expr, nil
		}

		//TODO: Handle case-sensitiveness when needed.
		return expression.NewAlias(expr, e.As.Lowered()), nil
	}
}
