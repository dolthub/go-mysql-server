package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildWith(inScope *scope, with *ast.With) (outScope *scope) {
	// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
	// tree with the subqueries defined in the CTEs.

	// CTE resolution:
	// - pre-process, get the list of CTEs
	// - find uses of those CTEs in the regular query body
	// - replace references to the name with the subquery body
	// - avoid infinite recursion of CTE referencing itself

	// recursive CTE (more complicated)
	// push recursive half right, minimize recursive side
	// create *plan.RecursiveCte node
	// replace recursive references of cte name with *plan.RecursiveTable

	outScope = inScope.push()

	for _, cte := range with.Ctes {
		cte, ok := cte.(*ast.CommonTableExpr)
		if !ok {
			b.handleErr(sql.ErrUnsupportedFeature.New(fmt.Sprintf("Unsupported type of common table expression %T", cte)))
		}

		ate := cte.AliasedTableExpr
		sq, ok := ate.Expr.(*ast.Subquery)
		if !ok {
			b.handleErr(sql.ErrUnsupportedFeature.New(fmt.Sprintf("Unsupported type of common table expression %T", ate.Expr)))
		}

		if with.Recursive {
			switch n := sq.Select.(type) {
			case *ast.Union:
				b.buildRecursiveCte(outScope, n, ate.As.String(), columnsToStrings(cte.Columns))
			default:
				b.buildCte(outScope, ate, ate.As.String(), columnsToStrings(cte.Columns))
			}
		} else {
			b.buildCte(outScope, ate, ate.As.String(), columnsToStrings(cte.Columns))
		}
	}
	return
}

func (b *PlanBuilder) buildCte(inScope *scope, e ast.TableExpr, name string, columns []string) {
	cteScope := b.buildDataSource(inScope, e)
	b.renameSource(cteScope, name, columns)
	switch n := cteScope.node.(type) {
	case *plan.SubqueryAlias:
		cteScope.node = n.WithColumns(columns)
	}
	inScope.addCte(name, cteScope)
}

func (b *PlanBuilder) buildRecursiveCte(inScope *scope, union *ast.Union, name string, columns []string) {
	l, r := splitRecursiveCteUnion(name, union)
	if r == nil {
		// not recursive
		cteScope := b.buildSelectStmt(inScope, union)
		b.renameSource(cteScope, name, columns)
		inScope.addCte(name, cteScope)
		return
	}

	// resolve non-recusive portion
	leftScope := b.buildSelectStmt(inScope, l)

	// schema for non-recursive portion => recursive table
	var rTable *plan.RecursiveTable
	var rInit sql.Node
	var recSch sql.Schema
	cteScope := leftScope.replace()
	{
		rInit = leftScope.node
		recSch = make(sql.Schema, len(rInit.Schema()))
		for i, c := range rInit.Schema() {
			newC := c.Copy()
			if len(columns) > 0 {
				newC.Name = columns[i]
			}
			newC.Source = name
			// the recursive part of the CTE may produce wider types than the left/non-recursive part
			// we need to promote the type of the left part, so the final schema is the widest possible type
			newC.Type = newC.Type.Promote()
			recSch[i] = newC

		}

		for _, c := range leftScope.cols {
			cteScope.newColumn(c)
		}
		b.renameSource(cteScope, name, columns)

		rTable = plan.NewRecursiveTable(name, recSch)
		cteScope.node = rTable
	}

	rightInScope := inScope.replace()
	rightInScope.addCte(name, cteScope)
	rightScope := b.buildSelectStmt(rightInScope, r)

	distinct := union.Type != ast.UnionAllStr
	limit := b.buildLimit(inScope, union.Limit)

	orderByScope := b.analyzeOrderBy(rightInScope, inScope, union.OrderBy)
	b.buildOrderBy(rightInScope, orderByScope)

	rcte := plan.NewRecursiveCte(rInit, rightScope.node, name, columns, distinct, limit, nil)
	rcte = rcte.WithSchema(recSch).WithWorking(rTable)
	rightScope.node = plan.NewSubqueryAlias(name, "", rcte).WithColumns(columns)
	b.renameSource(rightScope, name, columns)
	inScope.addCte(name, rightScope)
}

// splitRecursiveCteUnion distinguishes between recursive and non-recursive
// portions of a recursive CTE. We walk a left deep tree of unions downwards
// as far as the right scope references the recursive binding. A subquery
// alias or a non-recursive right scope terminates the walk. We transpose all
// recursive right scopes into a new union tree, returning separate initial
// and recursive trees. If the node is not a recursive union, the returned
// right node will be nil.
//
// todo(max): better error messages to differentiate between syntax errors
// "should have one or more non-recursive query blocks followed by one or more recursive ones"
// "the recursive table must be referenced only once, and not in any subquery"
func splitRecursiveCteUnion(name string, n ast.SelectStatement) (ast.SelectStatement, ast.SelectStatement) {
	union, ok := n.(*ast.Union)
	if !ok {
		return n, nil
	}

	if !hasTable(name, union.Right) {
		return n, nil
	}

	l, r := splitRecursiveCteUnion(name, union.Left)
	if r == nil {
		return union.Left, union.Right
	}

	return l, &ast.Union{
		Type:    union.Type,
		Left:    r,
		Right:   union.Right,
		OrderBy: union.OrderBy,
		With:    union.With,
		Limit:   union.Limit,
		Lock:    union.Lock,
	}
}

func hasTable(name string, s ast.SelectStatement) bool {
	var found bool
	ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch t := (node).(type) {
		case *ast.AliasedTableExpr:
			switch e := t.Expr.(type) {
			case ast.TableName:
				if strings.ToLower(e.Name.String()) == name {
					found = true
				}
			}
		}
		return true, nil
	}, s)
	return found
}
