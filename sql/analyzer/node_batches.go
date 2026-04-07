package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func isSubqeryExpr(expr sql.Expression) bool {
	switch e := expr.(type) {
	case *plan.Subquery:
		return true
	case *plan.InSubquery:
		return true
	case *plan.ExistsSubquery:
		return true
	//case *expression.Alias:
	//	return isSubqeryExpr(e.Child)
	case expression.UnaryExpression:
		return isSubqeryExpr(e.UnaryChild())
	case expression.BinaryExpression:
		return isSubqeryExpr(e.Left()) || isSubqeryExpr(e.Right())
	default:
		return false
	}
}

func isSimpleSelect(proj *plan.Project) bool {
	child := proj.Child
	switch c := child.(type) {
	case *plan.Filter:
		if isSubqeryExpr(c.Expression) {
			return false
		}
		child = c.Child
	default:
	}
	for _, expr := range proj.Projections {
		if isSubqeryExpr(expr) {
			return false
		}
	}
	if _, isResTbl := child.(*plan.ResolvedTable); !isResTbl {
		return false
	}
	return true
}

// getBatchesForNode returns a partial analyzer ruleset for simple node
// types that require little prior validation before execution.
func getBatchesForNode(scope *plan.Scope, node sql.Node) ([]*Batch, bool) {
	switch n := node.(type) {
	case *plan.Commit:
		return nil, true
	case *plan.StartTransaction:
		return nil, true
	case *plan.InsertInto:
		if n.LiteralValueSource {
			return []*Batch{
				{
					Desc:       "alwaysBeforeDefault",
					Iterations: 1,
					Rules:      AlwaysBeforeDefault,
				},
				{
					Desc:       "simpleInsert",
					Iterations: 1,
					Rules: []Rule{
						{
							Id:    applyForeignKeysId,
							Apply: applyForeignKeys,
						},
						{
							Id:    validateReadOnlyDatabaseId,
							Apply: validateReadOnlyDatabase,
						},
						{
							Id:    validateReadOnlyTransactionId,
							Apply: validateReadOnlyTransaction,
						},
					},
				},
				{
					Desc:       "onceAfterAll",
					Iterations: 1,
					Rules:      OnceAfterAll,
				},
			}, true
		}
	case *plan.Update:
		if n.HasSingleRel && !n.IsJoin {
			return []*Batch{
				{
					Desc:       "alwaysBeforeDefault",
					Iterations: 1,
					Rules:      AlwaysBeforeDefault,
				},
				{
					Desc:       "simpleUpdate",
					Iterations: 1,
					Rules: []Rule{
						{
							Id:    validateReadOnlyDatabaseId,
							Apply: validateReadOnlyDatabase,
						},
						{
							Id:    validateReadOnlyTransactionId,
							Apply: validateReadOnlyTransaction,
						},
						{
							Id:    applyForeignKeysId,
							Apply: applyForeignKeys,
						},
						{
							Id:    optimizeJoinsId,
							Apply: optimizeJoins,
						},
						{
							Id:    applyHashInId,
							Apply: applyHashIn,
						},
					},
				},
				{
					Desc:       "onceAfterAll",
					Iterations: 1,
					Rules:      OnceAfterAll,
				},
			}, true
		}
	case *plan.DeleteFrom:
		if !n.HasExplicitTargets() && n.RefsSingleRel {
			return []*Batch{
				{
					Desc:       "alwaysBeforeDefault",
					Iterations: 1,
					Rules:      AlwaysBeforeDefault,
				},
				{
					Desc:       "simpleDelete",
					Iterations: 1,
					Rules: []Rule{
						{
							Id:    validateReadOnlyDatabaseId,
							Apply: validateReadOnlyDatabase,
						},
						{
							Id:    validateReadOnlyTransactionId,
							Apply: validateReadOnlyTransaction,
						},
						{
							Id:    processTruncateId,
							Apply: processTruncate,
						},
						{
							Id:    applyForeignKeysId,
							Apply: applyForeignKeys,
						},
						{
							Id:    optimizeJoinsId,
							Apply: optimizeJoins,
						},
						{
							Id:    applyHashInId,
							Apply: applyHashIn,
						},
					},
				},
				{
					Desc:       "onceAfterAll",
					Iterations: 1,
					Rules:      OnceAfterAll,
				},
			}, true
		}
	case *plan.Project:
		// Scope checks here are to prevent this from applying to subqueries
		if (scope == nil || scope.RecursionDepth() < 1) && isSimpleSelect(n) {
			return []*Batch{
				{
					Desc:       "onceBeforeDefault",
					Iterations: 1,
					Rules: []Rule{
						{Id: simplifyFiltersId, Apply: simplifyFilters},
						{Id: pushNotFiltersId, Apply: pushNotFilters},
					},
				},
				{
					Desc:       "alwaysBeforeDefault",
					Iterations: 1,
					Rules:      AlwaysBeforeDefault,
				},
				{
					Desc:       "defaultRules",
					Iterations: 1,
					Rules: []Rule{
						{Id: validateStarExpressionsId, Apply: validateStarExpressions},
						{Id: pruneTablesId, Apply: pruneTables},
					},
				},
				{
					Desc:       "simpleSelect",
					Iterations: 1,
					Rules: []Rule{
						{Id: stripTableNameInDefaultsId, Apply: stripTableNamesFromColumnDefaults},
						{Id: pushFiltersId, Apply: pushFilters},
						{Id: optimizeJoinsId, Apply: optimizeJoins},
						{Id: eraseProjectionId, Apply: eraseProjection},
						{Id: applyHashInId, Apply: applyHashIn},
						{Id: assignRoutinesId, Apply: assignRoutines},
					},
				},
				{
					Desc:       "onceAfterAll",
					Iterations: 1,
					Rules: []Rule{
						{Id: assignExecIndexesId, Apply: assignExecIndexes},
						{Id: QuoteDefaultColumnValueNamesId, Apply: quoteDefaultColumnValueNames},
						{Id: TrackProcessId, Apply: trackProcess},
					},
				},
				{
					// TODO: can skip resolveInsertRows here probably
					Desc:       "simpleSelectValidationRules",
					Iterations: 1,
					Rules: []Rule{
						{Id: ValidateOperandsId, Apply: validateOperands},
					},
				},
			}, true
		}
	default:
	}

	return nil, false
}
