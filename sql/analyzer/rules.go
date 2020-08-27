package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v1"
)

// OnceBeforeDefault contains the rules to be applied just once before the
// DefaultRules.
var OnceBeforeDefault = []Rule{
	{"resolve_views", resolveViews},
	{"resolve_tables", resolveTables},
	{"resolve_subqueries", resolveSubqueries},
	{"check_aliases", checkAliases},
}

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{"resolve_natural_joins", resolveNaturalJoins},
	{"resolve_orderby_literals", resolveOrderByLiterals},
	{"pushdown_sort", pushdownSort},
	{"pushdown_groupby_aliases", pushdownGroupByAliases},
	{"qualify_columns", qualifyColumns},
	{"resolve_columns", resolveColumns},
	{"resolve_database", resolveDatabase},
	{"expand_stars", expandStars},
	{"resolve_functions", resolveFunctions},
	{"resolve_having", resolveHaving},
	{"merge_union_schemas", mergeUnionSchemas},
	{"flatten_group_by_aggregations", flattenGroupByAggregations},
	{"reorder_projection", reorderProjection},
	{"resolve_subquery_exprs", resolveSubqueryExpressions},
	{"move_join_conds_to_filter", moveJoinConditionsToFilter},
	{"eval_filter", evalFilter},
	{"optimize_distinct", optimizeDistinct},
}

// OnceAfterDefault contains the rules to be applied just once after the
// DefaultRules.
var OnceAfterDefault = []Rule{
	{"resolve_column_defaults", resolveColumnDefaults},
	{"resolve_generators", resolveGenerators},
	{"remove_unnecessary_converts", removeUnnecessaryConverts},
	{"assign_catalog", assignCatalog},
	{"assign_info_schema", assignInfoSchema},
	{"prune_columns", pruneColumns},
	{"convert_dates", convertDates},
	{"pushdown_filters", pushdownFilters},
	{"optimize_joins", optimizeJoins},
	{"erase_projection", eraseProjection},
	// One final pass at analyzing subqueries to handle rewriting field indexes after changes to outer scope by
	// previous rules.
	{"resolve_subquery_exprs", resolveSubqueryExpressions},
	{"cache_subquery_results", cacheSubqueryResults},
}

// OnceAfterAll contains the rules to be applied just once after all other
// rules have been applied.
var OnceAfterAll = []Rule{
	{"track_process", trackProcess},
	{"parallelize", parallelize},
	{"clear_warnings", clearWarnings},
}

var (
	// ErrFieldMissing is returned when the field is not on the schema.
	ErrFieldMissing = errors.NewKind("field %q is not on schema")
	// ErrOrderByColumnIndex is returned when in an order clause there is a
	// column that is unknown.
	ErrOrderByColumnIndex = errors.NewKind("unknown column %d in order by clause")
)
