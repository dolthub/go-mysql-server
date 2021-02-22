// Copyright 2020-2021 Dolthub, Inc.
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

package analyzer

import (
	errors "gopkg.in/src-d/go-errors.v1"
)

// OnceBeforeDefault contains the rules to be applied just once before the
// DefaultRules.
var OnceBeforeDefault = []Rule{
	{"resolve_views", resolveViews},
	{"resolve_tables", resolveTables},
	{"resolve_set_variables", resolveSetVariables},
	{"resolve_create_like", resolveCreateLike},
	{"resolve_subqueries", resolveSubqueries},
	{"resolve_unions", resolveUnions},
	{"resolve_describe_query", resolveDescribeQuery},
	{"check_unique_table_names", checkUniqueTableNames},
	{"validate_create_trigger", validateCreateTrigger},
	{"validate_stored_procedure", validateStoredProcedure},
}

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{"resolve_natural_joins", resolveNaturalJoins},
	{"resolve_orderby_literals", resolveOrderByLiterals},
	{"pushdown_sort", pushdownSort},
	{"pushdown_groupby_aliases", pushdownGroupByAliases},
	{"qualify_columns", qualifyColumns},
	{"resolve_columns", resolveColumns},
	{"resolve_bareword_set_variables", resolveBarewordSetVariables},
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
	{"load_triggers", loadTriggers},
	{"process_truncate", processTruncate},
	{"resolve_column_defaults", resolveColumnDefaults},
	{"resolve_generators", resolveGenerators},
	{"remove_unnecessary_converts", removeUnnecessaryConverts},
	{"assign_catalog", assignCatalog},
	{"assign_info_schema", assignInfoSchema},
	{"prune_columns", pruneColumns},
	{"optimize_joins", constructJoinPlan},
	{"pushdown_filters", pushdownFilters},
	{"subquery_indexes", applyIndexesFromOuterScope},
	{"in_subquery_indexes", applyIndexesForSubqueryComparisons},
	{"pushdown_projections", pushdownProjections},
	{"erase_projection", eraseProjection},
	// One final pass at analyzing subqueries to handle rewriting field indexes after changes to outer scope by
	// previous rules.
	{"resolve_subquery_exprs", resolveSubqueryExpressions},
	{"cache_subquery_results", cacheSubqueryResults},
	{"resolve_insert_rows", resolveInsertRows},
	{"apply_triggers", applyTriggers},
	{"apply_procedures", applyProcedures},
	{"apply_row_update_accumulators", applyUpdateAccumulators},
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
