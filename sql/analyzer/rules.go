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
	{"validate_offset_and_limit", validateLimitAndOffset},
	{"validate_create_table", validateCreateTable},
	{"load_stored_procedures", loadStoredProcedures},
	{"resolve_variables", resolveVariables},
	{"resolve_set_variables", resolveSetVariables},
	{"resolve_views", resolveViews},
	{"lift_common_table_expressions", liftCommonTableExpressions},
	{"resolve_common_table_expressions", resolveCommonTableExpressions},
	{"lift_recursive_ctes", liftRecursiveCte},
	{"resolve_databases", resolveDatabases},
	{"resolve_tables", resolveTables},
	{"set_target_schemas", setTargetSchemas},
	{"resolve_create_like", resolveCreateLike},
	{"parse_column_defaults", parseColumnDefaults},
	{"resolve_drop_constraint", resolveDropConstraint},
	{"validate_drop_constraint", validateDropConstraint},
	{"load_check_constraints", loadChecks},
	{"resolve_create_select", resolveCreateSelect},
	{"resolve_subqueries", resolveSubqueries},
	{"resolve_unions", resolveUnions},
	{"resolve_describe_query", resolveDescribeQuery},
	{"check_unique_table_names", checkUniqueTableNames},
	{"resolve_declarations", resolveDeclarations},
	{"validate_create_trigger", validateCreateTrigger},
	{"validate_create_procedure", validateCreateProcedure},
	{"assign_info_schema", assignInfoSchema},
	{"validate_read_only_database", validateReadOnlyDatabase},
	{"validate_read_only_transaction", validateReadOnlyTransaction},
	{"validate_database_set", validateDatabaseSet},
	{"check_privileges", checkPrivileges}, // Ensure that checking privileges happens after db & table resolution
}

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{"resolve_natural_joins", resolveNaturalJoins},
	{"resolve_orderby_literals", resolveOrderByLiterals},
	{"resolve_functions", resolveFunctions},
	{"flatten_table_aliases", flattenTableAliases},
	{"pushdown_sort", pushdownSort},
	{"pushdown_groupby_aliases", pushdownGroupByAliases},
	{"pushdown_subquery_alias_filters", pushdownSubqueryAliasFilters},
	{"qualify_columns", qualifyColumns},
	{"resolve_columns", resolveColumns},
	{"resolve_column_defaults", resolveColumnDefaults},
	{"validate_check_constraint", validateCheckConstraints},
	{"resolve_bareword_set_variables", resolveBarewordSetVariables},
	{"expand_stars", expandStars},
	{"resolve_having", resolveHaving},
	{"merge_union_schemas", mergeUnionSchemas},
	{"flatten_aggregation_exprs", flattenAggregationExpressions},
	{"reorder_projection", reorderProjection},
	{"resolve_subquery_exprs", resolveSubqueryExpressions},
	{"replace_cross_joins", replaceCrossJoins},
	{"move_join_conds_to_filter", moveJoinConditionsToFilter},
	{"eval_filter", evalFilter},
	{"optimize_distinct", optimizeDistinct},
}

// OnceAfterDefault contains the rules to be applied just once after the
// DefaultRules.
var OnceAfterDefault = []Rule{
	{"finalize_subqueries", finalizeSubqueries},
	{"finalize_unions", finalizeUnions},
	{"load_triggers", loadTriggers},
	{"process_truncate", processTruncate},
	{"validate_alter_column", validateAlterColumn},
	{"resolve_generators", resolveGenerators},
	{"remove_unnecessary_converts", removeUnnecessaryConverts},
	{"assign_catalog", assignCatalog},
	{"prune_columns", pruneColumns},
	{"optimize_joins", constructJoinPlan},
	{"pushdown_filters", pushdownFilters},
	{"subquery_indexes", applyIndexesFromOuterScope},
	{"in_subquery_indexes", applyIndexesForSubqueryComparisons},
	{"pushdown_projections", pushdownProjections},
	{"set_join_scope_len", setJoinScopeLen},
	{"erase_projection", eraseProjection},
	{"insert_topn", insertTopNNodes},
	// One final pass at analyzing subqueries to handle rewriting field indexes after changes to outer scope by
	// previous rules.
	{"resolve_subquery_exprs", resolveSubqueryExpressions},
	{"cache_subquery_results", cacheSubqueryResults},
	{"cache_subquery_aliases_in_joins", cacheSubqueryAlisesInJoins},
	{"apply_hash_lookups", applyHashLookups},
	{"apply_hash_in", applyHashIn},
	{"resolve_insert_rows", resolveInsertRows},
	{"apply_triggers", applyTriggers},
	{"apply_procedures", applyProcedures},
	{"modify_update_expressions_for_join", modifyUpdateExpressionsForJoin},
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
