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
	{applyDefaultSelectLimitId, applyDefaultSelectLimit},
	{validateOffsetAndLimitId, validateLimitAndOffset},
	{validateCreateTableId, validateCreateTable},
	{validateExprSemId, validateExprSem},
	{resolveVariablesId, resolveVariables},
	{resolveNamedWindowsId, replaceNamedWindows},
	{resolveSetVariablesId, resolveSetVariables},
	{resolveViewsId, resolveViews},
	{liftCtesId, hoistCommonTableExpressions},
	{resolveCtesId, resolveCommonTableExpressions},
	{liftRecursiveCtesId, hoistRecursiveCte},
	{resolveDatabasesId, resolveDatabases},
	{resolveTablesId, resolveTables},
	{reresolveTablesId, reresolveTables},
	{setTargetSchemasId, setTargetSchemas},
	{loadCheckConstraintsId, loadChecks},
	{resolveAlterColumnId, resolveAlterColumn},
	{validateDropTablesId, validateDropTables},
	{resolveCreateLikeId, resolveCreateLike},
	{parseColumnDefaultsId, parseColumnDefaults},
	{resolveDropConstraintId, resolveDropConstraint},
	{validateDropConstraintId, validateDropConstraint},
	{resolveCreateSelectId, resolveCreateSelect},
	// We still need to do an initial round of resolveSubqueries up front and try to resolve as much as we can, since
	// analyzing the top-level scope depends on knowing the types/schemas for the subscopes. Before, we could resolve
	// all the SubqueryAliases
	{resolveSubqueriesId, resolveSubqueries},
	{setViewTargetSchemaId, setViewTargetSchema},
	{resolveUnionsId, resolveUnions},
	{resolveDescribeQueryId, resolveDescribeQuery},
	{checkUniqueTableNamesId, validateUniqueTableNames},
	{resolveTableFunctionsId, resolveTableFunctions},
	{resolveDeclarationsId, resolveDeclarations},
	{validateCreateTriggerId, validateCreateTrigger},
	{validateCreateProcedureId, validateCreateProcedure},
	{loadInfoSchemaId, loadInfoSchema},
	{validateReadOnlyDatabaseId, validateReadOnlyDatabase},
	{validateReadOnlyTransactionId, validateReadOnlyTransaction},
	{validateDatabaseSetId, validateDatabaseSet},
	{validatePrivilegesId, validatePrivileges}, // Ensure that checking privileges happens after db, table  & table function resolution
}

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{resolveNaturalJoinsId, resolveNaturalJoins},
	{resolveOrderbyLiteralsId, resolveOrderByLiterals},
	{resolveFunctionsId, resolveFunctions},
	{flattenTableAliasesId, flattenTableAliases},
	{pushdownSortId, pushdownSort},
	{pushdownGroupbyAliasesId, pushdownGroupByAliases},
	{pushdownSubqueryAliasFiltersId, pushdownSubqueryAliasFilters},
	{qualifyColumnsId, qualifyColumns},
	{pruneTablesId, pruneTables},
	{resolveColumnsId, resolveColumns},
	{resolveColumnDefaultsId, resolveColumnDefaults},
	{validateCheckConstraintId, validateCheckConstraints},
	{resolveBarewordSetVariablesId, resolveBarewordSetVariables},
	{expandStarsId, expandStars},
	{resolveHavingId, resolveHaving},
	{mergeUnionSchemasId, mergeUnionSchemas},
	{flattenAggregationExprsId, flattenAggregationExpressions},
	{reorderProjectionId, reorderProjection},
	// TODO: If SubqueryAliases have outer scope access now, do they need to be analyzed as part of the default rules now?
	//       Seems like we need to combine resolveSubqueires and resolveSubqueryExpressions in order to handle scopes properly
	{resolveSubqueriesId, resolveSubqueries},
	// TODO: We shouldn't need this rule AND resolveSubqueries... resolveSubqueries is supposed to be doing everything,
	//       but it seems like it's not working.
	//       Adding back resolveSubqueryExpressions makes MANY more TestQueries pass, but we're still failing on
	//
	//{resolveSubqueryExprsId, resolveSubqueryExpressions}, // TODO: This should be resolveSubqueries?

	// TODO: My theory is that between the transforms and re-running analyze, the changes to the nodes are getting
	//       lost somewhere.
	{replaceCrossJoinsId, replaceCrossJoins},
	{moveJoinCondsToFilterId, moveJoinConditionsToFilter},
	{evalFilterId, simplifyFilters},
	{optimizeDistinctId, optimizeDistinct},
}

// OnceAfterDefault contains the rules to be applied just once after the
// DefaultRules.
var OnceAfterDefault = []Rule{
	{finalizeSubqueriesId, finalizeSubqueries},
	{finalizeUnionsId, finalizeUnions},
	{loadTriggersId, loadTriggers},
	{processTruncateId, processTruncate},
	{removeUnnecessaryConvertsId, removeUnnecessaryConverts},
	{assignCatalogId, assignCatalog},
	{pruneColumnsId, pruneColumns},
	{optimizeJoinsId, constructJoinPlan},
	{pushdownFiltersId, pushdownFilters},
	{subqueryIndexesId, applyIndexesFromOuterScope},
	{inSubqueryIndexesId, applyIndexesForSubqueryComparisons},
	{replaceSortPkId, replacePkSort},
	{setJoinScopeLenId, setJoinScopeLen},
	{eraseProjectionId, eraseProjection},
	{insertTopNId, insertTopNNodes},
	// One final pass at analyzing subqueries to handle rewriting field indexes after changes to outer scope by
	// previous rules.
	//{resolveSubqueryExprsId, resolveSubqueryExpressions},
	// Switching to the resolveSubqueries rule, instead of resolveSubqueryExpressions, which probably
	// breaks all subquery query tests, since subquery expressions don't seem to be getting analyzed correctly.
	{resolveSubqueriesId, resolveSubqueries},

	{cacheSubqueryResultsId, cacheSubqueryResults},
	{cacheSubqueryAliasesInJoinsId, cacheSubqueryAlisesInJoins},
	{applyHashLookupsId, applyHashLookups},
	{applyHashInId, applyHashIn},
	{resolveInsertRowsId, resolveInsertRows},
	{resolvePreparedInsertId, resolvePreparedInsert},
	{applyTriggersId, applyTriggers},
	{applyProceduresId, applyProcedures},
	{assignRoutinesId, assignRoutines},
	{modifyUpdateExprsForJoinId, modifyUpdateExpressionsForJoin},
	{applyRowUpdateAccumulatorsId, applyUpdateAccumulators},
	{wrapWithRollbackId, wrapWritesWithRollback},
	{applyFKsId, applyForeignKeys},
}

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []Rule{
	{validateResolvedId, validateIsResolved},
	{validateOrderById, validateOrderBy},
	{validateGroupById, validateGroupBy},
	{validateSchemaSourceId, validateSchemaSource},
	{validateIndexCreationId, validateIndexCreation},
	{validateOperandsId, validateOperands},
	{validateIntervalUsageId, validateIntervalUsage},
	{validateSubqueryColumnsId, validateSubqueryColumns},
	{validateUnionSchemasMatchId, validateUnionSchemasMatch},
	{validateAggregationsId, validateAggregations},
}

// OnceAfterAll contains the rules to be applied just once after all other
// rules have been applied.
var OnceAfterAll = []Rule{
	{AutocommitId, addAutocommitNode},
	{TrackProcessId, trackProcess},
	{parallelizeId, parallelize},
	{clearWarningsId, clearWarnings},
}

var (
	// ErrFieldMissing is returned when the field is not on the schema.
	ErrFieldMissing = errors.NewKind("field %q is not on schema")
	// ErrOrderByColumnIndex is returned when in an order clause there is a
	// column that is unknown.
	ErrOrderByColumnIndex = errors.NewKind("unknown column %d in order by clause")
)
