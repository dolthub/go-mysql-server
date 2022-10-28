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
	{setInsertColumnsId, setInsertColumns},
	{setTargetSchemasId, setTargetSchemas},
	{loadCheckConstraintsId, loadChecks},
	{resolveAlterColumnId, resolveAlterColumn},
	{validateDropTablesId, validateDropTables},
	{resolveCreateLikeId, resolveCreateLike},
	{assignCatalogId, assignCatalog},
	{parseColumnDefaultsId, parseColumnDefaults},
	{resolveDropConstraintId, resolveDropConstraint},
	{validateDropConstraintId, validateDropConstraint},
	{resolveCreateSelectId, resolveCreateSelect},
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
	{resolveColumnDefaultsId, resolveColumnDefaults},
	{validateColumnDefaultsId, validateColumnDefaults},
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
	{validateCheckConstraintId, validateCheckConstraints},
	{resolveBarewordSetVariablesId, resolveBarewordSetVariables},
	{expandStarsId, expandStars},
	{transposeRightJoinsId, transposeRightJoins},
	{resolveHavingId, resolveHaving},
	{mergeUnionSchemasId, mergeUnionSchemas},
	{flattenAggregationExprsId, flattenAggregationExpressions},
	{reorderProjectionId, reorderProjection},
	{resolveSubqueriesId, resolveSubqueries},
	{replaceCrossJoinsId, replaceCrossJoins},
	{moveJoinCondsToFilterId, moveJoinConditionsToFilter},
	{evalFilterId, simplifyFilters},
	{optimizeDistinctId, optimizeDistinct},
}

// OnceAfterDefault contains the rules to be applied just once after the
// DefaultRules.
var OnceAfterDefault = []Rule{
	{hoistSelectExistsId, hoistSelectExists},
	{finalizeSubqueriesId, finalizeSubqueries},
	{finalizeUnionsId, finalizeUnions},
	{loadTriggersId, loadTriggers},
	{processTruncateId, processTruncate},
	{removeUnnecessaryConvertsId, removeUnnecessaryConverts},
	{stripTableNameInDefaultsId, stripTableNamesFromColumnDefaults},
	{hoistSelectExistsId, hoistSelectExists},
	{optimizeJoinsId, constructJoinPlan},
	{pushdownFiltersId, pushdownFilters},
	{pruneColumnsId, pruneColumns},
	{subqueryIndexesId, applyIndexesFromOuterScope},
	{inSubqueryIndexesId, applyIndexesForSubqueryComparisons},
	{replaceSortPkId, replacePkSort},
	{setJoinScopeLenId, setJoinScopeLen},
	{eraseProjectionId, eraseProjection},
	{insertTopNId, insertTopNNodes},
	{cacheSubqueryResultsId, cacheSubqueryResults},
	{cacheSubqueryAliasesInJoinsId, cacheSubqueryAliasesInJoins},
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
