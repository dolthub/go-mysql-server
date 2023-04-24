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
	{applyBinlogReplicaControllerId, applyBinlogReplicaController},
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
	{validateCreateProcedureId, validateCreateProcedure},
	{resolveCreateProcedureId, resolveCreateProcedure},
	{resolveDatabasesId, resolveDatabases},
	{resolveTablesId, resolveTables},
	{reresolveTablesId, reresolveTables},
	{setInsertColumnsId, setInsertColumns},
	{setTargetSchemasId, setTargetSchemas},
	{loadCheckConstraintsId, loadChecks},
	{resolveAlterColumnId, resolveAlterColumn},
	{validateDropTablesId, validateDropTables},
	{pruneDropTablesId, pruneDropTables},
	{resolveCreateLikeId, resolveCreateLike},
	{resolveAnalyzeTablesId, resolveAnalyzeTables},
	{assignCatalogId, assignCatalog},
	{parseColumnDefaultsId, parseColumnDefaults},
	{resolveDropConstraintId, resolveDropConstraint},
	{validateDropConstraintId, validateDropConstraint},
	{resolveCreateSelectId, resolveCreateSelect},
	{resolveSubqueriesId, resolveSubqueries},
	{setViewTargetSchemaId, setViewTargetSchema},
	{resolveUnionsId, resolveUnions},
	{resolveDescribeQueryId, resolveDescribeQuery},
	{disambiguateTableFunctionsId, disambiguateTableFunctions},
	{checkUniqueTableNamesId, validateUniqueTableNames},
	{resolveTableFunctionsId, resolveTableFunctions},
	{resolveDeclarationsId, resolveDeclarations},
	{validateCreateTriggerId, validateCreateTrigger},
	{loadInfoSchemaId, loadInfoSchema},
	{resolveColumnDefaultsId, resolveColumnDefaults},
	{validateColumnDefaultsId, validateColumnDefaults},
	{validateReadOnlyDatabaseId, validateReadOnlyDatabase},
	{validateReadOnlyTransactionId, validateReadOnlyTransaction},
	{validateDatabaseSetId, validateDatabaseSet},
	{validateDeleteFromId, validateDeleteFrom},
	{validatePrivilegesId, validatePrivileges}, // Ensure that checking privileges happens after db, table  & table function resolution
}

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{resolveNaturalJoinsId, resolveNaturalJoins},
	{qualifyColumnsId, qualifyColumns},
	{resolveOrderbyLiteralsId, resolveOrderByLiterals},
	{resolveFunctionsId, resolveFunctions},
	{validateStarExpressionsId, validateStarExpressions},
	{replaceCountStarId, replaceCountStar},
	{flattenTableAliasesId, flattenTableAliases},
	{pushdownSortId, pushdownSort},
	{pushdownGroupbyAliasesId, pushdownGroupByAliases},
	{pushdownSubqueryAliasFiltersId, pushdownSubqueryAliasFilters},
	{pruneTablesId, pruneTables},
	{resolveColumnsId, resolveColumns},
	{validateCheckConstraintId, validateCheckConstraints},
	{expandStarsId, expandStars},
	{transposeRightJoinsId, transposeRightJoins},
	{resolveHavingId, resolveHaving},
	{mergeUnionSchemasId, mergeUnionSchemas},
	{flattenAggregationExprsId, flattenAggregationExpressions},
	{reorderProjectionId, reorderProjection},
	{resolveSubqueriesId, resolveSubqueries},
	{resolveBarewordSetVariablesId, resolveBarewordSetVariables},
	{replaceCrossJoinsId, replaceCrossJoins},
	{moveJoinCondsToFilterId, moveJoinConditionsToFilter},
	{evalFilterId, simplifyFilters},
	{optimizeDistinctId, optimizeDistinct},
}

// OnceAfterDefault contains the rules to be applied just once after the
// DefaultRules.
var OnceAfterDefault = []Rule{
	{hoistOutOfScopeFiltersId, hoistOutOfScopeFilters},
	{transformJoinApplyId, transformJoinApply},
	{hoistSelectExistsId, hoistSelectExists},
	{finalizeUnionsId, finalizeUnions},
	{loadTriggersId, loadTriggers},
	{loadEventsId, loadEvents},
	{processTruncateId, processTruncate},
	{removeUnnecessaryConvertsId, removeUnnecessaryConverts},
	{stripTableNameInDefaultsId, stripTableNamesFromColumnDefaults},
	{foldEmptyJoinsId, foldEmptyJoins},
	{optimizeJoinsId, constructJoinPlan},
	{pushdownFiltersId, pushdownFilters},
	{pruneColumnsId, pruneColumns},
	{finalizeSubqueriesId, finalizeSubqueries},
	{subqueryIndexesId, applyIndexesFromOuterScope},
	{replaceSortPkId, replacePkSort},
	{setJoinScopeLenId, setJoinScopeLen},
	{eraseProjectionId, eraseProjection},
	{insertTopNId, insertTopNNodes},
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
	{cacheSubqueryResultsId, cacheSubqueryResults},
	{cacheSubqueryAliasesInJoinsId, cacheSubqueryAliasesInJoins},
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
