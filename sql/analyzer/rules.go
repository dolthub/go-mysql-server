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

//go:generate stringer -type=RuleId

type RuleId int

const (
	// once before
	validateOffsetAndLimitId RuleId = iota
	validateCreateTableId
	validateExprSemId
	resolveVariablesId
	resolveNamedWindowsId
	resolveSetVariablesId
	resolveViewsId
	liftCtesId
	resolveCtesId
	liftRecursiveCtesId
	resolveDatabasesId
	resolveTablesId
	loadStoredProceduresId
	validateDropTablesId
	setTargetSchemasId
	resolveCreateLikeId
	parseColumnDefaultsId
	resolveDropConstraintId
	validateDropConstraintId
	loadCheckConstraintsId
	resolveCreateSelectId
	resolveSubqueriesId
	resolveUnionsId
	resolveDescribeQueryId
	checkUniqueTableNamesId
	resolveTableFunctionsId
	resolveDeclarationsId
	validateCreateTriggerId
	validateCreateProcedureId
	loadInfoSchemaId
	validateReadOnlyDatabaseId
	validateReadOnlyTransactionId
	validateDatabaseSetId
	validatePriviledgesId
	stripDecorationsId
	unresolveTablesId
	validateJoinComplexityId

	// default
	resolveNaturalJoinsId
	resolveOrderbyLiteralsId
	resolveFunctionsId
	flattenTableAliasesId
	pushdownSortId
	pushdownGroupbyAliasesId
	pushdownSubqueryAliasFiltersId
	qualifyColumnsId
	resolveColumnsId
	resolveColumnDefaultsId
	validateCheckConstraintId
	resolveBarewordSetVariablesId
	expandStarsId
	resolveHavingId
	mergeUnionSchemasId
	flattenAggregationExprsId
	reorderProjectionId
	resolveSubqueryExprsId
	replaceCrossJoinsId
	moveJoinCondsToFilterId
	evalFilterId
	optimizeDistinctId

	// after default
	finalizeSubqueriesId
	finalizeUnionsId
	loadTriggersId
	processTruncateId
	validateAlterColumnId
	resolveGeneratorsId
	removeUnnecessaryConvertsId
	assignCatalogId
	pruneColumnsId
	optimizeJoinsId
	pushdownFiltersId
	subqueryIndexesId
	inSubqueryIndexesId
	pushdownProjectionsId
	setJoinScopeLenId
	eraseProjectionId
	insertTopNId
	cacheSubqueryResultsId
	cacheSubqueryAliasesInJoinsId
	applyHashLookupsId
	applyHashInId
	resolveInsertRowsId
	applyTriggersId
	applyProceduresId
	assignRoutinesId
	modifyUpdateExprsForJoinId
	applyRowUpdateAccumulatorsId
	applyFKsId

	// validate
	validateResolvedId
	validateOrderById
	validateGroupById
	validateSchemaSourceId
	validateIndexCreationId
	validateOperandsId
	validateCaseResultTypesId
	validateIntervalUsageId
	validateExplodeUsageId
	validateSubqueryColumnsId
	validateUnionSchemasMatchId
	validateAggregationsId

	// after all
	CommittingTransactionId
	TrackProcessId
	parallelizeId
	clearWarningsId
)

// OnceBeforeDefault contains the rules to be applied just once before the
// DefaultRules.
var OnceBeforeDefault = []Rule{
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
	{loadStoredProceduresId, loadStoredProcedures}, // Ensure that loading procedures happens after table resolution
	{validateDropTablesId, validateDropTables},
	{setTargetSchemasId, setTargetSchemas},
	{resolveCreateLikeId, resolveCreateLike},
	{parseColumnDefaultsId, parseColumnDefaults},
	{resolveDropConstraintId, resolveDropConstraint},
	{validateDropConstraintId, validateDropConstraint},
	{loadCheckConstraintsId, loadChecks},
	{resolveCreateSelectId, resolveCreateSelect},
	{resolveSubqueriesId, resolveSubqueries},
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
	{validatePriviledgesId, validatePrivileges}, // Ensure that checking privileges happens after db, table  & table function resolution
	{stripDecorationsId, stripDecorations},
	{validateJoinComplexityId, validateJoinComplexity},
	{unresolveTablesId, unresolveTables},
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
	{resolveColumnsId, resolveColumns},
	{resolveColumnDefaultsId, resolveColumnDefaults},
	{validateCheckConstraintId, validateCheckConstraints},
	{resolveBarewordSetVariablesId, resolveBarewordSetVariables},
	{expandStarsId, expandStars},
	{resolveHavingId, resolveHaving},
	{mergeUnionSchemasId, mergeUnionSchemas},
	{flattenAggregationExprsId, flattenAggregationExpressions},
	{reorderProjectionId, reorderProjection},
	{resolveSubqueryExprsId, resolveSubqueryExpressions},
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
	{validateAlterColumnId, validateAlterColumn},
	{resolveGeneratorsId, resolveGenerators},
	{removeUnnecessaryConvertsId, removeUnnecessaryConverts},
	{assignCatalogId, assignCatalog},
	{pruneColumnsId, pruneColumns},
	{optimizeJoinsId, constructJoinPlan},
	{pushdownFiltersId, pushdownFilters},
	{subqueryIndexesId, applyIndexesFromOuterScope},
	{inSubqueryIndexesId, applyIndexesForSubqueryComparisons},
	{pushdownProjectionsId, pushdownProjections},
	{setJoinScopeLenId, setJoinScopeLen},
	{eraseProjectionId, eraseProjection},
	{insertTopNId, insertTopNNodes},
	// One final pass at analyzing subqueries to handle rewriting field indexes after changes to outer scope by
	// previous rules.
	{resolveSubqueryExprsId, resolveSubqueryExpressions},
	{cacheSubqueryResultsId, cacheSubqueryResults},
	{cacheSubqueryAliasesInJoinsId, cacheSubqueryAlisesInJoins},
	{applyHashLookupsId, applyHashLookups},
	{applyHashInId, applyHashIn},
	{resolveInsertRowsId, resolveInsertRows},
	{applyTriggersId, applyTriggers},
	{applyProceduresId, applyProcedures},
	{assignRoutinesId, assignRoutines},
	{modifyUpdateExprsForJoinId, modifyUpdateExpressionsForJoin},
	{applyRowUpdateAccumulatorsId, applyUpdateAccumulators},
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
	{validateCaseResultTypesId, validateCaseResultTypes},
	{validateIntervalUsageId, validateIntervalUsage},
	{validateExplodeUsageId, validateExplodeUsage},
	{validateSubqueryColumnsId, validateSubqueryColumns},
	{validateUnionSchemasMatchId, validateUnionSchemasMatch},
	{validateAggregationsId, validateAggregations},
}

// OnceAfterAll contains the rules to be applied just once after all other
// rules have been applied.
var OnceAfterAll = []Rule{
	{CommittingTransactionId, wrapAutocommitNode},
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
