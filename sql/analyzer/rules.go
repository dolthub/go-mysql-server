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

func init() {
	OnceAfterAll = []Rule{
		{assignExecIndexesId, assignExecIndexes},
		// resolveInsertRows inserts a projection wrapping values that cannot be seen by fixup
		{resolveInsertRowsId, resolveInsertRows},
		{applyTriggersId, applyTriggers},
		{applyProceduresId, applyProcedures},
		{applyRowUpdateAccumulatorsId, applyUpdateAccumulators},
		{wrapWithRollbackId, wrapWritesWithRollback},
		{inlineSubqueryAliasRefsId, inlineSubqueryAliasRefs},
		{cacheSubqueryAliasesInJoinsId, cacheSubqueryAliasesInJoins},

		{backtickDefaulColumnValueNamesId, backtickDefaultColumnValueNames},

		{AutocommitId, addAutocommitNode},
		{TrackProcessId, trackProcess},
		{parallelizeId, parallelize},
		{clearWarningsId, clearWarnings},
	}
}

// OnceBeforeDefault contains the rules to be applied just once before the
// DefaultRules.
var OnceBeforeDefault = []Rule{
	{applyDefaultSelectLimitId, applyDefaultSelectLimit},
	{replaceCountStarId, replaceCountStar},
	{applyEventSchedulerId, applyEventScheduler},
	{validateOffsetAndLimitId, validateLimitAndOffset},
	{validateCreateTableId, validateCreateTable},
	{validateAlterTableId, validateAlterTable},
	{validateExprSemId, validateExprSem},
	{validateCreateProcedureId, validateCreateProcedure},
	{resolveDropConstraintId, resolveDropConstraint},
	{resolveAlterColumnId, resolveAlterColumn},
	{validateDropTablesId, validateDropTables},
	{resolveCreateSelectId, resolveCreateSelect},
	{validateDropConstraintId, validateDropConstraint},
	{resolveUnionsId, resolveUnions},
	{resolveDescribeQueryId, resolveDescribeQuery}, //TODO
	{validateCreateTriggerId, validateCreateTrigger},
	{ValidateColumnDefaultsId, validateColumnDefaults},
	{validateReadOnlyDatabaseId, validateReadOnlyDatabase},
	{validateReadOnlyTransactionId, validateReadOnlyTransaction},
	{validateDatabaseSetId, validateDatabaseSet},
	{validateDeleteFromId, validateDeleteFrom},
	{validatePrivilegesId, validatePrivileges}, // Ensure that checking privileges happens after db, table  & table function resolution
	{simplifyFiltersId, simplifyFilters},       //TODO inline?
	{pushNotFiltersId, pushNotFilters},         //TODO inline?
	{hoistOutOfScopeFiltersId, hoistOutOfScopeFilters},
}

// AlwaysBeforeDefault contains the rules to be applied just once before the
// DefaultRules. These are an extension of the OnceBeforeDefault rules that
// will always apply to nodes, unlike the OnceBeforeDefault rules that may
// be excluded depending on the node. This is only used by integrators.
var AlwaysBeforeDefault []Rule

// DefaultRules to apply when analyzing nodes.
var DefaultRules = []Rule{
	{validateStarExpressionsId, validateStarExpressions}, //TODO
	{pushdownSubqueryAliasFiltersId, pushdownSubqueryAliasFilters},
	{pruneTablesId, pruneTables},
	{validateCheckConstraintId, validateCheckConstraints},
	{unnestInSubqueriesId, unnestInSubqueries},
	{resolveSubqueriesId, resolveSubqueries},
	{replaceCrossJoinsId, replaceCrossJoins},
}

var OnceAfterDefault = []Rule{
	{unnestExistsSubqueriesId, unnestExistsSubqueries},
	{moveJoinCondsToFilterId, moveJoinConditionsToFilter},
	{finalizeUnionsId, finalizeUnions},
	{loadTriggersId, loadTriggers},
	{processTruncateId, processTruncate},
	{stripTableNameInDefaultsId, stripTableNamesFromColumnDefaults},
	{pushFiltersId, pushFilters},
	{optimizeJoinsId, optimizeJoins},
	{finalizeSubqueriesId, finalizeSubqueries},
	{applyIndexesFromOuterScopeId, applyIndexesFromOuterScope},
	{replaceAggId, replaceAgg},
	{replaceIdxSortId, replaceIdxSort},
	{eraseProjectionId, eraseProjection},
	{flattenDistinctId, flattenDistinct},
	{insertTopNId, insertTopNNodes},
	{applyHashInId, applyHashIn},
	{assignRoutinesId, assignRoutines},
	{modifyUpdateExprsForJoinId, modifyUpdateExpressionsForJoin},
	{applyFKsId, applyForeignKeys},
}

// DefaultValidationRules to apply while analyzing nodes.
var DefaultValidationRules = []Rule{
	{validateResolvedId, validateIsResolved},
	{validateOrderById, validateOrderBy},
	{validateGroupById, validateGroupBy},
	{validateSchemaSourceId, validateSchemaSource},
	{validateIndexCreationId, validateIndexCreation},
	{ValidateOperandsId, validateOperands},
	{validateIntervalUsageId, validateIntervalUsage},
	{validateSubqueryColumnsId, validateSubqueryColumns},
	{validateUnionSchemasMatchId, validateUnionSchemasMatch},
	{validateAggregationsId, validateAggregations},
}

var OnceAfterAll []Rule
