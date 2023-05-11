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

// OnceBeforeDefault_Exp contains the rules to be applied just once before the
// DefaultRules.
var OnceBeforeDefault_Exp = []Rule{
	{applyDefaultSelectLimitId, applyDefaultSelectLimit},
	{applyBinlogReplicaControllerId, applyBinlogReplicaController},
	{validateOffsetAndLimitId, validateLimitAndOffset},
	{validateCreateTableId, validateCreateTable},
	{validateExprSemId, validateExprSem},
	{validateCreateProcedureId, validateCreateProcedure},
	{setTargetSchemasId, setTargetSchemas},     //TODO
	{loadCheckConstraintsId, loadChecks},       //TODO
	{resolveAlterColumnId, resolveAlterColumn}, //TODO
	{validateDropTablesId, validateDropTables},
	{pruneDropTablesId, pruneDropTables}, //TODO
	{resolveCreateLikeId, resolveCreateLike},
	{resolveAnalyzeTablesId, resolveAnalyzeTables},
	{assignCatalogId, assignCatalog}, //TODO
	{parseColumnDefaultsId, parseColumnDefaults},
	{resolveDropConstraintId, resolveDropConstraint}, //TODO
	{validateDropConstraintId, validateDropConstraint},
	{resolveCreateSelectId, resolveCreateSelect}, //TODO
	//{resolveSubqueriesId, resolveSubqueries}, // i think redundant now
	{setViewTargetSchemaId, setViewTargetSchema},
	{resolveUnionsId, resolveUnions},
	{resolveDescribeQueryId, resolveDescribeQuery},             //TODO
	{disambiguateTableFunctionsId, disambiguateTableFunctions}, //TODO
	{checkUniqueTableNamesId, validateUniqueTableNames},        //TODO
	{resolveDeclarationsId, resolveDeclarations},
	{validateCreateTriggerId, validateCreateTrigger},
	{loadInfoSchemaId, loadInfoSchema},               //TODO
	{resolveColumnDefaultsId, resolveColumnDefaults}, //TODO
	{validateColumnDefaultsId, validateColumnDefaults},
	{validateReadOnlyDatabaseId, validateReadOnlyDatabase},
	{validateReadOnlyTransactionId, validateReadOnlyTransaction},
	{validateDatabaseSetId, validateDatabaseSet},
	{validateDeleteFromId, validateDeleteFrom},
	{validatePrivilegesId, validatePrivileges}, // Ensure that checking privileges happens after db, table  & table function resolution
}

// DefaultRules_Exp to apply when analyzing nodes.
var DefaultRules_Exp = []Rule{
	{resolveNaturalJoinsId, resolveNaturalJoins},
	{validateStarExpressionsId, validateStarExpressions}, //TODO
	{flattenTableAliasesId, flattenTableAliases},         //TODO
	{pushdownSubqueryAliasFiltersId, pushdownSubqueryAliasFilters},
	{pruneTablesId, pruneTables},
	{fixupSortNodesId, fixupSortNodes},
	{validateCheckConstraintId, validateCheckConstraints},
	{transposeRightJoinsId, transposeRightJoins}, //TODO
	{mergeUnionSchemasId, mergeUnionSchemas},     //TODO
	{resolveSubqueriesId, resolveSubqueries},
	{resolveBarewordSetVariablesId, resolveBarewordSetVariables}, //TODO
	{replaceCrossJoinsId, replaceCrossJoins},
	{moveJoinCondsToFilterId, moveJoinConditionsToFilter}, // depends on indexes being correct
	{evalFilterId, simplifyFilters},                       //TODO inline?
}
