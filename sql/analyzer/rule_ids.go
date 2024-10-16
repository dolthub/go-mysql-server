package analyzer

//go:generate stringer -type=RuleId -linecomment

type RuleId int

const (
	// once before
	applyDefaultSelectLimitId     RuleId = iota // applyDefaultSelectLimit
	validateOffsetAndLimitId                    // validateOffsetAndLimit
	validateStarExpressionsId                   // validateStarExpressions
	validateCreateTableId                       // validateCreateTable
	validateAlterTableId                        // validateAlterTable
	validateExprSemId                           // validateExprSem
	loadStoredProceduresId                      // loadStoredProcedures
	validateDropTablesId                        // validateDropTables
	resolveDropConstraintId                     // resolveDropConstraint
	validateDropConstraintId                    // validateDropConstraint
	resolveCreateSelectId                       // resolveCreateSelect
	resolveSubqueriesId                         // resolveSubqueries
	resolveUnionsId                             // resolveUnions
	resolveDescribeQueryId                      // resolveDescribeQuery
	ValidateColumnDefaultsId                    // validateColumnDefaults
	validateCreateTriggerId                     // validateCreateTrigger
	validateCreateProcedureId                   // validateCreateProcedure
	validateReadOnlyDatabaseId                  // validateReadOnlyDatabase
	validateReadOnlyTransactionId               // validateReadOnlyTransaction
	validateDatabaseSetId                       // validateDatabaseSet
	validatePrivilegesId                        // validatePrivileges
	applyEventSchedulerId                       // applyEventScheduler

	// default
	flattenTableAliasesId          // flattenTableAliases
	pushdownSubqueryAliasFiltersId // pushdownSubqueryAliasFilters
	validateCheckConstraintId      // validateCheckConstraints
	replaceCountStarId             // replaceCountStar
	replaceCrossJoinsId            // replaceCrossJoins
	moveJoinCondsToFilterId        // moveJoinConditionsToFilter
	simplifyFiltersId              // simplifyFilters
	pushNotFiltersId               // pushNotFilters

	// after default
	hoistOutOfScopeFiltersId     // hoistOutOfScopeFilters
	unnestInSubqueriesId         // unnestInSubqueries
	unnestExistsSubqueriesId     // unnestExistsSubqueries
	finalizeSubqueriesId         // finalizeSubqueries
	finalizeUnionsId             // finalizeUnions
	loadTriggersId               // loadTriggers
	processTruncateId            // processTruncate
	resolveAlterColumnId         // resolveAlterColumn
	stripTableNameInDefaultsId   // stripTableNamesFromColumnDefaults
	optimizeJoinsId              // optimizeJoins
	pushFiltersId                // pushFilters
	applyIndexesFromOuterScopeId // applyIndexesFromOuterScope
	pruneTablesId                // pruneTables
	assignExecIndexesId          // assignExecIndexes
	inlineSubqueryAliasRefsId    // inlineSubqueryAliasRefs
	eraseProjectionId            // eraseProjection
	flattenDistinctId            // flattenDistinct
	replaceAggId                 // replaceAgg
	replaceIdxSortId             // replaceIdxSort
	insertTopNId                 // insertTopNNodes
	replaceIdxOrderByDistanceId  // replaceIdxOrderByDistance
	applyHashInId                // applyHashIn
	resolveInsertRowsId          // resolveInsertRows
	applyTriggersId              // applyTriggers
	applyProceduresId            // applyProcedures
	assignRoutinesId             // assignRoutines
	modifyUpdateExprsForJoinId   // modifyUpdateExprsForJoin
	applyUpdateAccumulatorsId    // applyUpdateAccumulators
	wrapWithRollbackId           // wrapWithRollback
	applyForeignKeysId           // applyForeignKeys

	// validate
	validateResolvedId          // validateResolved
	validateOrderById           // validateOrderBy
	validateGroupById           // validateGroupBy
	validateSchemaSourceId      // validateSchemaSource
	validateIndexCreationId     // validateIndexCreation
	ValidateOperandsId          // validateOperands
	validateIntervalUsageId     // validateIntervalUsage
	validateSubqueryColumnsId   // validateSubqueryColumns
	validateUnionSchemasMatchId // validateUnionSchemasMatch
	validateAggregationsId      // validateAggregations
	validateDeleteFromId        // validateDeleteFrom

	// after all
	cacheSubqueryAliasesInJoinsId    // cacheSubqueryAliasesInJoins
	backtickDefaulColumnValueNamesId // backtickDefaultColumnValueNames
	AutocommitId                     // addAutocommit
	TrackProcessId                   // trackProcess
	parallelizeId                    // parallelize
)
