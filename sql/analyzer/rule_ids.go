package analyzer

//go:generate stringer -type=RuleId -linecomment

type RuleId int

const (
	// once before
	applyDefaultSelectLimitId      RuleId = iota // applyDefaultSelectLimit
	validateOffsetAndLimitId                     // validateOffsetAndLimit
	validateStarExpressionsId                    // validateStarExpressions
	validateCreateTableId                        // validateCreateTable
	validateAlterTableId                         // validateAlterTable
	validateExprSemId                            // validateExprSem
	resolveVariablesId                           // resolveVariables
	resolveNamedWindowsId                        // resolveNamedWindows
	resolveSetVariablesId                        // resolveSetVariables
	resolveViewsId                               // resolveViews
	liftCtesId                                   // liftCtes
	resolveCtesId                                // resolveCtes
	liftRecursiveCtesId                          // liftRecursiveCtes
	resolveDatabasesId                           // resolveDatabases
	resolveTablesId                              // resolveTables
	loadStoredProceduresId                       // loadStoredProcedures
	validateDropTablesId                         // validateDropTables
	pruneDropTablesId                            // pruneDropTables
	setTargetSchemasId                           // setTargetSchemas
	resolveCreateLikeId                          // resolveCreateLike
	parseColumnDefaultsId                        // parseColumnDefaults
	resolveDropConstraintId                      // resolveDropConstraint
	validateDropConstraintId                     // validateDropConstraint
	loadCheckConstraintsId                       // loadCheckConstraints
	assignCatalogId                              // assignCatalog
	resolveAnalyzeTablesId                       // resolveAnalyzeTables
	resolveCreateSelectId                        // resolveCreateSelect
	resolveSubqueriesId                          // resolveSubqueries
	setViewTargetSchemaId                        // setViewTargetSchema
	resolveUnionsId                              // resolveUnions
	resolveDescribeQueryId                       // resolveDescribeQuery
	checkUniqueTableNamesId                      // checkUniqueTableNames
	resolveTableFunctionsId                      // resolveTableFunctions
	resolveDeclarationsId                        // resolveDeclarations
	resolveColumnDefaultsId                      // resolveColumnDefaults
	validateColumnDefaultsId                     // validateColumnDefaults
	validateCreateTriggerId                      // validateCreateTrigger
	validateCreateProcedureId                    // validateCreateProcedure
	resolveCreateProcedureId                     // resolveCreateProcedure
	loadInfoSchemaId                             // loadInfoSchema
	validateReadOnlyDatabaseId                   // validateReadOnlyDatabase
	validateReadOnlyTransactionId                // validateReadOnlyTransaction
	validateDatabaseSetId                        // validateDatabaseSet
	validatePrivilegesId                         // validatePrivileges
	reresolveTablesId                            // reresolveTables
	setInsertColumnsId                           // setInsertColumns
	validateJoinComplexityId                     // validateJoinComplexity
	applyBinlogReplicaControllerId               // applyBinlogReplicaController
	applyEventSchedulerId                        // applyEventScheduler

	// default
	resolveUsingJoinsId            // resolveUsingJoins
	resolveOrderbyLiteralsId       // resolveOrderbyLiterals
	resolveFunctionsId             // resolveFunctions
	flattenTableAliasesId          // flattenTableAliases
	pushdownSortId                 // pushdownSort
	pushdownGroupbyAliasesId       // pushdownGroupbyAliases
	pushdownSubqueryAliasFiltersId // pushdownSubqueryAliasFilters
	qualifyColumnsId               // qualifyColumns
	resolveColumnsId               // resolveColumns
	validateCheckConstraintId      // validateCheckConstraint
	resolveBarewordSetVariablesId  // resolveBarewordSetVariables
	replaceCountStarId             // replaceCountStar
	expandStarsId                  // expandStars
	transposeRightJoinsId          // transposeRightJoins
	resolveHavingId                // resolveHaving
	mergeUnionSchemasId            // mergeUnionSchemas
	flattenAggregationExprsId      // flattenAggregationExprs
	reorderProjectionId            // reorderProjection
	resolveSubqueryExprsId         // resolveSubqueryExprs
	replaceCrossJoinsId            // replaceCrossJoins
	moveJoinCondsToFilterId        // moveJoinCondsToFilter
	evalFilterId                   // evalFilter
	optimizeDistinctId             // optimizeDistinct

	// after default
	hoistOutOfScopeFiltersId     // hoistOutOfScopeFilters
	transformJoinApplyId         // transformJoinApply
	hoistSelectExistsId          // hoistSelectExists
	finalizeSubqueriesId         // finalizeSubqueries
	finalizeUnionsId             // finalizeUnions
	loadTriggersId               // loadTriggers
	loadEventsId                 // loadEvents
	processTruncateId            // processTruncate
	resolveAlterColumnId         // resolveAlterColumn
	resolveGeneratorsId          // resolveGenerators
	removeUnnecessaryConvertsId  // removeUnnecessaryConverts
	pruneColumnsId               // pruneColumns
	stripTableNameInDefaultsId   // stripTableNamesFromColumnDefaults
	foldEmptyJoinsId             // foldEmptyJoins
	optimizeJoinsId              // optimizeJoins
	generateIndexScansId         // generateIndexScans
	matchAgainstId               // matchAgainst
	pushFiltersId                // pushFilters
	applyIndexesFromOuterScopeId // applyIndexesFromOuterScope
	pruneTablesId                // pruneTables
	fixupAuxiliaryExprsId        // fixupAuxiliaryExprs
	assignExecIndexesId          // assignExecIndexes
	inlineSubqueryAliasRefsId    // inlineSubqueryAliasRefs
	eraseProjectionId            // eraseProjection
	replaceAggId                 // replaceAgg
	replaceIdxSortId             // replaceIdxSort
	insertTopNId                 // insertTopN
	applyHashInId                // applyHashIn
	resolveInsertRowsId          // resolveInsertRows
	resolvePreparedInsertId      // resolvePreparedInsert
	applyTriggersId              // applyTriggers
	applyProceduresId            // applyProcedures
	assignRoutinesId             // assignRoutines
	modifyUpdateExprsForJoinId   // modifyUpdateExprsForJoin
	applyRowUpdateAccumulatorsId // applyRowUpdateAccumulators
	wrapWithRollbackId           // rollback triggers
	applyFKsId                   // applyFKs

	// validate
	validateResolvedId          // validateResolved
	validateOrderById           // validateOrderBy
	validateGroupById           // validateGroupBy
	validateSchemaSourceId      // validateSchemaSource
	validateIndexCreationId     // validateIndexCreation
	validateOperandsId          // validateOperands
	validateCaseResultTypesId   // validateCaseResultTypes
	validateIntervalUsageId     // validateIntervalUsage
	validateExplodeUsageId      // validateExplodeUsage
	validateSubqueryColumnsId   // validateSubqueryColumns
	validateUnionSchemasMatchId // validateUnionSchemasMatch
	validateAggregationsId      // validateAggregations
	validateDeleteFromId        // validateDeleteFrom

	// after all
	cacheSubqueryResultsId        // cacheSubqueryResults
	cacheSubqueryAliasesInJoinsId // cacheSubqueryAliasesInJoins
	AutocommitId                  // addAutocommitNode
	TrackProcessId                // trackProcess
	parallelizeId                 // parallelize
	clearWarningsId               // clearWarnings
)
