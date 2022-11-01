package analyzer

//go:generate stringer -type=RuleId -linecomment

type RuleId int

const (
	// once before
	applyDefaultSelectLimitId     RuleId = iota // applyDefaultSelectLimit
	validateOffsetAndLimitId                    //validateOffsetAndLimit
	validateCreateTableId                       // validateCreateTable
	validateExprSemId                           // validateExprSem
	resolveVariablesId                          // resolveVariables
	resolveNamedWindowsId                       // resolveNamedWindows
	resolveSetVariablesId                       // resolveSetVariables
	resolveViewsId                              // resolveViews
	liftCtesId                                  // liftCtes
	resolveCtesId                               // resolveCtes
	liftRecursiveCtesId                         // liftRecursiveCtes
	resolveDatabasesId                          // resolveDatabases
	resolveTablesId                             // resolveTables
	loadStoredProceduresId                      // loadStoredProcedures
	validateDropTablesId                        // validateDropTables
	setTargetSchemasId                          // setTargetSchemas
	resolveCreateLikeId                         // resolveCreateLike
	parseColumnDefaultsId                       // parseColumnDefaults
	resolveDropConstraintId                     // resolveDropConstraint
	validateDropConstraintId                    // validateDropConstraint
	loadCheckConstraintsId                      // loadCheckConstraints
	assignCatalogId                             // assignCatalog
	resolveCreateSelectId                       // resolveCreateSelect
	resolveSubqueriesId                         // resolveSubqueries
	setViewTargetSchemaId                       // setViewTargetSchema
	resolveUnionsId                             // resolveUnions
	resolveDescribeQueryId                      // resolveDescribeQuery
	checkUniqueTableNamesId                     // checkUniqueTableNames
	resolveTableFunctionsId                     // resolveTableFunctions
	resolveDeclarationsId                       // resolveDeclarations
	resolveColumnDefaultsId                     // resolveColumnDefaults
	validateColumnDefaultsId                    // validateColumnDefaults
	validateCreateTriggerId                     // validateCreateTrigger
	validateCreateProcedureId                   // validateCreateProcedure
	loadInfoSchemaId                            // loadInfoSchema
	validateReadOnlyDatabaseId                  // validateReadOnlyDatabase
	validateReadOnlyTransactionId               // validateReadOnlyTransaction
	validateDatabaseSetId                       // validateDatabaseSet
	validatePrivilegesId                        // validatePrivileges
	reresolveTablesId                           // reresolveTables
	setInsertColumnsId                          // setInsertColumns
	validateJoinComplexityId                    // validateJoinComplexity

	// default
	resolveNaturalJoinsId          // resolveNaturalJoins
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
	expandStarsId                  // expandStars
	transposeRightJoinsId          // transposeRightJoins
	resolveHavingId                // resolveHaving
	mergeUnionSchemasId            // mergeUnionSchemas
	flattenAggregationExprsId      // flattenAggregationExprs
	reorderProjectionId            // reorderProjection
	resolveSubqueryExprsId         // resolveSubqueryExprs
	finalizeSubqueryExprsId        // finalizeSubqueryExprs
	replaceCrossJoinsId            // replaceCrossJoins
	moveJoinCondsToFilterId        // moveJoinCondsToFilter
	evalFilterId                   // evalFilter
	optimizeDistinctId             // optimizeDistinct

	// after default
	finalizeSubqueriesId          // finalizeSubqueries
	finalizeUnionsId              // finalizeUnions
	loadTriggersId                // loadTriggers
	processTruncateId             // processTruncate
	resolveAlterColumnId          // resolveAlterColumn
	resolveGeneratorsId           // resolveGenerators
	removeUnnecessaryConvertsId   // removeUnnecessaryConverts
	pruneColumnsId                // pruneColumns
	stripTableNameInDefaultsId    // stripTableNamesFromColumnDefaults
	hoistSelectExistsId           // hoistSelectExists
	optimizeJoinsId               // optimizeJoins
	pushdownFiltersId             // pushdownFilters
	subqueryIndexesId             // subqueryIndexes
	inSubqueryIndexesId           // inSubqueryIndexes
	pruneTablesId                 // pruneTables
	setJoinScopeLenId             // setJoinScopeLen
	eraseProjectionId             // eraseProjection
	replaceSortPkId               // replaceSortPk
	insertTopNId                  // insertTopN
	cacheSubqueryResultsId        // cacheSubqueryResults
	cacheSubqueryAliasesInJoinsId // cacheSubqueryAliasesInJoins
	applyHashLookupsId            // applyHashLookups
	applyHashInId                 // applyHashIn
	resolveInsertRowsId           // resolveInsertRows
	resolvePreparedInsertId       // resolvePreparedInsert
	applyTriggersId               // applyTriggers
	applyProceduresId             // applyProcedures
	assignRoutinesId              // assignRoutines
	modifyUpdateExprsForJoinId    // modifyUpdateExprsForJoin
	applyRowUpdateAccumulatorsId  // applyRowUpdateAccumulators
	wrapWithRollbackId            // rollback triggers
	applyFKsId                    // applyFKs

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

	// after all
	AutocommitId    // addAutocommitNode
	TrackProcessId  // trackProcess
	parallelizeId   // parallelize
	clearWarningsId // clearWarnings
)
