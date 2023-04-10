package row_exec

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type Builder struct{}

func (b *Builder) Build(n sql.Node) (sql.RowIter, error) {
	//return b.buildExec(n, nil)
	return nil, nil
}

func (b *Builder) buildCreateForeignKey(n sql.Node, row sql.Row) (sql.RowIter, error) {
	if p.FkDef.OnUpdate == sql.ForeignKeyReferentialAction_SetDefault || p.FkDef.OnDelete == sql.ForeignKeyReferentialAction_SetDefault {
		return nil, sql.ErrForeignKeySetDefault.New()
	}
	db, err := p.dbProvider.Database(ctx, p.FkDef.Database)
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, p.FkDef.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.FkDef.Table)
	}

	refDb, err := p.dbProvider.Database(ctx, p.FkDef.ParentDatabase)
	if err != nil {
		return nil, err
	}
	refTbl, ok, err := refDb.GetTableInsensitive(ctx, p.FkDef.ParentTable)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.FkDef.ParentTable)
	}

	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.Table)
	}
	refFkTbl, ok := refTbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.ParentTable)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}

	err = ResolveForeignKey(ctx, fkTbl, refFkTbl, *p.FkDef, true, fkChecks.(int8) == 1)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

func (b *Builder) buildAlterTableCollation(n sql.Node, row sql.Row) (sql.RowIter, error) {
	renamer, ok := r.db.(sql.TableRenamer)
	if !ok {
		return nil, sql.ErrRenameTableNotSupported.New(r.db.Name())
	}

	var err error
	for i, oldName := range r.oldNames {
		var tbl sql.Table
		var ok bool
		tbl, ok, err = r.db.GetTableInsensitive(ctx, oldName)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, sql.ErrTableNotFound.New(oldName)
		}

		if fkTable, ok := tbl.(sql.ForeignKeyTable); ok {
			parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, parentFk := range parentFks {
				//TODO: support renaming tables across databases for foreign keys
				if strings.ToLower(parentFk.Database) != strings.ToLower(parentFk.ParentDatabase) {
					return nil, fmt.Errorf("updating foreign key table names across databases is not yet supported")
				}
				parentFk.ParentTable = r.newNames[i]
				childTbl, ok, err := r.db.GetTableInsensitive(ctx, parentFk.Table)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, sql.ErrTableNotFound.New(parentFk.Table)
				}
				childFkTbl, ok := childTbl.(sql.ForeignKeyTable)
				if !ok {
					return nil, fmt.Errorf("referenced table `%s` supports foreign keys but declaring table `%s` does not", parentFk.ParentTable, parentFk.Table)
				}
				err = childFkTbl.UpdateForeignKey(ctx, parentFk.Name, parentFk)
				if err != nil {
					return nil, err
				}
			}

			fks, err := fkTable.GetDeclaredForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, fk := range fks {
				fk.Table = r.newNames[i]
				err = fkTable.UpdateForeignKey(ctx, fk.Name, fk)
				if err != nil {
					return nil, err
				}
			}
		}

		err = renamer.RenameTable(ctx, tbl.Name(), r.newNames[i])
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}
func (b *Builder) buildCreateRole(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildLoop(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildTransactionCommittingNode(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildDropColumn(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildAnalyzeTable(n sql.Node, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &analyzeTableIter{
		idx:    0,
		tables: n.Tables,
		stats:  n.Stats,
	}, nil
}
func (b *Builder) buildQueryProcess(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildShowReplicaStatus(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildUpdateSource(n sql, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildelseCaseError(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildPrepareQuery(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildResolvedTable(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildShowCreateTable(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildShowIndexes(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildprependNode(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildUnresolvedTable(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildUse(n sql.Node, row sql.Row) (sql.RowIter, error)             { return nil, nil }
func (b *Builder) buildCreateTable(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildCreateProcedure(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildCreateTrigger(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildIfConditional(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildShowGrants(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildShowDatabases(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildUpdateJoin(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildCall(n sql.Node, row sql.Row) (sql.RowIter, error)            {
	for i, paramExpr := range c.Params {
		val, err := paramExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		paramName := c.Procedure.Params[i].Name
		paramType := c.Procedure.Params[i].Type
		err = c.pRef.InitializeVariable(paramName, paramType, val)
		if err != nil {
			return nil, err
		}
	}
	c.pRef.PushScope()
	innerIter, err := c.Procedure.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &callIter{
		call:      c,
		innerIter: innerIter,
	}, nil
}
func (b *Builder) buildClose(n sql.Node, row sql.Row) (sql.RowIter, error)           { return nil, nil }
func (b *Builder) buildDescribe(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildExecuteQuery(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildProcedureResolvedTable(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildShowTriggers(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildBeginEndBlock(n sql.Node, row sql.Row) (sql.RowIter, error) {
	b.pRef.PushScope()
	rowIter, err := b.Block.RowIter(ctx, row)
	if err != nil {
		if exitErr, ok := err.(expression.ProcedureBlockExitError); ok && b.pRef.CurrentHeight() == int(exitErr) {
			err = nil
		} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(b.Label) {
			if controlFlow.IsExit {
				err = nil
			} else {
				err = fmt.Errorf("encountered ITERATE on BEGIN...END, which should should have been caught by the analyzer")
			}
		}
		if nErr := b.pRef.PopScope(ctx); err == nil && nErr != nil {
			err = nErr
		}
		return sql.RowsToRowIter(), err
	}
	return &beginEndIter{
		BeginEndBlock: b,
		rowIter:       rowIter,
	}, nil
}
func (b *Builder) buildAlterDB(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildGrant(n sql.Node, row sql.Row) (sql.RowIter, error)         { return nil, nil }
func (b *Builder) buildIterate(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildOpen(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildChangeReplicationFilter(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildStopReplica(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildShowVariables(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildSort(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildSubqueryAlias(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildUnion(n sql.Node, row sql.Row) (sql.RowIter, error)         { return nil, nil }
func (b *Builder) buildIndexedTableAccess(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildAddColumn(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildRenameColumn(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildDropDB(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildDistinct(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildHaving(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildSignal(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildTriggerRollback(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildExternalProcedure(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildInto(n sql.Node, row sql.Row) (sql.RowIter, error)           { return nil, nil }
func (b *Builder) buildLockTables(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildtestNode(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildTruncate(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildDeclareHandler(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildDropProcedure(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildChangeReplicationSource(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildMax1Row(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildRollback(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildLimit(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildRecursiveCte(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildShowColumns(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildDropIndex(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildResetReplica(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildShowCreateTrigger(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildTableCopier(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildDeclareVariables(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildFilter(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildKill(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildShowCreateDatabase(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildShowPrivileges(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildAlterPK(n sql.Node, row sql.Row) (sql.RowIter, error)        {
	// We grab the table from the database to ensure that state is properly refreshed, thereby preventing multiple keys
	// being defined.
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, a.Database(), a.Table)
	if err != nil {
		return nil, err
	}

	// TODO: these validation checks belong in the analysis phase, not here
	pkAlterable, ok := table.(sql.PrimaryKeyAlterableTable)
	if !ok {
		return nil, ErrNotPrimaryKeyAlterable.New(a.Table)
	}
	if err != nil {
		return nil, err
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		if hasPrimaryKeys(pkAlterable) {
			return sql.RowsToRowIter(), sql.ErrMultiplePrimaryKeysDefined.New()
		}

		for _, c := range a.Columns {
			if !pkAlterable.Schema().Contains(c.Name, pkAlterable.Name()) {
				return sql.RowsToRowIter(), sql.ErrKeyColumnDoesNotExist.New(c.Name)
			}
		}

		return &createPkIter{
			targetSchema: a.targetSchema,
			columns:      a.Columns,
			pkAlterable:  pkAlterable,
		}, nil
	case PrimaryKeyAction_Drop:
		return &dropPkIter{
			targetSchema: a.targetSchema,
			pkAlterable:  pkAlterable,
		}, nil
	default:
		panic("unreachable")
	}
}
func (b *Builder) buildnothing(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildRevokeAll(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildDeferredAsOfTable(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildCreateUser(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildDropView(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildGroupBy(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildRowUpdateAccumulator(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildBlock(n sql.Node, row sql.Row) (sql.RowIter, error) {
	var returnRows []sql.Row
	var returnNode sql.Node
	var returnSch sql.Schema

	selectSeen := false
	for _, s := range b.statements {
		err := func() error {
			rowCache, disposeFunc := ctx.Memory.NewRowsCache()
			defer disposeFunc()

			var isSelect bool
			subIter, err := s.RowIter(ctx, row)
			if err != nil {
				return err
			}
			subIterNode := s
			subIterSch := s.Schema()
			if blockSubIter, ok := subIter.(BlockRowIter); ok {
				subIterNode = blockSubIter.RepresentingNode()
				subIterSch = blockSubIter.Schema()
			}
			if isSelect = nodeRepresentsSelect(subIterNode); isSelect {
				selectSeen = true
				returnNode = subIterNode
				returnSch = subIterSch
			} else if !selectSeen {
				returnNode = subIterNode
				returnSch = subIterSch
			}

			for {
				newRow, err := subIter.Next(ctx)
				if err == io.EOF {
					err := subIter.Close(ctx)
					if err != nil {
						return err
					}
					if isSelect || !selectSeen {
						returnRows = rowCache.Get()
					}
					break
				} else if err != nil {
					return err
				} else if isSelect || !selectSeen {
					err = rowCache.Add(newRow)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	b.rowIterSch = returnSch
	return &blockIter{
		internalIter: sql.RowsToRowIter(returnRows...),
		repNode:      returnNode,
		sch:          returnSch,
	}, nil
}

func (b *Builder) buildInsertDestination(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildSet(n sql.Node, row sql.Row) (sql.RowIter, error)             { return nil, nil }
func (b *Builder) buildTriggerExecutor(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildAlterDefaultDrop(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildCachedResults(n sql.Node, row sql.Row) (sql.RowIter, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.disposed {
		return nil, fmt.Errorf("%w: %T", ErrRowIterDisposed, n)
	}

	if rows := n.getCachedResults(); rows != nil {
		return sql.RowsToRowIter(rows...), nil
	} else if n.noCache {
		return n.UnaryNode.Child.RowIter(ctx, r)
	} else if n.finalized {
		return emptyIter, nil
	}

	ci, err := n.UnaryNode.Child.RowIter(ctx, r)
	if err != nil {
		return nil, err
	}
	cache, dispose := ctx.Memory.NewRowsCache()
	return &cachedResultsIter{n, ci, cache, dispose}, nil
}
func (b *Builder) buildCreateDB(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildRevoke(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildDeclareCondition(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildTriggerBeginEndBlock(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildRecursiveTable(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildAlterIndex(n sql.Node, row sql.Row) (sql.RowIter, error)     {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}
func (b *Builder) buildTransformedNamedNode(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildCreateIndex(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildProcedure(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildNoopTriggerRollback(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildWith(n sql.Node, row sql.Row) (sql.RowIter, error)            { return nil, nil }
func (b *Builder) buildProject(n sql.Node, row sql.Row) (sql.RowIter, error)         { return nil, nil }
func (b *Builder) buildModifyColumn(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildDeclareCursor(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildOrderedDistinct(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildSingleDropView(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildEmptyTable(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildJoinNode(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildRenameUser(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildShowCreateProcedure(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildCommit(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildDeferredFilteredTable(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildValues(n sql.Node, row sql.Row) (sql.RowIter, error)          { return nil, nil }
func (b *Builder) buildDropRole(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildFetch(n sql.Node, row sql.Row) (sql.RowIter, error)           { return nil, nil }
func (b *Builder) buildRevokeRole(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildShowStatus(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildShowTableStatus(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildSignalName(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildStartTransaction(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildValueDerivedTable(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildCreateView(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildInsertInto(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildTopN(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildWindow(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildDropCheck(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildDropTrigger(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildIndexedInSubqueryFilter(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildDeallocateQuery(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildRollbackSavepoint(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildReleaseSavepoint(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildUpdate(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildShowWarnings(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildReleaser(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildConcat(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildDeleteFrom(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildDescribeQuery(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildForeignKeyHandler(n sql.Node, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}
func (b *Builder) buildLoadData(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildShowCharset(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildStripRowNode(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildDropConstraint(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildFlushPrivileges(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildLeave(n sql.Node, row sql.Row) (sql.RowIter, error)           { return nil, nil }
func (b *Builder) buildShowProcessList(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildCreateSavepoint(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildCreateCheck(n sql.Node, row sql.Row) (sql.RowIter, error) {
	err := c.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (b *Builder) buildAlterDefaultSet(n *plan.AlterDefualtSet, row sql.Row) (sql.RowIter, error) {
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, d.Database(), d.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(d.Table)
	}

	if err != nil {
		return nil, err
	}
	loweredColName := strings.ToLower(d.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}
	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(d.Table, d.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = d.Default
	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, d.ColumnName, newCol, nil)
}

func (b *Builder) buildDropUser(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildIfElseBlock(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildNamedWindows(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildRepeat(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildRevokeProxy(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildRenameTable(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildCaseStatement(n sql.Node, row sql.Row) (sql.RowIter, error) {
	caseValue, err := c.Expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	for _, ifConditional := range c.IfElse.IfConditionals {
		whenValue, err := ifConditional.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		comparison, err := c.Expr.Type().Compare(caseValue, whenValue)
		if err != nil {
			return nil, err
		}
		if comparison != 0 {
			continue
		}

		return c.constructRowIter(ctx, row, ifConditional, ifConditional.Body)
	}

	// All conditions failed so we run the else
	return c.constructRowIter(ctx, row, c.IfElse.Else, c.IfElse.Else)
}
func (b *Builder) buildGrantRole(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
func (b *Builder) buildGrantProxy(n sql.Node, row sql.Row) (sql.RowIter, error)    { return nil, nil }
func (b *Builder) buildOffset(n sql.Node, row sql.Row) (sql.RowIter, error)        { return nil, nil }
func (b *Builder) buildStartReplica(n sql.Node, row sql.Row) (sql.RowIter, error)  { return nil, nil }
func (b *Builder) buildWhile(n sql.Node, row sql.Row) (sql.RowIter, error)         { return nil, nil }
func (b *Builder) buildAlterAutoIncrement(n sql.Node, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}
func (b *Builder) buildDropForeignKey(n sql.Node, row sql.Row) (sql.RowIter, error) { return nil, nil }
func (b *Builder) buildDropTable(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildJSONTable(n sql.Node, row sql.Row) (sql.RowIter, error)      { return nil, nil }
func (b *Builder) buildUnlockTables(n sql.Node, row sql.Row) (sql.RowIter, error)   { return nil, nil }
func (b *Builder) buildExchange(n sql.Node, row sql.Row) (sql.RowIter, error)       { return nil, nil }
func (b *Builder) buildHashLookup(n sql.Node, row sql.Row) (sql.RowIter, error)     { return nil, nil }
