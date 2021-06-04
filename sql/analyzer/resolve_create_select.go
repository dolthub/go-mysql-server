package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveCreateSelect(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	planCreate, ok := n.(*plan.CreateTable)
	if !ok || planCreate.Select() == nil {
		return n, nil
	}

	analyzedSelect, err := a.Analyze(ctx, planCreate.Select(), scope)
	if err != nil {
		return nil, err
	}

	// TODO: Merge select schema and create schema for queries like CREATE TABLE x(pk int) AS SELECT * (unsupported in
	// parser

	// Get the correct schema of the CREATE TABLE based on the select query
	newCreateTable := plan.NewCreateTable(planCreate.Database(), planCreate.Name(), planCreate.IfNotExists(), planCreate.Temporary(), planCreate.TableSpec())
	analyzedCreate, err := a.Analyze(ctx, newCreateTable, scope)
	if err != nil {
		return nil, err
	}

	return plan.NewTableCopier(planCreate.Database(), stripQueryProcess(analyzedCreate), stripQueryProcess(analyzedSelect), plan.CopierProps{}), nil
}
