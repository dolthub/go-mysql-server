package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveCreateSelect(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok || ct.Select() == nil {
		return n, nil
	}

	analyzedSelect, err := a.Analyze(ctx, ct.Select(), scope)
	if err != nil {
		return nil, err
	}

	// Get the correct schema of the CREATE TABLE based on the select query
	inputSpec := ct.TableSpec()
	selectSchema := analyzedSelect.Schema()
	mergedSchema := mergeSchemas(inputSpec.Schema.Schema, selectSchema)
	newSch := make(sql.Schema, len(mergedSchema))

	for i, col := range mergedSchema {
		tempCol := *col
		tempCol.Source = ct.Name()
		newSch[i] = &tempCol
	}

	pkOrdinals := make([]int, 0)
	for i, col := range newSch {
		if col.PrimaryKey {
			pkOrdinals = append(pkOrdinals, i)
		}
	}

	newSpec := inputSpec.WithSchema(sql.NewPrimaryKeySchema(newSch, pkOrdinals...))

	newCreateTable := plan.NewCreateTable(ct.Database(), ct.Name(), ct.IfNotExists(), ct.Temporary(), newSpec)
	analyzedCreate, err := a.Analyze(ctx, newCreateTable, scope)
	if err != nil {
		return nil, err
	}

	return plan.NewTableCopier(ct.Database(), StripQueryProcess(analyzedCreate), StripQueryProcess(analyzedSelect), plan.CopierProps{}), nil
}

// mergeSchemas takes in the table spec of the CREATE TABLE and merges it with the schema used by the
// select query. The ultimate structure for the new table will be [CREATE TABLE exclusive columns, columns with the same
// name, SELECT exclusive columns]
func mergeSchemas(inputSchema sql.Schema, selectSchema sql.Schema) sql.Schema {
	if inputSchema == nil {
		return selectSchema
	}

	// Get the matching columns between the two via name
	matchingColumns := make([]*sql.Column, 0)
	leftExclusive := make([]*sql.Column, 0)
	for _, col := range inputSchema {
		found := false
		for _, col2 := range selectSchema {
			if col.Name == col2.Name {
				matchingColumns = append(matchingColumns, col)
				found = true
			}
		}

		if !found {
			leftExclusive = append(leftExclusive, col)
		}
	}

	rightExclusive := make([]*sql.Column, 0)
	for _, col := range selectSchema {
		found := false
		for _, col2 := range inputSchema {
			if col.Name == col2.Name {
				found = true
				break
			}
		}

		if !found {
			rightExclusive = append(rightExclusive, col)
		}
	}

	return append(append(leftExclusive, matchingColumns...), rightExclusive...)
}
