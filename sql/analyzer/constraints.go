// Copyright 2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveDropConstraint replaces DropConstraint nodes with a concrete type of alter table node as appropriate, or
// throws a constraint not found error if the named constraint isn't found on the table given. For multi-clause
// ALTER TABLE blocks, it also attaches resolved DropCheck names to each ModifyColumn and defers check validation to
// the final ModifyColumn.
func resolveDropConstraint(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if block, ok := n.(*plan.Block); ok && block.IsMultiAlter {
			var droppedChecks map[string]bool
			var modifies []*plan.ModifyColumn
			for _, stmt := range block.Children() {
				switch s := stmt.(type) {
				case *plan.DropCheck:
					if droppedChecks == nil {
						droppedChecks = map[string]bool{}
					}
					droppedChecks[strings.ToLower(s.Name)] = true
				case *plan.ModifyColumn:
					modifies = append(modifies, s)
				}
			}
			for idx, mc := range modifies {
				if len(droppedChecks) > 0 {
					mc.DroppedChecks = droppedChecks
				}
				if len(modifies) > 1 {
					if idx < len(modifies)-1 {
						mc.SkipCheckValidation = true
					} else {
						mc.ForceCheckValidation = true
					}
				}
			}
			return n, transform.SameTree, nil
		}

		dropConstraint, ok := n.(*plan.DropConstraint)
		if !ok {
			return n, transform.SameTree, nil
		}

		rt, ok := dropConstraint.Child.(*plan.ResolvedTable)
		if !ok {
			return nil, transform.SameTree, ErrInAnalysis.New("Expected a TableNode for ALTER TABLE DROP CONSTRAINT statement")
		}

		//TODO: handle if a foreign key and check constraint have the same name, it should error saying to use the specific drop
		table := sql.GetUnderlyingTable(rt.Table)
		fkt, ok := table.(sql.ForeignKeyTable)
		if ok {
			decFks, err := fkt.GetDeclaredForeignKeys(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}
			refFks, err := fkt.GetReferencedForeignKeys(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}
			for _, fk := range append(decFks, refFks...) {
				if strings.ToLower(fk.Name) == strings.ToLower(dropConstraint.Name) {
					n, err = plan.NewAlterDropForeignKey(rt.SqlDatabase.Name(), rt.Table.Name(), dropConstraint.Name).
						WithDatabaseProvider(a.Catalog.DbProvider)
					return n, transform.NewTree, err
				}
			}
		}

		ct, ok := table.(sql.CheckTable)
		if !ok {
			return nil, transform.SameTree, plan.ErrNoCheckConstraintSupport.New(table.Name())
		}

		checks, err := ct.GetChecks(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		for _, check := range checks {
			if strings.ToLower(check.Name) == strings.ToLower(dropConstraint.Name) {
				return plan.NewAlterDropCheck(rt, check.Name), transform.NewTree, nil
			}
		}

		if dropConstraint.IfExists {
			newAlterDropCheck := plan.NewAlterDropCheck(rt, dropConstraint.Name)
			newAlterDropCheck.IfExists = true
			return newAlterDropCheck, transform.NewTree, nil
		}

		if ia, ok := table.(sql.IndexAddressable); ok {
			indexes, err := ia.GetIndexes(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}

			for _, index := range indexes {
				if index.IsUnique() {
					if index.ID() == dropConstraint.Name {
						newDropIndex := plan.NewDropIndex(dropConstraint.Name, rt)
						newDropIndex.Catalog = a.Catalog
						newDropIndex.CurrentDatabase = rt.Database().Name()
						return newDropIndex, transform.NewTree, nil
					}
				}
			}
		}

		return nil, transform.SameTree, sql.ErrUnknownConstraint.New(dropConstraint.Name)
	})
}

// validateDropConstraint returns an error if the constraint named to be dropped doesn't exist
func validateDropConstraint(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	validate := func(dc *plan.DropCheck, seenChecks map[string]bool) error {
		// Don't bother validating that the constraint exists if the IfExists flag is set
		if dc.IfExists {
			return nil
		}

		rt := dc.Table

		ct, ok := sql.GetUnderlyingTable(rt.Table).(sql.CheckTable)
		if ok {
			checks, err := ct.GetChecks(ctx)
			if err != nil {
				return err
			}

			lowerName := strings.ToLower(dc.Name)
			if seenChecks != nil && seenChecks[lowerName] {
				return sql.ErrUnknownConstraint.New(dc.Name)
			}
			for _, check := range checks {
				if strings.ToLower(check.Name) == lowerName {
					if seenChecks != nil {
						seenChecks[lowerName] = true
					}
					return nil
				}
			}

			return sql.ErrUnknownConstraint.New(dc.Name)
		}

		return plan.ErrNoCheckConstraintSupport.New(rt.Table.Name())
	}

	switch n := n.(type) {
	case *plan.DropCheck:
		if err := validate(n, nil); err != nil {
			return nil, transform.SameTree, err
		}
		return n, transform.SameTree, nil
	case *plan.Block:
		if !n.IsMultiAlter {
			return n, transform.SameTree, nil
		}
		seenChecks := map[string]bool{}
		for _, stmt := range n.Children() {
			if dc, ok := stmt.(*plan.DropCheck); ok {
				if err := validate(dc, seenChecks); err != nil {
					return nil, transform.SameTree, err
				}
			}
		}
		return n, transform.SameTree, nil
	default:
		return n, transform.SameTree, nil
	}
}
