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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveDropConstraint replaces DropConstraint nodes with a concrete type of alter table node as appropriate, or
// throws a constraint not found error if the named constraint isn't found on the table given.
func resolveDropConstraint(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	dropConstraint, ok := n.(*plan.DropConstraint)
	if !ok {
		return n, nil
	}

	rt, ok := dropConstraint.Child.(*plan.ResolvedTable)
	if !ok {
		return nil, ErrInAnalysis.New("Expected a ResolvedTable for ALTER TABLE DROP CONSTRAINT statement")
	}

	table := rt.Table
	fkt, ok := table.(sql.ForeignKeyTable)
	if ok {
		fks, err := fkt.GetForeignKeys(ctx)
		if err != nil {
			return nil, err
		}

		for _, fk := range fks {
			if strings.ToLower(fk.Name) == strings.ToLower(dropConstraint.Name) {
				return plan.NewAlterDropForeignKey(rt, dropConstraint.Name), nil
			}
		}
	}

	ct, ok := table.(sql.CheckTable)
	if ok {
		checks, err := ct.GetChecks(ctx)
		if err != nil {
			return nil, err
		}

		for _, check := range checks {
			if strings.ToLower(check.Name) == strings.ToLower(dropConstraint.Name) {
				return plan.NewAlterDropCheck(rt, check.Name), nil
			}
		}
	}

	return nil, sql.ErrUnknownConstraint.New(dropConstraint.Name)
}

// validateDropConstraint returns an error if the constraint named to be dropped doesn't exist
func validateDropConstraint(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.DropForeignKey:
		rt, ok := n.Child.(*plan.ResolvedTable)
		if !ok {
			return nil, ErrInAnalysis.New("Expected a ResolvedTable for ALTER TABLE DROP CONSTRAINT statement")
		}

		fkt, ok := rt.Table.(sql.ForeignKeyTable)
		if ok {
			fks, err := fkt.GetForeignKeys(ctx)
			if err != nil {
				return nil, err
			}

			for _, fk := range fks {
				if strings.ToLower(fk.Name) == strings.ToLower(n.Name) {
					return n, nil
				}
			}

			return nil, sql.ErrUnknownConstraint.New(n.Name)
		}

		return nil, plan.ErrNoForeignKeySupport.New(rt.Table.Name())
	case *plan.DropCheck:
		rt, ok := n.Child.(*plan.ResolvedTable)
		if !ok {
			return nil, ErrInAnalysis.New("Expected a ResolvedTable for ALTER TABLE DROP CONSTRAINT statement")
		}

		ct, ok := rt.Table.(sql.CheckTable)
		if ok {
			checks, err := ct.GetChecks(ctx)
			if err != nil {
				return nil, err
			}

			for _, check := range checks {
				if strings.ToLower(check.Name) == strings.ToLower(n.Name) {
					return n, nil
				}
			}

			return nil, sql.ErrUnknownConstraint.New(n.Name)
		}

		return nil, plan.ErrNoCheckConstraintSupport.New(rt.Table.Name())
	default:
		return n, nil
	}
}
