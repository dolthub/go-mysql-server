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

package fixidx

import (
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ErrFieldMissing is returned when the field is not on the schema.
var ErrFieldMissing = errors.NewKind("field %q is not on schema")

// FixFieldIndexes transforms the given expression by correcting the indexes of columns in GetField expressions,
// according to the schema given. Used when combining multiple tables together into a single join result, or when
// otherwise changing / combining schemas in the node tree.
func FixFieldIndexes(scope *plan.Scope, logFn func(string, ...any), schema sql.Schema, exp sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
	scopeLen := len(scope.Schema())

	return transform.Expr(exp, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		// For each GetField expression, re-index it with the appropriate index from the schema.
		case *expression.GetField:
			partial := -1
			for i, col := range schema {
				newIndex := scopeLen + i
				if strings.EqualFold(e.Name(), col.Name) && strings.EqualFold(e.Table(), col.Source) {
					if e.Table() == "" && e.Name() != col.Name {
						// aliases with same lowered representation need to case-sensitive match
						partial = newIndex
						continue
					}
					if newIndex != e.Index() {
						if logFn != nil {
							logFn("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
						}
						return e.WithIndex(newIndex), transform.NewTree, nil
					}
					return e, transform.SameTree, nil
				}
			}
			if partial >= 0 {
				if partial != e.Index() {
					if logFn != nil {
						logFn("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), partial)
					}
					return e.WithIndex(partial), transform.NewTree, nil
				}
				return e, transform.SameTree, nil
			}

			// If we didn't find the column in the schema of the node itself, look outward in surrounding scopes. Work
			// inner-to-outer, in  accordance with MySQL scope naming precedence rules.
			offset := 0
			for _, n := range scope.InnerToOuter() {
				schema := Schemas(n.Children())
				offset += len(schema)
				for i, col := range schema {
					if strings.EqualFold(e.Name(), col.Name) && strings.EqualFold(e.Table(), col.Source) {
						newIndex := scopeLen - offset + i
						if e.Index() != newIndex {
							if logFn != nil {
								logFn("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
							}
							return e.WithIndex(newIndex), transform.NewTree, nil
						}
						return e, transform.SameTree, nil
					}
				}
			}

			return nil, transform.SameTree, ErrFieldMissing.New(e.Name())
		}

		return e, transform.SameTree, nil
	})
}

// Schemas returns the Schemas for the nodes given appended in to a single one
func Schemas(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema()...)
	}
	return schema
}
