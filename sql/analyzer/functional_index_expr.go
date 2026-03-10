// Copyright 2026 Dolthub, Inc.
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
	"reflect"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
)

// indexedExprEntry holds a hidden system column's generated expression and the column name
// that backs it. Used to match filter expressions against indexed functional expressions.
type indexedExprEntry struct {
	expr    sql.Expression
	colName string
}

// newIndexedExprEntry constructs an indexedExprEntry from a resolved expression tree.
// The expression must not be an UnresolvedColumnDefault; callers are responsible for
// resolving the schema before calling this (see buildIndexedExprToColumnNameMap).
// No normalization is needed at storage time: expressionsEqual handles table qualifier
// and quoteName differences inline during matching.
func newIndexedExprEntry(expr sql.Expression, colName string) indexedExprEntry {
	return indexedExprEntry{expr: expr, colName: colName}
}

// matches reports whether the given filter expression refers to the same functional expression
// as this entry. Comparison is structural via expressionsEqual, which ignores table qualifiers
// and quoteName flags on GetField nodes.
func (e indexedExprEntry) matches(filter sql.Expression) bool {
	return expressionsEqual(e.expr, filter)
}

// buildIndexedExprToColumnNameMap builds a slice of indexed functional expressions available
// from the specified |indexes|. Each entry pairs the expression tree for the indexed functional
// expression with the hidden system column name that generates it. This function is used as
// part of planning/costing index scans.
func buildIndexedExprToColumnNameMap(ctx *sql.Context, cat sql.Catalog, indexes []sql.Index, rt sql.TableNode) ([]indexedExprEntry, error) {
	var indexedExprs []indexedExprEntry
	tableName := rt.UnderlyingTable().Name()

	// We don't support indexed functional expressions on TableFunctions
	if _, ok := rt.(sql.TableFunction); ok {
		return indexedExprs, nil
	}

	// Use the schema from the table node already in the query plan, not from cat.Table() —
	// the catalog would return raw unresolved expressions from storage (e.g. in Dolt,
	// hidden system column expressions would still be UnresolvedColumnDefault strings).
	// resolveTableSchema handles lazy resolution of any remaining placeholders.
	sch := resolveTableSchema(ctx, cat, rt)

	for _, idx := range indexes {
		for _, qualifiedColName := range idx.Expressions() {
			unqualifiedColName := strings.TrimPrefix(qualifiedColName, tableName+".")
			columnIdx := sch.IndexOfColName(unqualifiedColName)
			if columnIdx < 0 {
				// The column may not appear in a projected schema (e.g. inside a subquery
				// that only selects a subset of columns). Non-hidden columns can be safely
				// skipped here — we only care about HiddenSystem columns.
				continue
			}
			if sch[columnIdx].HiddenSystem && sch[columnIdx].Generated != nil {
				indexedExprs = append(indexedExprs, newIndexedExprEntry(sch[columnIdx].Generated.Expr, unqualifiedColName))
			}
		}
	}
	return indexedExprs, nil
}

// buildColumnIdToIndexedExprMap builds a map of sql.ColumnId to the indexed expression they
// generate. Each key in the returned map is the sql.ColumnId of a hidden system column that
// generates an expression included in a secondary index. Each value is a pre-normalized
// indexedExprEntry for efficient match-time comparison. This function is used for planning/costing joins.
func buildColumnIdToIndexedExprMap(ctx *sql.Context, cat sql.Catalog, tableNode sql.TableNode, indexes []*memo.Index) map[sql.ColumnId]indexedExprEntry {
	if tableNode == nil {
		return nil
	}

	result := make(map[sql.ColumnId]indexedExprEntry)
	sch := resolveTableSchema(ctx, cat, tableNode)

	for _, idx := range indexes {
		cols := idx.Cols()
		if len(idx.SqlIdx().Expressions()) != len(cols) {
			continue
		}
		for i, qualifiedColName := range idx.SqlIdx().Expressions() {
			unqualifiedColName := strings.TrimPrefix(qualifiedColName, idx.SqlIdx().Table()+".")
			schIdx := sch.IndexOfColName(unqualifiedColName)
			if schIdx < 0 {
				continue
			}
			col := sch[schIdx]
			if col.HiddenSystem && col.Generated != nil {
				result[cols[i]] = newIndexedExprEntry(col.Generated.Expr, unqualifiedColName)
			}
		}
	}
	return result
}

// expressionsEqual reports whether two sql.Expression trees are structurally equal
// for the purpose of functional index matching. Table qualifiers and quoteName flags
// on GetField nodes are ignored so that expressions using different table aliases match.
// This function does not currently consider expressions such as 10*c1 and c1*10 as
// equivalent, but this could be extended in the future to allow for more usage of
// functional expression indexes.
//
// TODO: this function is a candidate for eventual promotion to a package-level utility
// in the sql or expression package (e.g., expression.ExprEquals). Do NOT add Equals to
// the sql.Expression interface itself — unlike sql.Type.Equals, expression equality has
// no single unambiguous semantics: GetField.IsSameField includes the table qualifier;
// this function ignores it. NOW() is structurally self-equal but never value-equal.
// Any interface-level Equals would have to pick one semantic and be wrong for other
// callers. If more call sites emerge, a standalone function or an optional
// ExpressionComparer interface is likely a better path.
func expressionsEqual(a, b sql.Expression) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}

	switch av := a.(type) {
	case *expression.GetField:
		// Ignore table qualifier and quoteName — only the column name matters for
		// functional index matching. This is intentionally different from
		// GetField.IsSameField, which includes the table name.
		return strings.EqualFold(av.Name(), b.(*expression.GetField).Name())
	case *expression.Literal:
		// String() comparison is robust to numeric type coercion differences between
		// stored and filter expressions (e.g. int32(10) vs int64(10) both produce "10").
		return av.String() == b.String()
	}

	// For arithmetic-like nodes where a single Go type encodes multiple operators:
	// Arithmetic uses one struct type for +, -, and *, distinguished by the Op field.
	// Div, Mod, and IntDiv are each their own type (Operator() returns a fixed string),
	// so the reflect.TypeOf check above already covers them — but checking Operator()
	// here is harmless and future-proof against similar polymorphic types.
	if ao, ok := a.(interface{ Operator() string }); ok {
		if ao.Operator() != b.(interface{ Operator() string }).Operator() {
			return false
		}
	}

	// Recurse into children uniformly for all other node types (functions, comparisons,
	// conversions, etc.). The reflect.TypeOf check above ensures the node types match,
	// so Children() returns semantically parallel slices.
	aChildren := a.Children()
	bChildren := b.Children()
	if len(aChildren) != len(bChildren) {
		return false
	}
	for i := range aChildren {
		if !expressionsEqual(aChildren[i], bChildren[i]) {
			return false
		}
	}
	return true
}

// resolveTableSchema returns tableNode's schema with any UnresolvedColumnDefault expressions
// in hidden system columns resolved to real expression trees. If no unresolved expressions are
// present, the original schema slice is returned unchanged (no allocation).
//
// In Dolt, schemas are loaded from storage before a SQL context is available, so generated
// expressions are stored as UnresolvedColumnDefault placeholders. This function resolves them
// lazily: it only invokes planbuilder.ResolveSchemaDefaults when at least one hidden column
// still carries an unresolved placeholder.
func resolveTableSchema(ctx *sql.Context, cat sql.Catalog, tableNode sql.TableNode) sql.Schema {
	sch := tableNode.Schema(ctx)
	for _, col := range sch {
		if col.HiddenSystem && col.Generated != nil {
			if _, ok := col.Generated.Expr.(*sql.UnresolvedColumnDefault); ok {
				var dbName string
				if dbTab, ok := tableNode.UnderlyingTable().(sql.Databaseable); ok {
					dbName = strings.ToLower(dbTab.Database())
				} else if dber, ok := tableNode.UnderlyingTable().(sql.Databaser); ok {
					dbName = dber.Database().Name()
				}
				b := planbuilder.New(ctx, cat, nil)
				return b.ResolveSchemaDefaults(dbName, tableNode.UnderlyingTable().Name(), sch)
			}
		}
	}
	return sch
}
