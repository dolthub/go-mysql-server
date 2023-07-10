// Copyright 2023 Dolthub, Inc.
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

package expression

import (
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/types"
)

//TODO: doc
type MatchAgainst struct {
	Columns        []sql.Expression
	Expr           sql.Expression
	SearchModifier fulltext.FullTextSearchModifier

	idIndex        sql.Index
	ftIndex        fulltext.FulltextIndex
	docIdColPos    int
	parentTable    sql.IndexAddressableTable
	configTable    sql.IndexAddressableTable
	wordToPosTable sql.IndexAddressableTable
	countTable     sql.IndexAddressableTable
}

var _ sql.Expression = (*MatchAgainst)(nil)

//TODO: doc
func NewMatchAgainst(columns []sql.Expression, expr sql.Expression, searchModifier fulltext.FullTextSearchModifier) *MatchAgainst {
	return &MatchAgainst{
		Columns:        columns,
		Expr:           expr,
		SearchModifier: searchModifier,
		idIndex:        nil,
		ftIndex:        nil,
		docIdColPos:    0,
		parentTable:    nil,
		configTable:    nil,
		wordToPosTable: nil,
		countTable:     nil,
	}
}

// Children implements sql.Expression
func (expr *MatchAgainst) Children() []sql.Expression {
	exprs := make([]sql.Expression, len(expr.Columns)+1)
	copy(exprs, expr.Columns)
	exprs[len(exprs)-1] = expr.Expr
	return exprs
}

// Eval implements sql.Expression
func (expr *MatchAgainst) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	switch expr.SearchModifier {
	case fulltext.FullTextSearchModifier_NaturalLanguage:
		return expr.inNaturalLanguageMode(ctx, row)
	case fulltext.FullTextSearchModifier_NaturalLangaugeQueryExpansion:
		return expr.inNaturalLanguageModeWithQueryExpansion(ctx, row)
	case fulltext.FullTextSearchModifier_Boolean:
		return expr.inBooleanMode(ctx, row)
	case fulltext.FullTextSearchModifier_QueryExpansion:
		return expr.withQueryExpansion(ctx, row)
	default:
		panic("invalid MATCH...AGAINST search modifier")
	}
}

// IsNullable implements sql.Expression
func (expr *MatchAgainst) IsNullable() bool {
	return false
}

// Resolved implements sql.Expression
func (expr *MatchAgainst) Resolved() bool {
	for _, col := range expr.Columns {
		if !col.Resolved() {
			return false
		}
	}
	return expr.Expr.Resolved()
}

// String implements sql.Expression
func (expr *MatchAgainst) String() string {
	var searchModifierStr string
	switch expr.SearchModifier {
	case fulltext.FullTextSearchModifier_NaturalLanguage:
		searchModifierStr = "IN NATURAL LANGUAGE MODE"
	case fulltext.FullTextSearchModifier_NaturalLangaugeQueryExpansion:
		searchModifierStr = "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"
	case fulltext.FullTextSearchModifier_Boolean:
		searchModifierStr = "IN BOOLEAN MODE"
	case fulltext.FullTextSearchModifier_QueryExpansion:
		searchModifierStr = "WITH QUERY EXPANSION"
	default:
		panic("invalid MATCH...AGAINST search modifier")
	}
	columns := make([]string, len(expr.Columns))
	for i := range expr.Columns {
		columns[i] = expr.Columns[i].String()
	}
	return fmt.Sprintf("MATCH (%s) AGAINST (%s %s)", strings.Join(columns, ","), expr.Expr.String(), searchModifierStr)
}

// Type implements sql.Expression
func (expr *MatchAgainst) Type() sql.Type {
	return types.Float32
}

// WithChildren implements sql.Expression
func (expr *MatchAgainst) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(expr.Columns)+1 {
		return nil, sql.ErrInvalidChildrenNumber.New(expr, len(children), len(expr.Columns)+1)
	}
	columns := make([]sql.Expression, len(children)-1)
	copy(columns, children)
	return &MatchAgainst{
		Columns:        columns,
		Expr:           children[len(children)-1],
		SearchModifier: expr.SearchModifier,
		idIndex:        expr.idIndex,
		ftIndex:        expr.ftIndex,
		docIdColPos:    expr.docIdColPos,
		parentTable:    expr.parentTable,
		configTable:    expr.configTable,
		wordToPosTable: expr.wordToPosTable,
		countTable:     expr.countTable,
	}, nil
}

// WithInfo returns a new *MatchAgainst with the given tables and other needed information to perform matching.
func (expr *MatchAgainst) WithInfo(parent, config, wordToPos, count sql.IndexAddressableTable, docIdColPos int) sql.Expression {
	return &MatchAgainst{
		Columns:        expr.Columns,
		Expr:           expr.Expr,
		SearchModifier: expr.SearchModifier,
		idIndex:        expr.idIndex,
		ftIndex:        expr.ftIndex,
		docIdColPos:    docIdColPos,
		parentTable:    parent,
		configTable:    config,
		wordToPosTable: wordToPos,
		countTable:     count,
	}
}

// GetIndexes returns the document ID's UNIQUE index and relevant Full-Text index for this expression, or nil if either
// index has not yet been set.
func (expr *MatchAgainst) GetIndexes() (documentIdIndex sql.Index, fulltextIndex fulltext.FulltextIndex) {
	return expr.idIndex, expr.ftIndex
}

// SetIndexes sets the indexes for this expression. This does not create and return a new expression, which differs from
// the "With" functions.
func (expr *MatchAgainst) SetIndexes(documentIdIndex sql.Index, fulltextIndex fulltext.FulltextIndex) {
	if documentIdIndex == nil || fulltextIndex == nil {
		return
	}
	expr.idIndex = documentIdIndex
	expr.ftIndex = fulltextIndex
}

// ColumnsAsGetFields returns the columns as *GetField expressions. If the columns have not yet been resolved, then this
// returns a nil (empty) slice.
func (expr *MatchAgainst) ColumnsAsGetFields() []*GetField {
	var ok bool
	fields := make([]*GetField, len(expr.Columns))
	for i, col := range expr.Columns {
		fields[i], ok = col.(*GetField)
		if !ok {
			return nil
		}
	}
	return fields
}

// inNaturalLanguageMode calculates the result using "IN NATURAL LANGUAGE MODE" (default mode).
func (expr *MatchAgainst) inNaturalLanguageMode(ctx *sql.Context, row sql.Row) (float32, error) {
	//TODO: TEMPORARY implementation that only validates if the row is valid.
	// Actual solution will make use of some flavor of https://en.wikipedia.org/wiki/Tf%E2%80%93idf
	// I'm attempting to reverse engineer MySQL's exact formula for consistency, but may need to just make my own approximation.
	// None of this code will remain in the future, so this function can be ignored for the purposes of reviewing.
	words, err := expr.Expr.Eval(ctx, row)
	if err != nil {
		return 0, err
	}
	wordsStr, ok := words.(string)
	if !ok {
		return 0, fmt.Errorf("expected WORD to be a string, but had type `%T`", words)
	}
	indexes, err := expr.wordToPosTable.GetIndexes(ctx)
	if err != nil {
		return 0, err
	}
	if len(indexes) == 0 {
		return 0, fmt.Errorf("expected to find a primary key on the table `%s`", expr.wordToPosTable.Name())
	}

	for _, wordStr := range strings.Split(wordsStr, " ") {
		lookup := sql.IndexLookup{Ranges: []sql.Range{
			{
				sql.ClosedRangeColumnExpr(wordStr, wordStr, expr.wordToPosTable.Schema()[0].Type),
				sql.ClosedRangeColumnExpr(row[expr.docIdColPos], row[expr.docIdColPos], expr.wordToPosTable.Schema()[1].Type),
			},
		}, Index: indexes[0]}

		editorData := expr.wordToPosTable.IndexedAccess(lookup)
		partIter, err := editorData.LookupPartitions(ctx, lookup)
		if err != nil {
			return 0, err
		}

		rowIter := sql.NewTableRowIter(ctx, editorData, partIter)
		defer rowIter.Close(ctx)

		_, err = rowIter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				// This did not match, so we continue
				continue
			}
			return 0, err
		}
		// We found a match, so this was successful
		return 1, nil
	}
	// No words matched, so return 0
	return 0, nil
}

// inNaturalLanguageModeWithQueryExpansion calculates the result using "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION".
func (expr *MatchAgainst) inNaturalLanguageModeWithQueryExpansion(ctx *sql.Context, row sql.Row) (float32, error) {
	return 0, fmt.Errorf("'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION' has not yet been implemented")
}

// inBooleanMode calculates the result using "IN BOOLEAN MODE".
func (expr *MatchAgainst) inBooleanMode(ctx *sql.Context, row sql.Row) (float32, error) {
	return 0, fmt.Errorf("'IN BOOLEAN MODE' has not yet been implemented")
}

// withQueryExpansion calculates the result using "WITH QUERY EXPANSION".
func (expr *MatchAgainst) withQueryExpansion(ctx *sql.Context, row sql.Row) (float32, error) {
	return 0, fmt.Errorf("'WITH QUERY EXPANSION' has not yet been implemented")
}
