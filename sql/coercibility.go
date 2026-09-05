// Copyright 2023-2026 Dolthub, Inc.
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

package sql

// Coercibility values define the precedence order for character set
// and collation resolution. Lower values have higher precedence.
//
// See [coercibility-docs] and [derivation-ref].
//
// [derivation-ref]: https://github.com/mysql/mysql-server/blob/e174239c5b3c2bcf164649042ab8a7fc972ce88d/sql/field.h#L176-L185
// [coercibility-docs]: https://dev.mysql.com/doc/refman/8.4/en/charset-collation-coercibility.html
const (
	// CoercibilityExplicit indicates an explicit COLLATE clause.
	CoercibilityExplicit byte = 0
	// CoercibilityNone indicates conflicting collations of
	// equal strength.
	CoercibilityNone byte = 1
	// CoercibilityImplicit indicates a table column or stored variable.
	CoercibilityImplicit byte = 2
	// CoercibilitySysConst indicates a system constant or function.
	CoercibilitySysConst byte = 3
	// CoercibilityCoercible indicates a string literal.
	CoercibilityCoercible byte = 4
	// CoercibilityNumeric indicates numeric or temporal value converted
	// to string.
	CoercibilityNumeric byte = 5
	// CoercibilityIgnorable indicates a NULL literal.
	CoercibilityIgnorable byte = 6
	// CoercibilityUnknown indicates an uninitialized or fallback
	// coercibility.
	CoercibilityUnknown byte = 7
)

// CollationCoercible represents the coercibility of an expression or node. Although the resulting value from the node
// or expression may be NULL, this interface returns the coercibility as though a NULL would not be returned.
type CollationCoercible interface {
	// CollationCoercibility returns the collation and coercibility of the expression or node.
	CollationCoercibility(ctx *Context) (collation CollationID, coercibility byte)
}

// ResolveCoercibility returns the dominant collation and coercibility
// between two operands according to coercibility rules.
//
// When coercibilities differ, the operand with lower coercibility
// (higher precedence) wins. When coercibilities match, binary
// character sets take precedence over non-binary character sets.
// Two explicit collations that differ produce CoercibilityNone.
//
// See [coercibility-docs] and [aggregation-ref].
//
// [coercibility-docs]: https://dev.mysql.com/doc/refman/8.4/en/charset-collation-coercibility.html
// [aggregation-ref]: https://github.com/mysql/mysql-server/blob/e174239c5b3c2bcf164649042ab8a7fc972ce88d/sql/item.cc#L2759
func ResolveCoercibility(leftCollation CollationID, leftCoercibility byte, rightCollation CollationID, rightCoercibility byte) (CollationID, byte) {
	if leftCoercibility < rightCoercibility {
		return leftCollation, leftCoercibility
	} else if leftCoercibility > rightCoercibility {
		return rightCollation, rightCoercibility
	} else if leftCollation == rightCollation {
		return leftCollation, leftCoercibility
	} else if leftCollation == Collation_Unspecified {
		return rightCollation, rightCoercibility
	} else if rightCollation == Collation_Unspecified {
		return leftCollation, leftCoercibility
	}

	// TODO(elianddb): Support bubbling up collation errors when both
	// operands are explicit.
	if leftCoercibility == CoercibilityExplicit && rightCoercibility == CoercibilityExplicit {
		return Collation_binary, CoercibilityNone
	}

	leftCharset := leftCollation.CharacterSet()
	rightCharset := rightCollation.CharacterSet()

	if leftCharset == CharacterSet_binary && rightCharset != CharacterSet_binary {
		return leftCollation, leftCoercibility
	}
	if rightCharset == CharacterSet_binary && leftCharset != CharacterSet_binary {
		return rightCollation, rightCoercibility
	}

	if leftCharset != rightCharset {
		// TODO(elianddb): Implement full charset superset conversion
		// (MY_COLL_ALLOW_SUPERSET_CONV).
		// TODO(elianddb): Implement constant string charset conversion
		// (MY_COLL_ALLOW_COERCIBLE_CONV) during analyzer planning.
		if leftCharset.MaxLength() == 1 && rightCharset.MaxLength() > 1 {
			return rightCollation, rightCoercibility
		} else if leftCharset.MaxLength() > 1 && rightCharset.MaxLength() == 1 {
			return leftCollation, leftCoercibility
		}
		// TODO(elianddb): Differentiate function string results
		// (MY_COLL_ALLOW_NONE) from comparisons (MY_COLL_CMP_CONV)
		// which reject CoercibilityNone.
		return Collation_binary, CoercibilityNone
	}

	leftIsBin := leftCollation.IsBinary()
	rightIsBin := rightCollation.IsBinary()
	if leftIsBin != rightIsBin {
		if leftIsBin {
			return leftCollation, leftCoercibility
		}
		return rightCollation, rightCoercibility
	}

	binCol := leftCharset.BinaryCollation()
	if binCol == Collation_Unspecified {
		binCol = Collation_binary
	}
	// TODO(elianddb): Differentiate function string results
	// (MY_COLL_ALLOW_NONE) from comparisons which reject
	// CoercibilityNone.
	return binCol, CoercibilityNone
}

// GetCoercibility returns the coercibility of the given node or expression.
//
// TODO(elianddb): Analyzer should lock and assign collations to
// nodes during planning rather than inferring them dynamically from
// children.
func GetCoercibility(ctx *Context, nodeOrExpr interface{}) (collation CollationID, coercibility byte) {
	if nodeOrExpr == nil {
		return Collation_binary, CoercibilityIgnorable
	}
	if cc, ok := nodeOrExpr.(CollationCoercible); ok {
		return cc.CollationCoercibility(ctx)
	}
	collation = Collation_binary
	coercibility = CoercibilityUnknown
	if n, ok := nodeOrExpr.(Node); ok {
		for _, child := range n.Children() {
			nextCollation, nextCoercibility := GetCoercibility(ctx, child)
			collation, coercibility = ResolveCoercibility(collation, coercibility, nextCollation, nextCoercibility)
		}
	}
	if e, ok := nodeOrExpr.(Expressioner); ok {
		for _, child := range e.Expressions() {
			nextCollation, nextCoercibility := GetCoercibility(ctx, child)
			collation, coercibility = ResolveCoercibility(collation, coercibility, nextCollation, nextCoercibility)
		}
	}
	if e, ok := nodeOrExpr.(Expression); ok {
		for _, child := range e.Children() {
			nextCollation, nextCoercibility := GetCoercibility(ctx, child)
			collation, coercibility = ResolveCoercibility(collation, coercibility, nextCollation, nextCoercibility)
		}
	}
	return collation, coercibility
}

// ResolveCoercibilityExpressions returns the combined collation and
// coercibility across a slice of expressions.
//
// It evaluates each expression in order and reduces them using
// ResolveCoercibility. Empty slices return Collation_binary with
// CoercibilityIgnorable.
//
// TODO(elianddb): Support MY_COLL_ALLOW_NUMERIC_CONV in string
// functions when all arguments are numeric.
//
// See [coercibility-docs] and [multi-agg-ref].
//
// [coercibility-docs]: https://dev.mysql.com/doc/refman/8.4/en/charset-collation-coercibility.html
// [multi-agg-ref]: https://github.com/mysql/mysql-server/blob/e174239c5b3c2bcf164649042ab8a7fc972ce88d/sql/item.cc#L2882
func ResolveCoercibilityExpressions(ctx *Context, exprs ...Expression) (CollationID, byte) {
	if len(exprs) == 0 {
		return Collation_binary, CoercibilityIgnorable
	}
	collation, coercibility := GetCoercibility(ctx, exprs[0])
	for i := 1; i < len(exprs); i++ {
		nextCollation, nextCoercibility := GetCoercibility(ctx, exprs[i])
		collation, coercibility = ResolveCoercibility(
			collation, coercibility, nextCollation, nextCoercibility,
		)
	}
	return collation, coercibility
}
