// Copyright 2022 Dolthub, Inc.
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
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

// CollatedExpression represents an expression (returning a string or byte slice) that carries a collation (which
// implicitly also carries a character set). This does not handle any encoding or decoding of the character set, as this
// is strictly for collations.
type CollatedExpression struct {
	expr      sql.Expression
	collation sql.CollationID
}

var _ sql.Expression = (*CollatedExpression)(nil)
var _ sql.DebugStringer = (*CollatedExpression)(nil)

// NewCollatedExpression creates a new CollatedExpression expression. If the given expression is already a
// CollatedExpression, then the previous collation is overriden with the given one.
func NewCollatedExpression(expr sql.Expression, collation sql.CollationID) *CollatedExpression {
	if collatedExpr, ok := expr.(*CollatedExpression); ok {
		return &CollatedExpression{
			expr:      collatedExpr.expr,
			collation: collation,
		}
	}
	return &CollatedExpression{
		expr:      expr,
		collation: collation,
	}
}

// Resolved implements the sql.Expression interface.
func (ce *CollatedExpression) Resolved() bool {
	return ce.expr.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (ce *CollatedExpression) IsNullable() bool {
	return ce.expr.IsNullable()
}

// Type implements the sql.Expression interface.
func (ce *CollatedExpression) Type() sql.Type {
	typ := ce.expr.Type()
	if collatedType, ok := typ.(sql.TypeWithCollation); ok {
		newType, err := collatedType.WithNewCollation(ce.collation)
		if err == nil {
			return newType
		}
	}
	// If this isn't a collated type then this should fail, as we can't apply a collation to an expression that does not
	// have a charset. We also can't check in the constructor, as expressions such as unresolved columns will not have
	// the correct type until after analysis. Therefore, we'll have to check (and potentially fail) in the Eval function.
	return typ
}

// Eval implements the sql.Expression interface.
func (ce *CollatedExpression) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := ce.expr.Type()
	if !types.IsText(typ) {
		return nil, sql.ErrCollatedExprWrongType.New()
	}
	if ce.collation.CharacterSet() != typ.(sql.TypeWithCollation).Collation().CharacterSet() {
		return nil, sql.ErrCollationInvalidForCharSet.New(
			ce.collation.Name(), typ.(sql.TypeWithCollation).Collation().CharacterSet().Name())
	}
	return ce.expr.Eval(ctx, row)
}

func (ce *CollatedExpression) String() string {
	return fmt.Sprintf("%s COLLATE %s", ce.expr.String(), ce.collation.String())
}

// DebugString implements the sql.DebugStringer interface.
func (ce *CollatedExpression) DebugString() string {
	var innerDebugStr string
	if debugExpr, ok := ce.expr.(sql.DebugStringer); ok {
		innerDebugStr = debugExpr.DebugString()
	} else {
		innerDebugStr = ce.expr.String()
	}
	return fmt.Sprintf("%s COLLATE %s", innerDebugStr, ce.collation.String())
}

// WithChildren implements the sql.Expression interface.
func (ce *CollatedExpression) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ce, len(children), 1)
	}
	return &CollatedExpression{
		expr:      children[0],
		collation: ce.collation,
	}, nil
}

// Children implements the sql.Expression interface.
func (ce *CollatedExpression) Children() []sql.Expression {
	return []sql.Expression{ce.expr}
}

// Child returns the inner expression.
func (ce *CollatedExpression) Child() sql.Expression {
	return ce.expr
}

// GetCollationViaCoercion returns the collation and coercibility value that best represents the expression. This is
// determined by the rules of coercibility as defined by MySQL
// (https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html). In short, the lower the value of the
// returned integer, the more explicit the defined collation. A value of 0 indicates that an explicit COLLATE was given.
// Returns sql.Collation_Unspecified if the expression in invalid in some way.
//
// TODO: This function's implementation is extremely basic, and is sure to return an incorrect result in some cases. A
// more accurate implementation would have each expression return its own collation and coercion values.
func GetCollationViaCoercion(expr sql.Expression) (sql.CollationID, int) {
	if expr == nil {
		return sql.Collation_Default, 6
	}
	collation := sql.Collation_Default
	if typeWithCollation, ok := expr.Type().(sql.TypeWithCollation); ok {
		collation = typeWithCollation.Collation()
	} else {
		// From the docs (which seems applicable): The collation of a numeric or temporal value has a coercibility of 5.
		return sql.Collation_Default, 5
	}

	switch expr.(type) {
	case *CollatedExpression:
		return collation, 0
	case *GetField, *ProcedureParam, *UserVar, *SystemVar:
		return collation, 2
	case *Literal:
		return collation, 4
	default:
		if funcExpr, ok := expr.(sql.FunctionExpression); ok {
			switch funcExpr.FunctionName() {
			case "concat":
				coercibility := 6
				var childrenWithCoercibility []sql.CollationID
				for _, child := range funcExpr.Children() {
					childCollation, childCoercibility := GetCollationViaCoercion(child)
					if childCollation == sql.Collation_Unspecified {
						continue
					}
					if childCoercibility < coercibility {
						childrenWithCoercibility = childrenWithCoercibility[:0] // Reset slice while retaining array
						coercibility = childCoercibility
					}
					if childCoercibility == coercibility {
						childrenWithCoercibility = append(childrenWithCoercibility, childCollation)
					}
				}

				if len(childrenWithCoercibility) == 0 {
					return sql.Collation_Default, 1 // This should never happen, but we're checking just in case
				}
				// Check if all children have the same character set, and apply the _bin precedence rule
				charset := childrenWithCoercibility[0].CharacterSet()
				collation = childrenWithCoercibility[0]
				for i := 1; i < len(childrenWithCoercibility); i++ {
					childCollation := childrenWithCoercibility[i]
					//TODO: If one character set is Unicode and the other is non-Unicode, we shouldn't error but should
					// instead use the Unicode character set
					if childCollation.CharacterSet() != charset {
						return sql.Collation_Unspecified, 6
					}
					if !strings.HasSuffix(collation.Name(), "_bin") && strings.HasSuffix(childCollation.Name(), "_bin") {
						collation = childCollation
					}
				}

				return collation, 1
			case "user", "current_user", "version":
				return collation, 3
			default:
				// It appears that many functions return a value of 4, so it's the default value (using the function COERCIBILITY).
				// As a special rule, we inspect the expression tree of unknown expressions. This is not what MySQL does,
				// but it's a better approximation of the behavior than just checking the top of the tree at all times.
				coercibility := 4
				inspectExpression(funcExpr, func(expr sql.Expression) {
					switch expr.(type) {
					case *CollatedExpression:
						coercibility = 0
					case *GetField, *ProcedureParam, *UserVar, *SystemVar:
						if coercibility > 2 {
							coercibility = 2
						}
					}
				})
				return collation, coercibility
			}
		}
		// Some general expressions returns a value of 5, so we just return 5 for all unmatched expressions.
		return collation, 5
	}
}

// ResolveCoercibility returns the collation to use by comparing coercibility, along with giving priority to binary
// collations. This is an approximation of MySQL's coercibility rules:
// https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
func ResolveCoercibility(leftCollation sql.CollationID, leftCoercibility int, rightCollation sql.CollationID, rightCoercibility int) (sql.CollationID, error) {
	if leftCoercibility < rightCoercibility {
		return leftCollation, nil
	} else if leftCoercibility > rightCoercibility {
		return rightCollation, nil
	} else if leftCollation == rightCollation {
		return leftCollation, nil
	} else if leftCollation == sql.Collation_Unspecified {
		return rightCollation, nil
	} else if rightCollation == sql.Collation_Unspecified {
		return leftCollation, nil
	} else { // Collations are not equal
		leftCharset := leftCollation.CharacterSet()
		rightCharset := rightCollation.CharacterSet()
		if leftCharset != rightCharset {
			if leftCharset.MaxLength() == 1 && rightCharset.MaxLength() > 1 { // Left non-Unicode, Right Unicode
				return rightCollation, nil
			} else if leftCharset.MaxLength() > 1 && rightCharset.MaxLength() == 1 { // Left Unicode, Right non-Unicode
				return leftCollation, nil
			} else {
				return sql.Collation_Unspecified, sql.ErrCollationIllegalMix.New(leftCollation.Name(), rightCollation.Name())
			}
		} else { // Character sets are equal
			// If the right collation is not _bin, then we default to the left collation (regardless of whether it is
			// or is not _bin).
			if strings.HasSuffix(rightCollation.Name(), "_bin") {
				return rightCollation, nil
			} else {
				return leftCollation, nil
			}
		}
	}
}

// TODO: remove when finished with collation coercibility
func inspectExpression(expr sql.Expression, exprFunc func(sql.Expression)) {
	for _, child := range expr.Children() {
		inspectExpression(child, exprFunc)
	}
	exprFunc(expr)
}
