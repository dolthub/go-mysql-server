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

package analyzer

import (
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveSetVariables replaces SET @@var and SET @var expressions with appropriately resolved expressions for the
// left-hand side, and evaluate the right-hand side where possible, including filling in defaults. Also validates that
// system variables are known to the system.
func resolveSetVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.Set)
	if !ok || n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sf, ok := e.(*expression.SetField)
		if !ok {
			return e, nil
		}

		varName := trimVarName(sf.Left.String())
		setVal, err := getSetVal(ctx, varName, sf.Right)
		if err != nil {
			return nil, err
		}

		// For the left side of the SetField expression, we will attempt to resolve the variable being set. The rules to
		// determine whether to treat a left-hand expression as a system var or something else are subtle. These are all
		// the valid ways to assign to a system variable with session scope:
		// SET SESSION sql_mode = 'TRADITIONAL';
		// SET LOCAL sql_mode = 'TRADITIONAL';
		// SET @@SESSION.sql_mode = 'TRADITIONAL';
		// SET @@LOCAL.sql_mode = 'TRADITIONAL';
		// SET @@sql_mode = 'TRADITIONAL';
		// SET sql_mode = 'TRADITIONAL';
		// These are all equivalent, and all distinct from setting a user variable with the same name:
		// set @sql_mode = "abc"
		if uc, ok := sf.Left.(*expression.UnresolvedColumn); ok {
			if isSystemVariable(uc) {
				// TODO: clean up distinction between system and user vars in this interface
				typ, _ := ctx.Session.Get(varName)
				if typ == sql.Null {
					// TODO: since we don't support all system variables supported by MySQL yet, for compatibility reasons we
					//  will just accept them all here. But we should reject unknown ones.
					// return nil, sql.ErrUnknownSystemVariable.New(varName)
					typ = sf.Right.Type()
				}

				// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
				// unresolved columns.
				if uc, ok := setVal.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
					if !isSystemVariable(uc) && !isUserVariable(uc) {
						setVal = expression.NewLiteral(uc.Name(), sql.LongText)
					}
				}

				return sf.WithChildren(expression.NewSystemVar(varName, typ), setVal)
			}

			if isUserVariable(uc) {
				return sf.WithChildren(expression.NewUserVar(varName), setVal)
			}
		}

		return sf, nil
	})
}

// resolveUnquotedSetVariables does a similar pass as resolveSetVariables, but handles system vars that were provided
// as barewords (vars not prefixed with @@, and string values unquoted). These will have been deferred into
// deferredColumns by the resolve_columns rule.
func resolveBarewordSetVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.Set)
	if !ok || n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sf, ok := e.(*expression.SetField)
		if !ok {
			return e, nil
		}

		varName := trimVarName(sf.Left.String())
		setVal, err := getSetVal(ctx, varName, sf.Right)
		if err != nil {
			return nil, err
		}

		// If this column expression was deferred, it means that it wasn't prefixed with @@ and can't be found in any table.
		// So treat it as a naked system variable and see if it exists
		if uc, ok := sf.Left.(*deferredColumn); ok {
			varName := trimVarName(uc.String())
			typ, _ := ctx.Session.Get(varName)
			if typ == sql.Null {
				// TODO: since we don't support all system variables supported by MySQL yet, for compatibility reasons we
				//  will just accept them all here. But we should reject unknown ones.
				// return nil, sql.ErrUnknownSystemVariable.New(varName)

				// If the right-hand side isn't resolved, we can't process this as a bareword variable assignment
				// because we don't know the type
				if !setVal.Resolved() {
					return sf, nil
				}

				typ = sf.Right.Type()
			}

			// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
			// unresolved columns.
			if uc, ok := setVal.(column); ok && uc.Table() == "" {
				if !isSystemVariable(uc) && !isUserVariable(uc) {
					setVal = expression.NewLiteral(uc.Name(), sql.LongText)
				}
			}

			return sf.WithChildren(expression.NewSystemVar(varName, typ), setVal)
		}

		return sf, nil
	})
}

// getSetVal evaluates the right hand side of a SetField expression and returns an evaluated value as appropriate
func getSetVal(ctx *sql.Context, varName string, e sql.Expression) (sql.Expression, error) {
	if _, ok := e.(*expression.DefaultColumn); ok {
		valtyp, ok := sql.DefaultSessionConfig()[varName]
		if !ok {
			return nil, sql.ErrUnknownSystemVariable.New(varName)
		}
		value, typ := valtyp.Value, valtyp.Typ
		return expression.NewLiteral(value, typ), nil
	}

	if !e.Resolved() {
		return e, nil
	}

	if sql.IsTextOnly(e.Type()) {
		txt, err := e.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		val, ok := txt.(string)
		if !ok {
			// TODO: better error message
			return nil, parse.ErrUnsupportedFeature.New("invalid set variable")
		}

		switch strings.ToLower(val) {
		case sqlparser.KeywordString(sqlparser.ON):
			return expression.NewLiteral(1, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.TRUE):
			return expression.NewLiteral(1, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.OFF):
			return expression.NewLiteral(0, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.FALSE):
			return expression.NewLiteral(0, sql.Boolean), nil
		}
	} else if e.Type() == sql.Boolean {
		val, err := e.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		b, ok := val.(bool)
		if !ok {
			return e, nil
		}

		if b {
			return expression.NewLiteral(1, sql.Boolean), nil
		} else {
			return expression.NewLiteral(0, sql.Boolean), nil
		}
	}

	return e, nil
}
