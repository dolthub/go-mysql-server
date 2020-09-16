// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
	"strings"
)

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
				// TODO: this is gross, we need a better interface for system vars than this
				valtyp, ok := sql.DefaultSessionConfig()[varName]
				if !ok {
					return nil, sql.ErrUnknownSystemVariable.New(varName)
				}

				// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
				// unresolved columns.
				if uc, ok := setVal.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
					setVal = expression.NewLiteral(uc.Name(), sql.LongText)
				}

				return sf.WithChildren(expression.NewSystemVar(varName, valtyp.Typ), setVal)
			}
			if isUserVariable(uc) {
				return sf.WithChildren(expression.NewUserVar(uc.String()), setVal)
			}
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

	if !e.Resolved() || !sql.IsTextOnly(e.Type()) {
		return e, nil
	}

	txt, err := e.Eval(ctx, nil)
	if err != nil {
		return nil, err
	}

	val, ok := txt.(string)
	if !ok {
		return nil, parse.ErrUnsupportedFeature.New("invalid qualifiers in set variable names")
	}

	switch strings.ToLower(val) {
	case sqlparser.KeywordString(sqlparser.ON):
		return expression.NewLiteral(int64(1), sql.Int64), nil
	case sqlparser.KeywordString(sqlparser.TRUE):
		return expression.NewLiteral(true, sql.Boolean), nil
	case sqlparser.KeywordString(sqlparser.OFF):
		return expression.NewLiteral(int64(0), sql.Int64), nil
	case sqlparser.KeywordString(sqlparser.FALSE):
		return expression.NewLiteral(false, sql.Boolean), nil
	}

	return e, nil
}

func resolveSetColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
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
			valtyp, ok := sql.DefaultSessionConfig()[varName]
			if !ok {
				return nil, sql.ErrUnknownSystemVariable.New(varName)
			}

			// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
			// unresolved columns.
			if uc, ok := setVal.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
				setVal = expression.NewLiteral(uc.Name(), sql.LongText)
			}

			return sf.WithChildren(expression.NewSystemVar(varName, valtyp.Typ), setVal)
		}

		return sf, nil
	})
}
