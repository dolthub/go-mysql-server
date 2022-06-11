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

package plan

import (
	"reflect"
	"strconv"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var (
	boolType      = reflect.TypeOf(bool(false))
	byteSliceType = reflect.TypeOf([]byte{})
	intType       = reflect.TypeOf(int(0))
	uintType      = reflect.TypeOf(uint(0))
	decimalType   = reflect.TypeOf(decimal.Decimal{})
)

// ExternalProcedure is the sql.Node container for sql.ExternalStoredProcedureDetails.
type ExternalProcedure struct {
	sql.ExternalStoredProcedureDetails
	ParamDefinitions []ProcedureParam
	Params           []*expression.ProcedureParam
}

var _ sql.Node = (*ExternalProcedure)(nil)
var _ sql.Expressioner = (*ExternalProcedure)(nil)

// Resolved implements the interface sql.Node.
func (n *ExternalProcedure) Resolved() bool {
	return true
}

// String implements the interface sql.Node.
func (n *ExternalProcedure) String() string {
	return n.ExternalStoredProcedureDetails.Name
}

// Schema implements the interface sql.Node.
func (n *ExternalProcedure) Schema() sql.Schema {
	return n.ExternalStoredProcedureDetails.Schema
}

// Children implements the interface sql.Node.
func (n *ExternalProcedure) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *ExternalProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// Expressions implements the interface sql.Expressioner.
func (n *ExternalProcedure) Expressions() []sql.Expression {
	exprs := make([]sql.Expression, len(n.Params))
	for i, param := range n.Params {
		exprs[i] = param
	}
	return exprs
}

// WithExpressions implements the interface sql.Expressioner.
func (n *ExternalProcedure) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != len(n.Params) {
		return nil, sql.ErrInvalidExpressionNumber.New(n, len(expressions), len(n.Params))
	}
	newParams := make([]*expression.ProcedureParam, len(expressions))
	for i, expr := range expressions {
		newParams[i] = expr.(*expression.ProcedureParam)
	}
	nn := *n
	nn.Params = newParams
	return &nn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *ExternalProcedure) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: when DEFINER is implemented for stored procedures then this should be added
	return true
}

// RowIter implements the interface sql.Node.
func (n *ExternalProcedure) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// The function's structure has been verified by the analyzer, so no need to double-check any of it here
	funcVal := reflect.ValueOf(n.Function)
	funcType := funcVal.Type()
	// The first parameter is always the context, but it doesn't exist as far as the stored procedures are concerned, so
	// we prepend it here
	funcParams := make([]reflect.Value, len(n.Params)+1)
	funcParams[0] = reflect.ValueOf(ctx)

	for i := range n.Params {
		paramDefinition := n.ParamDefinitions[i]
		var funcParamType reflect.Type
		if paramDefinition.Variadic {
			funcParamType = funcType.In(funcType.NumIn() - 1).Elem()
		} else {
			funcParamType = funcType.In(i + 1)
		}
		// Grab the passed-in variable and convert it to the type we expect
		exprParamVal, err := n.Params[i].Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		exprParamVal, err = paramDefinition.Type.Convert(exprParamVal)
		if err != nil {
			return nil, err
		}

		funcParams[i+1], err = n.processParam(ctx, funcParamType, exprParamVal)
		if err != nil {
			return nil, err
		}
	}
	out := funcVal.Call(funcParams)

	// Again, these types are enforced in the analyzer, so it's safe to assume their types here
	if err, ok := out[1].Interface().(error); ok { // Only evaluates to true when error is not nil
		return nil, err
	}
	for i, paramDefinition := range n.ParamDefinitions {
		if paramDefinition.Direction == ProcedureParamDirection_Inout || paramDefinition.Direction == ProcedureParamDirection_Out {
			exprParam := n.Params[i]
			funcParamVal := funcParams[i+1].Elem().Interface()
			err := exprParam.Set(funcParamVal, exprParam.Type())
			if err != nil {
				return nil, err
			}
		}
	}
	// It's not invalid to return a nil RowIter, as having no rows to return is expected of many stored procedures.
	if rowIter, ok := out[0].Interface().(sql.RowIter); ok {
		return rowIter, nil
	}
	return sql.RowsToRowIter(), nil
}

func (n *ExternalProcedure) processParam(ctx *sql.Context, funcParamType reflect.Type, exprParamVal interface{}) (reflect.Value, error) {
	funcParamCompType := funcParamType
	if funcParamType.Kind() == reflect.Ptr {
		funcParamCompType = funcParamType.Elem()
	}
	// Convert to bool, []byte, int, and uint as they differ from their sql.Type value
	switch funcParamCompType {
	case boolType:
		val := false
		if exprParamVal.(int8) != 0 {
			val = true
		}
		exprParamVal = val
	case byteSliceType:
		exprParamVal = []byte(exprParamVal.(string))
	case intType:
		if strconv.IntSize == 32 {
			exprParamVal = int(exprParamVal.(int32))
		} else {
			exprParamVal = int(exprParamVal.(int64))
		}
	case uintType:
		if strconv.IntSize == 64 {
			exprParamVal = int(exprParamVal.(uint32))
		} else {
			exprParamVal = int(exprParamVal.(uint64))
		}
	case decimalType:
		exprParamVal = exprParamVal.(decimal.Decimal)
	}

	if funcParamType.Kind() == reflect.Ptr { // Coincides with INOUT
		funcParamVal := reflect.New(funcParamType.Elem())
		funcParamVal.Elem().Set(reflect.ValueOf(exprParamVal))
		return funcParamVal, nil
	} else { // Coincides with IN
		funcParamVal := reflect.New(funcParamType)
		funcParamVal.Elem().Set(reflect.ValueOf(exprParamVal))
		return funcParamVal.Elem(), nil
	}
}
