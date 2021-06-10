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

package expression

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// ProcedureParamReference contains the references to the parameters for a single CALL statement.
type ProcedureParamReference struct {
	nameToParam map[string]*procedureParamReferenceValue
}
type procedureParamReferenceValue struct {
	Name       string
	Value      interface{}
	SqlType    sql.Type
	HasBeenSet bool
}

// Initialize sets the initial value for the parameter.
func (ppr *ProcedureParamReference) Initialize(name string, sqlType sql.Type, val interface{}) error {
	name = strings.ToLower(name)
	convertedVal, err := sqlType.Convert(val)
	if err != nil {
		return err
	}
	ppr.nameToParam[name] = &procedureParamReferenceValue{
		Name:       name,
		Value:      convertedVal,
		SqlType:    sqlType,
		HasBeenSet: false,
	}
	return nil
}

// Get returns the value of the given parameter. Name is case-insensitive.
func (ppr *ProcedureParamReference) Get(name string) (interface{}, error) {
	name = strings.ToLower(name)
	paramRefVal, ok := ppr.nameToParam[name]
	if !ok {
		return nil, fmt.Errorf("cannot find value for parameter `%s`", name)
	}
	return paramRefVal.Value, nil
}

// GetType returns the type of the given parameter. Name is case-insensitive. Returns the NULL type if the type cannot
// be found.
func (ppr *ProcedureParamReference) GetType(name string) sql.Type {
	if ppr == nil {
		return sql.Null
	}
	name = strings.ToLower(name)
	paramRefVal, ok := ppr.nameToParam[name]
	if !ok {
		return sql.Null
	}
	return paramRefVal.SqlType
}

// Set updates the value of the given parameter. Name is case-insensitive.
func (ppr *ProcedureParamReference) Set(name string, val interface{}, valType sql.Type) error {
	name = strings.ToLower(name)
	paramRefVal, ok := ppr.nameToParam[name]
	if !ok {
		return fmt.Errorf("cannot find value for parameter `%s`", name)
	}
	//TODO: do some actual type checking using the given value's type
	val, err := paramRefVal.SqlType.Convert(val)
	if err != nil {
		return err
	}
	paramRefVal.Value = val
	paramRefVal.HasBeenSet = true
	return nil
}

// HasBeenSet returns whether the parameter has had its value altered from the initial value.
func (ppr *ProcedureParamReference) HasBeenSet(name string) bool {
	name = strings.ToLower(name)
	paramRefVal, ok := ppr.nameToParam[name]
	if !ok {
		return false
	}
	return paramRefVal.HasBeenSet
}

func NewProcedureParamReference() *ProcedureParamReference {
	return &ProcedureParamReference{make(map[string]*procedureParamReferenceValue)}
}

// ProcedureParam represents the parameter of a stored procedure or stored function.
type ProcedureParam struct {
	name       string
	pRef       *ProcedureParamReference
	hasBeenSet bool
}

// NewProcedureParam creates a new ProcedureParam expression.
func NewProcedureParam(name string) *ProcedureParam {
	return &ProcedureParam{name: strings.ToLower(name)}
}

// Children implements the sql.Expression interface.
func (*ProcedureParam) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
func (*ProcedureParam) Resolved() bool {
	return true
}

// IsNullable implements the sql.Expression interface.
func (*ProcedureParam) IsNullable() bool {
	return false
}

// Type implements the sql.Expression interface.
func (pp *ProcedureParam) Type() sql.Type {
	return pp.pRef.GetType(pp.name)
}

// Name implements the Nameable interface.
func (pp *ProcedureParam) Name() string {
	return pp.name
}

// String implements the sql.Expression interface.
func (pp *ProcedureParam) String() string {
	return pp.name
}

// Eval implements the sql.Expression interface.
func (pp *ProcedureParam) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	return pp.pRef.Get(pp.name)
}

// WithChildren implements the sql.Expression interface.
func (pp *ProcedureParam) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(pp, len(children), 0)
	}
	return pp, nil
}

// WithParamReference returns a new *ProcedureParam containing the given *ProcedureParamReference.
func (pp *ProcedureParam) WithParamReference(pRef *ProcedureParamReference) *ProcedureParam {
	npp := *pp
	npp.pRef = pRef
	return &npp
}

// Set sets the value of this procedure parameter to the given value.
func (pp *ProcedureParam) Set(val interface{}, valType sql.Type) error {
	return pp.pRef.Set(pp.name, val, valType)
}
