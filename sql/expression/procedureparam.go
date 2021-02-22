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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

//TODO: doc
type ProcedureParamReference struct {
	NameToParam map[string]interface{} // Names are always lowercase for simplicity
}

// ProcedureParam represents the parameter of a stored procedure or stored function.
type ProcedureParam struct {
	name string
	pRef *ProcedureParamReference
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
func (*ProcedureParam) Type() sql.Type {
	return sql.Null
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
	return pp.pRef.NameToParam[pp.name], nil
}

// WithChildren implements the sql.Expression interface.
func (pp *ProcedureParam) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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
