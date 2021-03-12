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

package plan

import (
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// ProcedureSecurityContext determines whether the stored procedure is executed using the privileges of the definer or
// the invoker.
type ProcedureSecurityContext byte

const (
	// ProcedureSecurityContext_Definer uses the definer's security context.
	ProcedureSecurityContext_Definer ProcedureSecurityContext = iota
	// ProcedureSecurityContext_Invoker uses the invoker's security context.
	ProcedureSecurityContext_Invoker
)

// ProcedureParamDirection represents the use case of the stored procedure parameter.
type ProcedureParamDirection byte

const (
	// ProcedureParamDirection_In means the parameter passes its contained value to the stored procedure.
	ProcedureParamDirection_In ProcedureParamDirection = iota
	// ProcedureParamDirection_Inout means the parameter passes its contained value to the stored procedure, while also
	// modifying the given variable.
	ProcedureParamDirection_Inout
	// ProcedureParamDirection_Out means the parameter variable will be modified, but will not be read from within the
	// stored procedure.
	ProcedureParamDirection_Out
)

// ProcedureParam represents the parameter of a stored procedure.
type ProcedureParam struct {
	Direction ProcedureParamDirection // Direction is the direction of the parameter.
	Name      string                  // Name is the name of the parameter.
	Type      sql.Type                // Type is the SQL type of the parameter.
}

// Characteristic represents a characteristic that is defined on either a stored procedure or stored function.
type Characteristic byte

const (
	Characteristic_LanguageSql Characteristic = iota
	Characteristic_Deterministic
	Characteristic_NotDeterministic
	Characteristic_ContainsSql
	Characteristic_NoSql
	Characteristic_ReadsSqlData
	Characteristic_ModifiesSqlData
)

// Procedure is a stored procedure that may be executed using the CALL statement.
type Procedure struct {
	Name                  string
	Definer               string
	Params                []ProcedureParam
	SecurityContext       ProcedureSecurityContext
	Comment               string
	Characteristics       []Characteristic
	CreateProcedureString string
	Body                  sql.Node
	CreatedAt             time.Time
	ModifiedAt            time.Time
}

var _ sql.Node = (*Procedure)(nil)
var _ sql.DebugStringer = (*Procedure)(nil)

// NewProcedure returns a *Procedure. All names contained within are lowercase, and all methods are case-insensitive.
func NewProcedure(
	name string,
	definer string,
	params []ProcedureParam,
	securityContext ProcedureSecurityContext,
	comment string,
	characteristics []Characteristic,
	createProcedureString string,
	body sql.Node,
	createdAt time.Time,
	modifiedAt time.Time,
) *Procedure {
	lowercasedParams := make([]ProcedureParam, len(params))
	for i, param := range params {
		lowercasedParams[i] = ProcedureParam{
			Direction: param.Direction,
			Name:      strings.ToLower(param.Name),
			Type:      param.Type,
		}
	}
	return &Procedure{
		Name:                  strings.ToLower(name),
		Definer:               definer,
		Params:                lowercasedParams,
		SecurityContext:       securityContext,
		Comment:               comment,
		Characteristics:       characteristics,
		CreateProcedureString: createProcedureString,
		Body:                  body,
		CreatedAt:             createdAt,
		ModifiedAt:            modifiedAt,
	}
}

// Resolved implements the sql.Node interface.
func (p *Procedure) Resolved() bool {
	return p.Body.Resolved()
}

// String implements the sql.Node interface.
func (p *Procedure) String() string {
	return p.Body.String()
}

// DebugString implements the sql.DebugStringer interface.
func (p *Procedure) DebugString() string {
	return sql.DebugString(p.Body)
}

// Schema implements the sql.Node interface.
func (p *Procedure) Schema() sql.Schema {
	return p.Body.Schema()
}

// Children implements the sql.Node interface.
func (p *Procedure) Children() []sql.Node {
	return []sql.Node{p.Body}
}

// WithChildren implements the sql.Node interface.
func (p *Procedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	np := *p
	np.Body = children[0]
	return &np, nil
}

// RowIter implements the sql.Node interface.
func (p *Procedure) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return p.Body.RowIter(ctx, row)
}

// String returns the original SQL representation.
func (pst ProcedureSecurityContext) String() string {
	switch pst {
	case ProcedureSecurityContext_Definer:
		return "SQL SECURITY DEFINER"
	case ProcedureSecurityContext_Invoker:
		return "SQL SECURITY INVOKER"
	default:
		panic(fmt.Errorf("invalid security context value `%d`", byte(pst)))
	}
}

// String returns the original SQL representation.
func (pp ProcedureParam) String() string {
	direction := ""
	switch pp.Direction {
	case ProcedureParamDirection_In:
		direction = "IN"
	case ProcedureParamDirection_Inout:
		direction = "INOUT"
	case ProcedureParamDirection_Out:
		direction = "OUT"
	}
	return fmt.Sprintf("%s %s %s", direction, pp.Name, pp.Type.String())
}

// String returns the original SQL representation.
func (c Characteristic) String() string {
	switch c {
	case Characteristic_LanguageSql:
		return "LANGUAGE SQL"
	case Characteristic_Deterministic:
		return "DETERMINISTIC"
	case Characteristic_NotDeterministic:
		return "NOT DETERMINISTIC"
	case Characteristic_ContainsSql:
		return "CONTAINS SQL"
	case Characteristic_NoSql:
		return "NO SQL"
	case Characteristic_ReadsSqlData:
		return "READS SQL DATA"
	case Characteristic_ModifiesSqlData:
		return "MODIFIES SQL DATA"
	default:
		panic(fmt.Errorf("invalid characteristic value `%d`", byte(c)))
	}
}
