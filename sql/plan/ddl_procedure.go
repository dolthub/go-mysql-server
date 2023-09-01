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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

type CreateProcedure struct {
	*Procedure
	ddlNode
	BodyString string
}

var _ sql.Node = (*CreateProcedure)(nil)
var _ sql.Databaser = (*CreateProcedure)(nil)
var _ sql.DebugStringer = (*CreateProcedure)(nil)
var _ sql.CollationCoercible = (*CreateProcedure)(nil)

// NewCreateProcedure returns a *CreateProcedure node.
func NewCreateProcedure(
	db sql.Database,
	name,
	definer string,
	params []ProcedureParam,
	createdAt, modifiedAt time.Time,
	securityContext ProcedureSecurityContext,
	characteristics []Characteristic,
	body sql.Node,
	comment, createString, bodyString string,
) *CreateProcedure {
	procedure := NewProcedure(
		name,
		definer,
		params,
		securityContext,
		comment,
		characteristics,
		createString,
		body,
		createdAt,
		modifiedAt)
	return &CreateProcedure{
		Procedure:  procedure,
		BodyString: bodyString,
		ddlNode:    ddlNode{db},
	}
}

// Database implements the sql.Databaser interface.
func (c *CreateProcedure) Database() sql.Database {
	return c.Db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateProcedure) WithDatabase(database sql.Database) (sql.Node, error) {
	cp := *c
	cp.Db = database
	return &cp, nil
}

// Resolved implements the sql.Node interface.
func (c *CreateProcedure) Resolved() bool {
	return c.ddlNode.Resolved() && c.Procedure.Resolved()
}

func (c *CreateProcedure) IsReadOnly() bool {
	return false
}

// Schema implements the sql.Node interface.
func (c *CreateProcedure) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (c *CreateProcedure) Children() []sql.Node {
	return []sql.Node{c.Procedure}
}

// WithChildren implements the sql.Node interface.
func (c *CreateProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	procedure, ok := children[0].(*Procedure)
	if !ok {
		return nil, fmt.Errorf("expected `*Procedure` but got `%T`", children[0])
	}

	nc := *c
	nc.Procedure = procedure
	return &nc, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateProcedure) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(c.Db.Name(), "", "", sql.PrivilegeType_CreateRoutine))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CreateProcedure) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// String implements the sql.Node interface.
func (c *CreateProcedure) String() string {
	definer := ""
	if c.Definer != "" {
		definer = fmt.Sprintf(" DEFINER = %s", c.Definer)
	}
	params := ""
	for i, param := range c.Params {
		if i > 0 {
			params += ", "
		}
		params += param.String()
	}
	comment := ""
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", c.Comment)
	}
	characteristics := ""
	for _, characteristic := range c.Characteristics {
		characteristics += fmt.Sprintf(" %s", characteristic.String())
	}
	return fmt.Sprintf("CREATE%s PROCEDURE %s (%s) %s%s%s %s",
		definer, c.Name, params, c.SecurityContext.String(), comment, characteristics, c.Procedure.String())
}

// DebugString implements the sql.DebugStringer interface.
func (c *CreateProcedure) DebugString() string {
	definer := ""
	if c.Definer != "" {
		definer = fmt.Sprintf(" DEFINER = %s", c.Definer)
	}
	params := ""
	for i, param := range c.Params {
		if i > 0 {
			params += ", "
		}
		params += param.String()
	}
	comment := ""
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", c.Comment)
	}
	characteristics := ""
	for _, characteristic := range c.Characteristics {
		characteristics += fmt.Sprintf(" %s", characteristic.String())
	}
	return fmt.Sprintf("CREATE%s PROCEDURE %s (%s) %s%s%s %s",
		definer, c.Name, params, c.SecurityContext.String(), comment, characteristics, sql.DebugString(c.Procedure))
}
