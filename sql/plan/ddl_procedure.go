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
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	return c.db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateProcedure) WithDatabase(database sql.Database) (sql.Node, error) {
	cp := *c
	cp.db = database
	return &cp, nil
}

// Resolved implements the sql.Node interface.
func (c *CreateProcedure) Resolved() bool {
	return c.ddlNode.Resolved() && c.Procedure.Resolved()
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
		sql.NewPrivilegedOperation(c.db.Name(), "", "", sql.PrivilegeType_CreateRoutine))
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

// RowIter implements the sql.Node interface.
func (c *CreateProcedure) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &createProcedureIter{
		spd: sql.StoredProcedureDetails{
			Name:            c.Name,
			CreateStatement: c.CreateProcedureString,
			CreatedAt:       c.CreatedAt,
			ModifiedAt:      c.ModifiedAt,
		},
		db: c.db,
	}, nil
}

// createProcedureIter is the row iterator for *CreateProcedure.
type createProcedureIter struct {
	once sync.Once
	spd  sql.StoredProcedureDetails
	db   sql.Database
}

// Next implements the sql.RowIter interface.
func (c *createProcedureIter) Next(ctx *sql.Context) (sql.Row, error) {
	run := false
	c.once.Do(func() {
		run = true
	})
	if !run {
		return nil, io.EOF
	}
	//TODO: if "automatic_sp_privileges" is true then the creator automatically gets EXECUTE and ALTER ROUTINE on this procedure
	pdb, ok := c.db.(sql.StoredProcedureDatabase)
	if !ok {
		return nil, sql.ErrStoredProceduresNotSupported.New(c.db.Name())
	}

	err := pdb.SaveStoredProcedure(ctx, c.spd)
	if err != nil {
		return nil, err
	}

	return sql.Row{types.NewOkResult(0)}, nil
}

// Close implements the sql.RowIter interface.
func (c *createProcedureIter) Close(ctx *sql.Context) error {
	return nil
}
