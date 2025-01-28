// Copyright 2021-2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

type CreateProcedure struct {
	ddlNode ddlNode

	StoredProcDetails sql.StoredProcedureDetails
	BodyString        string
}

var _ sql.Node = (*CreateProcedure)(nil)
var _ sql.Databaser = (*CreateProcedure)(nil)
var _ sql.DebugStringer = (*CreateProcedure)(nil)
var _ sql.CollationCoercible = (*CreateProcedure)(nil)

// NewCreateProcedure returns a *CreateProcedure node.
func NewCreateProcedure(
	db sql.Database,
	storedProcDetails sql.StoredProcedureDetails,
	bodyString string,
) *CreateProcedure {
	return &CreateProcedure{
		ddlNode:           ddlNode{db},
		StoredProcDetails: storedProcDetails,
		BodyString:        bodyString,
	}
}

// Database implements the sql.Databaser interface.
func (c *CreateProcedure) Database() sql.Database {
	return c.ddlNode.Db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateProcedure) WithDatabase(database sql.Database) (sql.Node, error) {
	cp := *c
	cp.ddlNode.Db = database
	return &cp, nil
}

// Resolved implements the sql.Node interface.
func (c *CreateProcedure) Resolved() bool {
	return c.ddlNode.Resolved()
}

func (c *CreateProcedure) IsReadOnly() bool {
	return false
}

// Schema implements the sql.Node interface.
func (c *CreateProcedure) Schema() sql.Schema {
	return types.OkResultSchema
}

// Children implements the sql.Node interface.
func (c *CreateProcedure) Children() []sql.Node {
	return []sql.Node{}
}

// WithChildren implements the sql.Node interface.
func (c *CreateProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CreateProcedure) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// String implements the sql.Node interface.
func (c *CreateProcedure) String() string {
	// move this logic elsewhere
	return "TODO"
	//definer := ""
	//if c.Procedure.Definer != "" {
	//	definer = fmt.Sprintf(" DEFINER = %s", c.Procedure.Definer)
	//}
	//params := ""
	//for i, param := range c.Procedure.Params {
	//	if i > 0 {
	//		params += ", "
	//	}
	//	params += param.String()
	//}
	//comment := ""
	//if c.Procedure.Comment != "" {
	//	comment = fmt.Sprintf(" COMMENT '%s'", c.Procedure.Comment)
	//}
	//characteristics := ""
	//for _, characteristic := range c.Procedure.Characteristics {
	//	characteristics += fmt.Sprintf(" %s", characteristic.String())
	//}
	//return fmt.Sprintf("CREATE%s PROCEDURE %s (%s) %s%s%s %s",
	//	definer, c.Procedure.Name, params, c.Procedure.SecurityContext.String(), comment, characteristics, c.Procedure.String())
}

// DebugString implements the sql.DebugStringer interface.
func (c *CreateProcedure) DebugString() string {
	// move this logic elsewhere
	return "TODO"
	//definer := ""
	//if c.Procedure.Definer != "" {
	//	definer = fmt.Sprintf(" DEFINER = %s", c.Procedure.Definer)
	//}
	//params := ""
	//for i, param := range c.Procedure.Params {
	//	if i > 0 {
	//		params += ", "
	//	}
	//	params += param.String()
	//}
	//comment := ""
	//if c.Procedure.Comment != "" {
	//	comment = fmt.Sprintf(" COMMENT '%s'", c.Procedure.Comment)
	//}
	//characteristics := ""
	//for _, characteristic := range c.Procedure.Characteristics {
	//	characteristics += fmt.Sprintf(" %s", characteristic.String())
	//}
	//return fmt.Sprintf("CREATE%s PROCEDURE %s (%s) %s%s%s %s",
	//	definer, c.Procedure.Name, params, c.Procedure.SecurityContext.String(), comment, characteristics, sql.DebugString(c.Procedure))
}
