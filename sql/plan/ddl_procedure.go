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
)

type CreateProcedure struct {
	Name                  string
	Definer               string
	Params                []sql.ProcedureParam
	CreatedAt             time.Time
	ModifiedAt            time.Time
	SecurityContext       sql.ProcedureSecurityContext
	Characteristics       []sql.Characteristic
	Comment               string
	CreateProcedureString string
	Body                  sql.Node
	BodyString            string
	Db                    sql.Database
}

var _ sql.Node = (*CreateProcedure)(nil)
var _ sql.Databaser = (*CreateProcedure)(nil)
var _ sql.DebugStringer = (*CreateProcedure)(nil)

// NewCreateProcedure returns a *CreateProcedure node.
func NewCreateProcedure(
	name,
	definer string,
	params []sql.ProcedureParam,
	createdAt, modifiedAt time.Time,
	securityContext sql.ProcedureSecurityContext,
	characteristics []sql.Characteristic,
	body sql.Node,
	comment, createString, bodyString string,
) *CreateProcedure {
	return &CreateProcedure{
		Name:                  name,
		Definer:               definer,
		Params:                params,
		CreatedAt:             createdAt,
		ModifiedAt:            modifiedAt,
		SecurityContext:       securityContext,
		Characteristics:       characteristics,
		Comment:               comment,
		CreateProcedureString: createString,
		Body:                  body,
		BodyString:            bodyString,
	}
}

// Database implements the sql.Databaser interface.
func (c *CreateProcedure) Database() sql.Database {
	return c.Db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateProcedure) WithDatabase(database sql.Database) (sql.Node, error) {
	nc := *c
	nc.Db = database
	return &nc, nil
}

// Resolved implements the sql.Node interface.
func (c *CreateProcedure) Resolved() bool {
	return c.Body.Resolved()
}

// Schema implements the sql.Node interface.
func (c *CreateProcedure) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (c *CreateProcedure) Children() []sql.Node {
	return []sql.Node{c.Body}
}

// WithChildren implements the sql.Node interface.
func (c *CreateProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}

	nc := *c
	nc.Body = children[0]
	return &nc, nil
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
		definer, c.Name, params, c.SecurityContext.String(), comment, characteristics, c.Body.String())
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
		definer, c.Name, params, c.SecurityContext.String(), comment, characteristics, sql.DebugString(c.Body))
}

// AsProcedure returns this *CreateProcedure as a *sql.Procedure.
func (c *CreateProcedure) AsProcedure() *sql.Procedure {
	return sql.NewProcedure(
		c.Name,
		c.Definer,
		c.Params,
		c.SecurityContext,
		c.Characteristics,
		c.CreateProcedureString,
		c.Body)
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
		db:  c.Db,
		ctx: ctx,
	}, nil
}

// createProcedureIter is the row iterator for *CreateProcedure.
type createProcedureIter struct {
	once sync.Once
	spd  sql.StoredProcedureDetails
	db   sql.Database
	ctx  *sql.Context
}

// Next implements the sql.RowIter interface.
func (c *createProcedureIter) Next() (sql.Row, error) {
	run := false
	c.once.Do(func() {
		run = true
	})
	if !run {
		return nil, io.EOF
	}

	pdb, ok := c.db.(sql.StoredProcedureDatabase)
	if !ok {
		return nil, sql.ErrStoredProceduresNotSupported.New(c.db.Name())
	}

	err := pdb.SaveStoredProcedure(c.ctx, c.spd)
	if err != nil {
		return nil, err
	}

	return sql.Row{sql.NewOkResult(0)}, nil
}

// Close implements the sql.RowIter interface.
func (c *createProcedureIter) Close(ctx *sql.Context) error {
	return nil
}
