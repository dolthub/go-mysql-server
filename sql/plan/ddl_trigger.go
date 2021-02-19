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

package plan

import (
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type TriggerOrder struct {
	PrecedesOrFollows string // PrecedesStr, FollowsStr
	OtherTriggerName  string
}

type CreateTrigger struct {
	TriggerName         string
	TriggerTime         string
	TriggerEvent        string
	TriggerOrder        *TriggerOrder
	Table               sql.Node
	Body                sql.Node
	CreateTriggerString string
	BodyString          string
	CreateDatabase      sql.Database
}

func NewCreateTrigger(triggerName, triggerTime, triggerEvent string, triggerOrder *TriggerOrder, table sql.Node, body sql.Node, createTriggerString, bodyString string) *CreateTrigger {
	return &CreateTrigger{
		TriggerName:         triggerName,
		TriggerTime:         triggerTime,
		TriggerEvent:        triggerEvent,
		TriggerOrder:        triggerOrder,
		Table:               table,
		Body:                body,
		BodyString:          bodyString,
		CreateTriggerString: createTriggerString,
	}
}

func (c *CreateTrigger) Database() sql.Database {
	return c.CreateDatabase
}

func (c *CreateTrigger) WithDatabase(database sql.Database) (sql.Node, error) {
	nc := *c
	nc.CreateDatabase = database
	return &nc, nil
}

func (c *CreateTrigger) Resolved() bool {
	return c.Table.Resolved() && c.Body.Resolved()
}

func (c *CreateTrigger) Schema() sql.Schema {
	return nil
}

func (c *CreateTrigger) Children() []sql.Node {
	return []sql.Node{c.Table, c.Body}
}

func (c *CreateTrigger) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 2)
	}

	nc := *c
	nc.Table = children[0]
	nc.Body = children[1]
	return &nc, nil
}

func (c *CreateTrigger) String() string {
	order := ""
	if c.TriggerOrder != nil {
		order = fmt.Sprintf("%s %s ", c.TriggerOrder.PrecedesOrFollows, c.TriggerOrder.OtherTriggerName)
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s%s", c.TriggerName, c.TriggerTime, c.TriggerEvent, c.Table, order, c.Body)
}

func (c *CreateTrigger) DebugString() string {
	order := ""
	if c.TriggerOrder != nil {
		order = fmt.Sprintf("%s %s ", c.TriggerOrder.PrecedesOrFollows, c.TriggerOrder.OtherTriggerName)
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s%s", c.TriggerName, c.TriggerTime, c.TriggerEvent, sql.DebugString(c.Table), order, sql.DebugString(c.Body))
}

type createTriggerIter struct {
	once       sync.Once
	definition sql.TriggerDefinition
	db         sql.Database
	ctx        *sql.Context
}

func (c *createTriggerIter) Next() (sql.Row, error) {
	run := false
	c.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	tdb, ok := c.db.(sql.TriggerDatabase)
	if !ok {
		return nil, sql.ErrTriggersNotSupported.New(c.db.Name())
	}

	err := tdb.CreateTrigger(c.ctx, c.definition)
	if err != nil {
		return nil, err
	}

	return sql.Row{sql.NewOkResult(0)}, nil
}

func (c *createTriggerIter) Close(*sql.Context) error {
	return nil
}

func (c *CreateTrigger) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &createTriggerIter{
		definition: sql.TriggerDefinition{
			Name:            c.TriggerName,
			CreateStatement: c.CreateTriggerString,
		},
		db:  c.CreateDatabase,
		ctx: ctx,
	}, nil
}
