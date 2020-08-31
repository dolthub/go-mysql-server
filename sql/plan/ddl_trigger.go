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

package plan

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

type TriggerOrder struct {
	PrecedesOrFollows string // PrecedesStr, FollowsStr
	OtherTriggerName  string
}

type CreateTrigger struct {
	triggerName  string
	triggerTime  string
	triggerEvent string
	triggerOrder *TriggerOrder
	table        sql.Node
	body         sql.Node
	bodyString   string
}

func NewCreateTrigger(triggerName, triggerTime, triggerEvent string, triggerOrder *TriggerOrder, table sql.Node, body sql.Node, bodyString string) *CreateTrigger {
	return &CreateTrigger{
		triggerName:  triggerName,
		triggerTime:  triggerTime,
		triggerEvent: triggerEvent,
		triggerOrder: triggerOrder,
		table:        table,
		body:         body,
		bodyString:   bodyString,
	}
}

func (c *CreateTrigger) Resolved() bool {
	return c.table.Resolved() && c.body.Resolved()
}

func (c *CreateTrigger) Schema() sql.Schema {
	return nil
}

func (c *CreateTrigger) Children() []sql.Node {
	return []sql.Node{c.table, c.body}
}

func (c *CreateTrigger) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 2)
	}

	nc := *c
	nc.table = children[0]
	nc.body = children[1]
	return &nc, nil
}

func (c *CreateTrigger) String() string {
	order := ""
	if c.triggerOrder != nil {
		order = fmt.Sprintf("%s %s ", c.triggerOrder.PrecedesOrFollows, c.triggerOrder.OtherTriggerName)
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s%s", c.triggerName, c.triggerTime, c.triggerEvent, c.table, order, c.bodyString)
}

func (c *CreateTrigger) DebugString() string {
	order := ""
	if c.triggerOrder != nil {
		order = fmt.Sprintf("%s %s ", c.triggerOrder.PrecedesOrFollows, c.triggerOrder.OtherTriggerName)
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s%s", c.triggerName, c.triggerTime, c.triggerEvent, sql.DebugString(c.table), order, sql.DebugString(c.body))
}

func (c *CreateTrigger) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// TODO: implement
	return nil, nil
}
