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
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

type ShowCreateTrigger struct {
	db          sql.Database
	TriggerName string
}

var _ sql.Databaser = (*ShowCreateTrigger)(nil)
var _ sql.Node = (*ShowCreateTrigger)(nil)

var showCreateTriggerSchema = sql.Schema{
	&sql.Column{Name: "Trigger", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "sql_mode", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "SQL Original Statement", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "character_set_client", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "collation_connection", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Database Collation", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Created", Type: sql.Datetime, Nullable: false},
}

// NewShowCreateTrigger creates a new ShowCreateTrigger node for SHOW CREATE TRIGGER statements.
func NewShowCreateTrigger(db sql.Database, trigger string) *ShowCreateTrigger {
	return &ShowCreateTrigger{
		db:          db,
		TriggerName: strings.ToLower(trigger),
	}
}

// String implements the sql.Node interface.
func (s *ShowCreateTrigger) String() string {
	return fmt.Sprintf("SHOW CREATE TRIGGER %s", s.TriggerName)
}

// Resolved implements the sql.Node interface.
func (s *ShowCreateTrigger) Resolved() bool {
	_, ok := s.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (s *ShowCreateTrigger) Children() []sql.Node {
	return nil
}

// Schema implements the sql.Node interface.
func (s *ShowCreateTrigger) Schema() sql.Schema {
	return showCreateTriggerSchema
}

// RowIter implements the sql.Node interface.
func (s *ShowCreateTrigger) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	triggerDb, ok := s.db.(sql.TriggerDatabase)
	if !ok {
		return nil, sql.ErrTriggersNotSupported.New(s.db.Name())
	}
	triggers, err := triggerDb.GetTriggers(ctx)
	if err != nil {
		return nil, err
	}
	for _, trigger := range triggers {
		if strings.ToLower(trigger.Name) == s.TriggerName {
			characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
			if err != nil {
				return nil, err
			}
			collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
			if err != nil {
				return nil, err
			}
			collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
			if err != nil {
				return nil, err
			}
			return sql.RowsToRowIter(sql.Row{
				trigger.Name,            // Trigger
				"",                      // sql_mode
				trigger.CreateStatement, // SQL Original Statement
				characterSetClient,      // character_set_client
				collationConnection,     // collation_connection
				collationServer,         // Database Collation
				time.Unix(0, 0).UTC(),   // Created
			}), nil
		}
	}
	return nil, sql.ErrTriggerDoesNotExist.New(s.TriggerName)
}

// WithChildren implements the sql.Node interface.
func (s *ShowCreateTrigger) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(s, children...)
}

// Database implements the sql.Databaser interface.
func (s *ShowCreateTrigger) Database() sql.Database {
	return s.db
}

// WithDatabase implements the sql.Databaser interface.
func (s *ShowCreateTrigger) WithDatabase(db sql.Database) (sql.Node, error) {
	ns := *s
	ns.db = db
	return &ns, nil
}
