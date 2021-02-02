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
	"bytes"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// ShowCreateDatabase returns the SQL for creating a database.
type ShowCreateDatabase struct {
	db          sql.Database
	IfNotExists bool
}

var showCreateDatabaseSchema = sql.Schema{
	{Name: "Database", Type: sql.LongText},
	{Name: "Create Database", Type: sql.LongText},
}

// NewShowCreateDatabase creates a new ShowCreateDatabase node.
func NewShowCreateDatabase(db sql.Database, ifNotExists bool) *ShowCreateDatabase {
	return &ShowCreateDatabase{db, ifNotExists}
}

var _ sql.Databaser = (*ShowCreateDatabase)(nil)

// Database implements the sql.Databaser interface.
func (s *ShowCreateDatabase) Database() sql.Database {
	return s.db
}

// WithDatabase implements the sql.Databaser interface.
func (s *ShowCreateDatabase) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *s
	nc.db = db
	return &nc, nil
}

// RowIter implements the sql.Node interface.
func (s *ShowCreateDatabase) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var name = s.db.Name()

	var buf bytes.Buffer

	buf.WriteString("CREATE DATABASE ")
	if s.IfNotExists {
		buf.WriteString("/*!32312 IF NOT EXISTS*/ ")
	}

	buf.WriteRune('`')
	buf.WriteString(name)
	buf.WriteRune('`')
	buf.WriteString(fmt.Sprintf(
		" /*!40100 DEFAULT CHARACTER SET %s COLLATE %s */",
		sql.Collation_Default.CharacterSet().String(),
		sql.Collation_Default.String(),
	))

	return sql.RowsToRowIter(
		sql.NewRow(name, buf.String()),
	), nil
}

// Schema implements the sql.Node interface.
func (s *ShowCreateDatabase) Schema() sql.Schema {
	return showCreateDatabaseSchema
}

func (s *ShowCreateDatabase) String() string {
	return fmt.Sprintf("SHOW CREATE DATABASE %s", s.db.Name())
}

// Children implements the sql.Node interface.
func (s *ShowCreateDatabase) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowCreateDatabase) Resolved() bool {
	_, ok := s.db.(sql.UnresolvedDatabase)
	return !ok
}

// WithChildren implements the Node interface.
func (s *ShowCreateDatabase) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}
