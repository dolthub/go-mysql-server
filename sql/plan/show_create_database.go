package plan

import (
	"bytes"
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowCreateDatabase returns the SQL for creating a database.
type ShowCreateDatabase struct {
	Database    sql.Database
	IfNotExists bool
}

const defaultCharacterSet = "utf8mb4"

var showCreateDatabaseSchema = sql.Schema{
	{Name: "Database", Type: sql.Text},
	{Name: "Create Database", Type: sql.Text},
}

// NewShowCreateDatabase creates a new ShowCreateDatabase node.
func NewShowCreateDatabase(db sql.Database, ifNotExists bool) *ShowCreateDatabase {
	return &ShowCreateDatabase{db, ifNotExists}
}

// RowIter implements the sql.Node interface.
func (s *ShowCreateDatabase) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var name = s.Database.Name()

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
		defaultCharacterSet,
		defaultCollation,
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
	return fmt.Sprintf("SHOW CREATE DATABASE %s", s.Database.Name())
}

// Children implements the sql.Node interface.
func (s *ShowCreateDatabase) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowCreateDatabase) Resolved() bool {
	_, ok := s.Database.(sql.UnresolvedDatabase)
	return !ok
}

// TransformExpressionsUp implements the sql.Node interface.
func (s *ShowCreateDatabase) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return s, nil
}

// TransformUp implements the sql.Node interface.
func (s *ShowCreateDatabase) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(s)
}
