package plan

import (
	"bytes"
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowCreateDatabase returns the SQL for creating a database.
type ShowCreateDatabase struct {
	db          sql.Database
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
func (s *ShowCreateDatabase) RowIter(ctx *sql.Context) (sql.RowIter, error) {
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
	return fmt.Sprintf("SHOW CREATE DATABASE %s", s.db.Name())
}

// Children implements the sql.Node interface.
func (s *ShowCreateDatabase) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowCreateDatabase) Resolved() bool {
	_, ok := s.db.(sql.UnresolvedDatabase)
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
