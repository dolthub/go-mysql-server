package plan

import "github.com/dolthub/go-mysql-server/sql"

// DummyResolvedDB is a transient database useful only for instances where a database is not available but required.
// No tables are persisted, nor will be returned.
type DummyResolvedDB struct {
	name string
}

var _ sql.Database = (*DummyResolvedDB)(nil)
var _ sql.TableCreator = (*DummyResolvedDB)(nil)
var _ sql.TableDropper = (*DummyResolvedDB)(nil)
var _ sql.TableRenamer = (*DummyResolvedDB)(nil)

// NewDummyResolvedDB creates a new dummy database with the given name.
func NewDummyResolvedDB(name string) *DummyResolvedDB {
	return &DummyResolvedDB{
		name: name,
	}
}

func (d *DummyResolvedDB) Name() string { return d.name }

func (d *DummyResolvedDB) Tables() map[string]sql.Table { return nil }

func (d *DummyResolvedDB) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	return nil, false, nil
}

func (d *DummyResolvedDB) GetTableNames(ctx *sql.Context) ([]string, error) { return nil, nil }

func (d *DummyResolvedDB) AddTable(name string, t sql.Table) {}

func (d *DummyResolvedDB) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	return nil
}

func (d *DummyResolvedDB) DropTable(ctx *sql.Context, name string) error { return nil }

func (d *DummyResolvedDB) RenameTable(ctx *sql.Context, oldName, newName string) error { return nil }
