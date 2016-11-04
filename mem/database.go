package mem

import "github.com/gitql/gitql/sql"

type Database struct {
	name   string
	tables map[string]sql.PhysicalRelation
}

func NewDatabase(name string) *Database {
	return &Database{
		name: name,
		tables: map[string]sql.PhysicalRelation{},
	}
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Relations() map[string]sql.PhysicalRelation {
	return d.tables
}

func (d *Database) AddTable(name string, t *Table) {
	d.tables[name] = t
}
