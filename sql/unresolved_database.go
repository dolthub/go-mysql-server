package sql

// UnresolvedDatabase is a database which has not been resolved yet.
type UnresolvedDatabase struct{}

// Name returns the database name, which is always "unresolved_database".
func (d *UnresolvedDatabase) Name() string {
	return "unresolved_database"
}

// Tables returns the tables in the database.
func (d *UnresolvedDatabase) Tables() map[string]Table {
	return make(map[string]Table)
}
