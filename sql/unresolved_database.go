package sql

// UnresolvedDatabase is a database which has not been resolved yet.
type UnresolvedDatabase string

// Name returns the database name.
func (d UnresolvedDatabase) Name() string {
	return string(d)
}

// Tables returns the tables in the database.
func (UnresolvedDatabase) Tables() map[string]Table {
	return make(map[string]Table)
}
