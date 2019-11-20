package sql

import "context"

var _ Database = UnresolvedDatabase("")

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

func (UnresolvedDatabase) GetTableInsensitive(ctx context.Context, tblName string) (Table, bool, error) {
	return nil, false, nil
}

func (UnresolvedDatabase) GetTableNames(ctx context.Context) ([]string, error) {
	return []string{}, nil
}
