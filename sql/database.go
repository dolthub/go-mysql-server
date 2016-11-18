package sql

type Database interface {
	Nameable
	Tables() map[string]Table
}
