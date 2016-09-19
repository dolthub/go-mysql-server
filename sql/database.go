package sql

type Database interface {
	Nameable
	Relations() map[string]PhysicalRelation
}
