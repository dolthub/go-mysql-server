package sql

type Nameable interface {
	Name() string
}

type Node interface {
	Schema() Schema
	Children() []Node
	RowIter() (RowIter, error)
}

type PhysicalRelation interface {
	Nameable
	Node
}
