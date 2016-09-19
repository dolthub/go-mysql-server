package sql

type Nameable interface {
	Name() string
}

type Node interface {
	Schema() Schema
	Children() []*Node
}

type PhysicalRelation interface {
	Nameable
	Node
	RowIter() (RowIter, error)
}
