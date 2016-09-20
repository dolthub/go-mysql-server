package sql

import "errors"

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

var ErrInvalidType = errors.New("invalid type")
