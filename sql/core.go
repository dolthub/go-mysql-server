package sql

import "errors"

type Nameable interface {
	Name() string
}

type Resolvable interface {
	Resolved() bool
}

type Transformable interface {
	TransformUp(func(Node) Node) Node
	TransformExpressionsUp(func(Expression) Expression) Node
}

type Node interface {
	Resolvable
	Transformable
	Schema() Schema
	Children() []Node
	RowIter() (RowIter, error)
}

type PhysicalRelation interface {
	Nameable
	Node
}

var ErrInvalidType = errors.New("invalid type")
