package sql

type Expression interface {
	Type() Type
	Name() string
	Eval(Row) interface{}
}
