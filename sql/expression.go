package sql

type Expression interface {
	Type() Type
	Eval(Row) interface{}
}
