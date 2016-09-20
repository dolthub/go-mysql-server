package expression

import "github.com/mvader/gitql/sql"

type Identifier struct {
	name string
}

func NewIdentifier(name string) *Identifier {
	return &Identifier{
		name: name,
	}
}

func (i Identifier) Type() sql.Type {
	return sql.String
}

func (i Identifier) Eval(row sql.Row) interface{} {
	// TODO: return real value
	return i.name
}
