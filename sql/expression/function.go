package expression

import (
	"bytes"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// IsBinary is a function that returns whether a blob is binary or not.
type IsBinary struct {
	UnaryExpression
}

// NewIsBinary creates a new IsBinary expression.
func NewIsBinary(e sql.Expression) sql.Expression {
	return &IsBinary{UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (ib *IsBinary) Eval(row sql.Row) (interface{}, error) {
	v, err := ib.Child.Eval(row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return false, nil
	}

	blob, err := sql.Blob.Convert(v)
	if err != nil {
		return nil, err
	}

	return isBinary(blob.([]byte)), nil
}

// Name implements the Expression interface.
func (ib *IsBinary) Name() string {
	return "is_binary"
}

// TransformUp implements the Expression interface.
func (ib *IsBinary) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return NewIsBinary(f(ib.Child))
}

// Type implements the Expression interface.
func (ib *IsBinary) Type() sql.Type {
	return sql.Boolean
}

const sniffLen = 8000

// isBinary detects if data is a binary value based on:
// http://git.kernel.org/cgit/git/git.git/tree/xdiff-interface.c?id=HEAD#n198
func isBinary(data []byte) bool {
	if len(data) > sniffLen {
		data = data[:sniffLen]
	}

	if bytes.IndexByte(data, byte(0)) == -1 {
		return false
	}

	return true
}
