package function

import (
	"bytes"
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// IsBinary is a function that returns whether a blob is binary or not.
type IsBinary struct {
	expression.UnaryExpression
}

// NewIsBinary creates a new IsBinary expression.
func NewIsBinary(e sql.Expression) sql.Expression {
	return &IsBinary{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (ib *IsBinary) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := ib.Child.Eval(ctx, row)
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

	blobBytes := blob.([]byte)
	return isBinary(blobBytes), nil
}

func (ib *IsBinary) String() string {
	return fmt.Sprintf("IS_BINARY(%s)", ib.Child)
}

// TransformUp implements the Expression interface.
func (ib *IsBinary) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := ib.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewIsBinary(child))
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
