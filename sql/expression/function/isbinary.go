package function

import (
	"bytes"
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
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

// WithChildren implements the Expression interface.
func (ib *IsBinary) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ib, len(children), 1)
	}
	return NewIsBinary(children[0]), nil
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
