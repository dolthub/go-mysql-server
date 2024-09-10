package rdparser

import (
	"context"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (p *parser) statement(ctx context.Context) (ast.Statement, bool) {
	id, _ := p.tok.Scan()
	switch id {
	case ast.INSERT:
		return p.insert(ctx)
	case ast.SELECT:
		return p.sel(ctx)
	default:
		return nil, false
	}
}
