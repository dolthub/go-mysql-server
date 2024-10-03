package rdparser

import ast "github.com/dolthub/vitess/go/vt/sqlparser"

func (p *parser) subquery() (*ast.Subquery, bool) {
	id, _ := p.peek()
	if id != '(' {
		return nil, false
	}
	p.next()
	selNoInt, ok := p.sel()
	if !ok {
		return nil, false
	}
	return &ast.Subquery{Select: selNoInt}, true
}
