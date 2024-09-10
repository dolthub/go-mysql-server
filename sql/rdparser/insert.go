package rdparser

import (
	"context"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (p *parser) insert(ctx context.Context) (ast.Statement, bool) {
	id, cur := p.tok.Scan()
	ins := new(ast.Insert)
	ins.Action = ast.InsertStr
	if id == ast.INTO {
		id, cur = p.tok.Scan()
	} else if id != ast.ID {
		return nil, false
	}

	p.push(id, cur)

	var ok bool
	ins.Table, ok = p.tableIdent(ctx)
	if !ok {
		return nil, false
	}

	// optional ()
	id, cur = p.pop()
	if id == '(' {
		ins.Columns, ok = p.columnList(ctx)
		if !ok {
			return nil, false
		}
		id, cur = p.pop()
		if id != ')' {
			return nil, false
		}
	}

	// VALUES or SELECT
	if id != ast.VALUES {
		return nil, false
	}

	ins.Rows, ok = p.valueList(ctx)
	if !ok {
		return nil, false
	}
	return ins, true
}

func (p *parser) push(id int, cur []byte) {
	p.curId, p.cur, p.curOk = id, cur, true
}

func (p *parser) pop() (int, []byte) {
	if p.curOk {
		p.curOk = false
		return p.curId, p.cur
	} else {
		return p.tok.Scan()
	}
}

func (p *parser) tableIdent(ctx context.Context) (ast.TableName, bool) {
	// schema.database.table

	id, firstTok := p.pop()
	if id != ast.ID {
		return ast.TableName{}, false
	}

	id, tok := p.pop()
	if id != '.' {
		p.push(id, tok)
		return ast.TableName{Name: ast.NewTableIdent(string(firstTok))}, true
	}

	id, secondTok := p.tok.Scan()
	if id != ast.ID {
		p.push(id, tok)
		return ast.TableName{
			Name: ast.NewTableIdent(string(firstTok)),
		}, true
	}

	id, tok = p.pop()
	if id != '.' {
		p.push(id, tok)
		return ast.TableName{
			DbQualifier: ast.NewTableIdent(string(firstTok)),
			Name:        ast.NewTableIdent(string(secondTok)),
		}, true
	}

	id, thirdTok := p.tok.Scan()
	if id != ast.ID {
		p.push(id, tok)
		return ast.TableName{
			SchemaQualifier: ast.NewTableIdent(string(firstTok)),
			DbQualifier:     ast.NewTableIdent(string(secondTok)),
		}, true
	}

	return ast.TableName{
		SchemaQualifier: ast.NewTableIdent(string(firstTok)),
		DbQualifier:     ast.NewTableIdent(string(secondTok)),
		Name:            ast.NewTableIdent(string(thirdTok)),
	}, true
}

func (p *parser) columnList(ctx context.Context) (ast.Columns, bool) {
	// id, ...
	var cols ast.Columns
	id, tok := p.pop()
	for {
		if id != ast.ID {
			break
		}
		cols = append(cols, ast.NewColIdent(string(tok)))
		id, tok = p.tok.Scan()
		if id != ',' {
			break
		}
	}
	p.push(id, tok)
	return cols, true
}

func (p *parser) valueList(ctx context.Context) (ast.InsertRows, bool) {
	var rows ast.Values
	id, tok := p.pop()
	for {
		if id != '(' {
			break
		}
		var row ast.ValTuple
		for {
			id, tok = p.pop()
			if id == ',' {
				id, tok = p.pop()
			}
			if id == ')' {
				break
			}
			value, ok := p.value(ctx, id, tok)
			if !ok {
				return nil, false
			}
			row = append(row, value)
		}
		rows = append(rows, row)
		id, tok = p.tok.Scan()
		if id != ',' {
			break
		}
		id, tok = p.tok.Scan()
	}
	p.push(id, tok)
	return &ast.AliasedValues{Values: rows}, true
}

func (p *parser) value(ctx context.Context, id int, tok []byte) (ast.Expr, bool) {
	switch id {
	case ast.STRING:
		return ast.NewStrVal(tok), true
	case ast.INTEGRAL:
		return ast.NewIntVal(tok), true
	case ast.FLOAT:
		return ast.NewFloatVal(tok), true
	case ast.NULL:
		return ast.NewStrVal(tok), true
	default:
		return nil, false
	}
}
