package rdparser

import (
	"context"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (p *parser) sel(ctx context.Context) (ast.Statement, bool) {
	// SELECT <exprs,> FROM <table> WHERE <expr>
	sel := new(ast.Select)
	var ok bool
	sel.SelectExprs, ok = p.selExprs(ctx)
	if !ok {
		return nil, false
	}

	tab, ok := p.tableIdent(ctx)
	if !ok {
		return nil, false
	}
	sel.From = []ast.TableExpr{&ast.AliasedTableExpr{Expr: tab}}

	sel.Where, ok = p.whereOpt(ctx)
	if !ok {
		return nil, false
	}
	return sel, true
}

func (p *parser) selExprs(ctx context.Context) (ast.SelectExprs, bool) {
	var exprs ast.SelectExprs
	id, tok := p.tok.Scan()
	for {
		if id == ast.FROM {
			break
		}
		// literal
		var expr ast.SelectExpr
		var toAlias ast.Expr
		var ok bool
		switch id {
		case ast.ID:
			p.push(id, tok)
			toAlias, ok = p.colName(ctx)
			if !ok {
				return nil, false
			}
		case '*':
			expr = &ast.StarExpr{}
		case ast.STRING, ast.INTEGRAL, ast.FLOAT, ast.NULL:
			toAlias, ok = p.value(ctx, id, tok)
			if !ok {
				return nil, false
			}
		default:
			return nil, false
		}
		if toAlias != nil {
			expr = &ast.AliasedExpr{Expr: toAlias}
			id, tok = p.pop()
			if id == ast.AS {
				expr = &ast.AliasedExpr{As: ast.NewColIdent(string(tok)), Expr: toAlias}
			}
		}
		exprs = append(exprs, expr)
	}
	return exprs, true
}

func (p *parser) colName(ctx context.Context) (*ast.ColName, bool) {
	id, firstTok := p.pop()
	if id != ast.ID {
		return nil, false
	}

	id, tok := p.pop()
	if id != '.' {
		p.push(id, tok)
		return &ast.ColName{Name: ast.NewColIdent(string(firstTok))}, true
	}

	id, secondTok := p.tok.Scan()
	if id != ast.ID {
		return nil, false
	}

	return &ast.ColName{
		Qualifier: ast.TableName{
			Name: ast.NewTableIdent(string(firstTok)),
		},
		Name: ast.NewColIdent(string(secondTok)),
	}, true

	//id, tok = p.pop()
	//if id == ast.AS {
	//	return &ast.AliasedExpr{
	//		As: ast.NewColIdent(string(tok)),
	//		Expr: &ast.ColName{
	//			Qualifier: ast.TableName{
	//				Name: ast.NewTableIdent(string(firstTok)),
	//			},
	//			Name: ast.NewColIdent(string(secondTok)),
	//		},
	//	}, true
	//}
	//
	//p.push(id, tok)
	//return &ast.AliasedExpr{
	//	Expr: &ast.ColName{
	//		Qualifier: ast.TableName{
	//			Name: ast.NewTableIdent(string(firstTok)),
	//		},
	//		Name: ast.NewColIdent(string(secondTok)),
	//	},
	//}, true
	//
	// db, schema, ...
}

func (p *parser) whereOpt(ctx context.Context) (*ast.Where, bool) {
	id, tok := p.pop()
	if id != ast.WHERE {
		p.push(id, tok)
		return nil, true
	}
	ret := new(ast.Where)
	ret.Type = ast.WhereStr
	var ok bool
	ret.Expr, ok = p.expr(ctx)
	if !ok {
		return nil, false
	}
	return ret, true
}

func (p *parser) expr(ctx context.Context) (ast.Expr, bool) {
	firstExpr, ok := p.exprHelper(ctx)
	if !ok {
		return nil, false
	}
	secondExpr, ok := p.exprHelper(ctx)
	if !ok {
		return firstExpr, true
	}
	switch e := secondExpr.(type) {
	case *ast.ComparisonExpr:
		thirdExpr, ok := p.exprHelper(ctx)
		if !ok {
			return nil, false
		}
		e.Left = firstExpr
		e.Right = thirdExpr
		return e, true
	default:
		return nil, false
	}
}

func (p *parser) exprHelper(ctx context.Context) (ast.Expr, bool) {
	id, tok := p.pop()
	var expr ast.Expr
	var ok bool
	switch id {
	case ast.ID:
		p.push(id, tok)
		expr, ok = p.colName(ctx)
	case ast.STRING, ast.INTEGRAL, ast.FLOAT, ast.NULL:
		expr, ok = p.value(ctx, id, tok)
	case '=':
		expr = &ast.ComparisonExpr{Operator: ast.EqualStr}
		ok = true
	default:
		return nil, false
	}
	if !ok {
		return nil, false
	}
	return expr, true
}
