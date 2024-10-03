package rdparser

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (p *parser) sel() (ast.Statement, bool) {
	// SELECT <exprs,> FROM <table> WHERE <expr>
	sel := new(ast.Select)
	var ok bool
	sel.SelectExprs, ok = p.selExprs()
	if !ok {
		return nil, false
	}

	tab, ok := p.tableIdent()
	if !ok {
		return nil, false
	}
	sel.From = []ast.TableExpr{&ast.AliasedTableExpr{Expr: tab}}

	sel.Where, ok = p.whereOpt()
	if !ok {
		return nil, false
	}
	return sel, true
}

func (p *parser) selExprs() (ast.SelectExprs, bool) {
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
			toAlias, ok = p.columnName()
			if !ok {
				return nil, false
			}
		case '*':
			expr = &ast.StarExpr{}
		case ast.STRING, ast.INTEGRAL, ast.FLOAT, ast.NULL:
			toAlias, ok = p.value(id, tok)
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

func (p *parser) columnName() (*ast.ColName, bool) {
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

func (p *parser) whereOpt() (*ast.Where, bool) {
	id, tok := p.pop()
	if id != ast.WHERE {
		p.push(id, tok)
		return nil, true
	}
	ret := new(ast.Where)
	ret.Type = ast.WhereStr
	var ok bool
	ret.Expr, ok = p.expression()
	if !ok {
		return nil, false
	}
	return ret, true
}

func (p *parser) expression() (ret ast.Expr, ok bool) {
	// condition
	// NOT
	// DEFAULT
	// valueExpression
	id, _ := p.peek()
	if id == ast.NOT {
		p.next()
		c, ok := p.expression()
		if !ok {
			return nil, false
		}
		ret = &ast.NotExpr{Expr: c}
	} else if id == ast.DEFAULT {
		p.next()
		var d string
		id, _ = p.peek()
		if id == '(' {
			p.next()
			if ident, ok := p.id(); ok {
				id, _ = p.peek()
				if id != ')' {
					p.next()
					ret = &ast.Default{ColName: ident}
				} else {
					p.fail("invalid default expression")
				}
			}
		}
		ret = &ast.Default{ColName: d}
	} else if ret, ok = p.condition(); ok {
		return ret, true
	} else if ret, ok = p.valueExpression(); ok {
		return ret, true
	} else {
		return nil, false
	}

	id, _ = p.peek()
	// AND OR XOR IS
	switch id {
	case ast.AND:
		p.next()
		right, ok := p.expression()
		if ok {
			ret = &ast.AndExpr{Left: ret, Right: right}
		}
	case ast.OR:
		p.next()
		if right, ok := p.expression(); ok {
			ret = &ast.OrExpr{Left: ret, Right: right}
		}
	case ast.XOR:
		p.next()
		if right, ok := p.expression(); ok {
			ret = &ast.XorExpr{Left: ret, Right: right}
		}
	case ast.IS:
		p.next()
		if is := p.isSuffix(); is != "" {
			ret = &ast.IsExpr{Operator: is, Expr: ret}
		}
	}
	return ret, true
}

func (p *parser) id() (string, bool) {
	id, tok := p.peek()
	if id == ast.ID {
		p.next()
		return string(tok), true
	}
	return "", false
}

func (p *parser) isSuffix() string {
	// (NOT) NULL|TRUE|FALSE
	id, _ := p.peek()
	var not bool
	if id == ast.NOT {
		p.next()
		not = true
	}
	id, _ = p.peek()
	switch id {
	case ast.TRUE:
		if not {
			return ast.IsNotTrueStr
		} else {
			return ast.IsTrueStr
		}
	case ast.FALSE:
		if not {
			return ast.IsNotFalseStr
		} else {
			return ast.IsFalseStr
		}
	case ast.NULL:
		if not {
			return ast.IsNotNullStr
		} else {
			return ast.IsNullStr
		}
	default:
		return ""
	}
}

func (p *parser) condition() (ret ast.Expr, ok bool) {
	id, _ := p.peek()
	if id == ast.EXISTS {
		if subq, ok := p.subquery(); ok {
			return &ast.ExistsExpr{Subquery: subq}, true
		}
		p.fail("expected subquery after EXISTS")
	}

	left, ok := p.valueExpression()
	if !ok {
		return nil, false
	}

	id, _ = p.peek()
	var not bool
	if id == ast.NOT {
		p.next()
		id, _ = p.peek()
		not = true
	}

	if comp := p.compare(); comp != "" {
		right, ok := p.valueExpression()
		if !ok {
			return nil, false
		}
		ret = &ast.ComparisonExpr{Operator: comp, Left: left, Right: right}
	} else {
		switch id {
		case ast.IN:
			right, ok := p.colTuple()
			if !ok {
				return nil, false
			}
			ret = &ast.ComparisonExpr{Operator: ast.InStr, Left: left, Right: right}
		case ast.LIKE:
			right, ok := p.valueExpression()
			if !ok {
				return nil, false
			}
			esc := p.likeEscapeOpt()
			ret = &ast.ComparisonExpr{Operator: ast.LikeStr, Left: left, Right: right, Escape: esc}
		case ast.REGEXP:
			right, ok := p.valueExpression()
			if !ok {
				return nil, false
			}
			ret = &ast.ComparisonExpr{Operator: ast.RegexpStr, Left: left, Right: right}
		case ast.BETWEEN:
			from, ok := p.valueExpression()
			if !ok {
				return nil, false
			}
			id, _ = p.next()
			if id != ast.AND {
				p.fail("between expected AND")
			}
			to, ok := p.valueExpression()
			if !ok {
				return nil, false
			}
			ret = &ast.RangeCond{Operator: ast.BetweenStr, Left: left, From: from, To: to}
		}
	}

	if not {
		ret = &ast.NotExpr{Expr: ret}
	}
	return ret, true
}

func (p *parser) valueExpression() (ret ast.Expr, ok bool) {
	// value
	// ACCOUNT, FORMAT
	// boolean_value
	// column_name
	// column_name_safe_keyword
	// tuple_expression
	// subquery
	// BINARY
	id, tok := p.peek()

	if id == ast.FORMAT || id == ast.ACCOUNT {
		return &ast.ColName{Name: ast.NewColIdent(string(tok))}, true
	} else if v, ok := p.value(id, tok); ok {
		return v, true
	} else if id == ast.TRUE || id == ast.FALSE {
		return ast.BoolVal(id == ast.TRUE), true
	} else if col, ok := p.columnName(); ok {
		p.next()
		id, _ = p.peek()
		switch id {
		case ast.JSON_EXTRACT_OP:
			p.next()
			id, tok := p.next()
			val, ok := p.value(id, tok)
			if !ok {
				return nil, false
			}
			return &ast.BinaryExpr{Operator: ast.JSONExtractOp, Left: col, Right: val}, true
		case ast.JSON_UNQUOTE_EXTRACT_OP:
			p.next()
			id, tok := p.next()
			val, ok := p.value(id, tok)
			if !ok {
				return nil, false
			}
			return &ast.BinaryExpr{Operator: ast.JSONUnquoteExtractOp, Left: col, Right: val}, true
		default:
			return col, true
		}
		return col, true
	} else if col, ok := p.columnNameSafeKeyWord(); ok {
		return col, true
	} else if tup, ok := p.tupleExpression(); ok {
		return tup, true
	} else if subq, ok := p.subquery(); ok {
		return subq, true
	}

	// underscore_charsets valueExpr UNARY
	// +  valueExpr UNARY
	// -  valueExpr UNARY
	// !  valueExpr UNARY
	// ~  valueExpr
	// INTERVAL value_expression sql_id

	// function_call_generic
	// function_call_keyword
	// function_call_nonkeyword
	// function_call_conflict
	// function_call_window
	// function_call_aggregate_with_window

	// valueExpr op valueExpr
	left, ok := p.valueExpression()
	if ok {
		return nil, false
	}
	switch id {
	case '+':
	case '-':
	case '*':
	case '/':
	case '^':
	case '&':
	case '|':
	case ast.DIV:
	case '%':
	case ast.MOD:
	case ast.SHIFT_LEFT:
	case ast.SHIFT_RIGHT:
	case ast.COLLATE:
	}
}

func (p *parser) compare() (ret string) {
	id, _ := p.peek()
	switch id {
	case '=':
		ret = ast.EqualStr
	case '<':
		ret = ast.LessThanStr
	case '>':
		ret = ast.GreaterThanStr
	case ast.LE:
		ret = ast.LessEqualStr
	case ast.GE:
		ret = ast.GreaterEqualStr
	case ast.NE:
		ret = ast.NotEqualStr
	case ast.NULL_SAFE_EQUAL:
		ret = ast.NullSafeEqualStr
	default:
		return ""
	}
	p.next()
	return ret
}

func (p *parser) colTuple() (ast.Expr, bool) {
	id, tok := p.peek()
	if id == ast.LIST_ARG {
		return ast.ListArg(tok), true
	} else if id != '(' {
		return nil, false
	}
	p.next()
	id, _ = p.peek()

	if id == ast.SELECT {
		selStmt, ok := p.selNoInto()
		if !ok {
			return nil, false
		}
		p.next()
		id, _ = p.next()
		if id != ')' {
			p.fail("expected subquery to end with ')'")
		}
		p.next()
		return &ast.Subquery{Select: selStmt}, true
	}

	//expr list
	var tup ast.ValTuple
	for {
		e, ok := p.expression()
		if !ok {
			return nil, false
		}
		tup = append(tup, e)
		id, _ = p.next()
		if id == ')' {
			break
		} else if id != ',' {
			p.fail("invalid expression list")
		}
	}
	return tup, true
}

func (p *parser) selNoInto() (*ast.Select, bool) {
}

func (p *parser) likeEscapeOpt() ast.Expr {
	id, _ := p.peek()
	if id != ast.ESCAPE {
		return nil
	}
	p.next()
	ret, ok := p.valueExpression()
	if !ok {
		p.fail("expected value expression after ESCAPE")
	}
	return ret
}
