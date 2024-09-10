package rdparser

import (
	"context"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

type parser struct {
	tok   *ast.Tokenizer
	curOk bool
	curId int
	cur   []byte
}

func NewParser() sql.Parser {
	return &parser{}
}

func (p *parser) parse(ctx context.Context, s string, options ast.ParserOptions) (ast.Statement, error) {
	// get next token
	p.tok = ast.NewStringTokenizer(s)
	if options.AnsiQuotes {
		p.tok = ast.NewStringTokenizerForAnsiQuotes(s)
	}

	if prePlan, ok := p.statement(ctx); ok {
		return prePlan, nil
	}

	return ast.ParseWithOptions(ctx, s, options)
}

var _ sql.Parser = (*parser)(nil)

func (p *parser) ParseSimple(query string) (ast.Statement, error) {
	return p.parse(context.Background(), query, ast.ParserOptions{})
}

func (p *parser) Parse(ctx *sql.Context, query string, multi bool) (ast.Statement, string, string, error) {
	return p.ParseWithOptions(ctx, query, ';', multi, ast.ParserOptions{})
}

func (p *parser) ParseWithOptions(ctx context.Context, query string, delimiter rune, multi bool, options ast.ParserOptions) (ast.Statement, string, string, error) {
	stmt, err := p.parse(context.Background(), query, options)
	if err != nil {
		return nil, "", "", nil
	}
	return stmt, "", "", nil
}

func (p *parser) ParseOneWithOptions(ctx context.Context, s string, options ast.ParserOptions) (ast.Statement, int, error) {
	ast, err := p.parse(ctx, s, options)
	if err != nil {
		return nil, 0, err
	}
	return ast, 0, nil
}
