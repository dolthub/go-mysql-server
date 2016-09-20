package parse

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mvader/gitql/sql/plan"
)

type ParseState uint

const (
	NilState ParseState = iota
	ErrorState
	SelectState
	SelectFieldList
	FromState
	FromListState
	WhereState
	WhereClauseState
	OrderState
	OrderClauseState
	DoneState

	ExprState
	ExprEnd
)

type parser struct {
	prevState  ParseState
	stateStack *stateStack
	lexer      *Lexer
	output     []*Token
	opStack    *tokenStack
	err        error

	projection []interface{}
}

func newParser(input io.Reader) *parser {
	return &parser{
		lexer:      NewLexer(input),
		stateStack: newStateStack(),
		opStack:    newTokenStack(),
	}
}

func (p *parser) parse() error {
	if err := p.lexer.Run(); err != nil {
		return err
	}

	state := p.stateStack.peek()
	for state != DoneState && state != ErrorState {
		p.prevState = state
		var t *Token
		switch state {
		case SelectState:
			t = p.lexer.Next()
			if t == nil {
				p.errorf("expecting 'SELECT', nothing received")
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "select") {
				p.errorf("expecting 'SELECT', %q received", t.Value)
			} else {
				p.stateStack.pop()
				p.stateStack.put(SelectFieldList)
			}

		case SelectFieldList:
			t = p.lexer.Next()
			if t == nil {
				p.errorf("expecting select field list expression, nothing received")
			} else if t.Type == KeywordToken && kwMatches(t.Value, "from") {
				if len(p.projection) > 0 {
					p.lexer.Backup()
					p.stateStack.pop()
					p.stateStack.put(FromState)
				} else {
					p.errorf(`unexpected "FROM", expecting select field list expression`)
				}
			} else {
				p.lexer.Backup()
				p.stateStack.put(ExprState)
			}

		case ExprState:
			expr, err := parseExpr(p.lexer)
			if err != nil {
				p.error(err)
			} else {
				p.projection = append(p.projection, expr)
			}
			p.stateStack.put(ExprEnd)

		case ExprEnd:
			t = p.lexer.Next()
			p.stateStack.pop()
			state := p.stateStack.peek()
			var (
				breakKeyword string
				nextState    ParseState
			)

			switch state {
			case SelectState:
				breakKeyword = "from"
				nextState = FromState
			case FromState:
				breakKeyword = "where"
				nextState = WhereState
			case WhereState:
				breakKeyword = "order"
				nextState = OrderState
			case OrderState:
			// empty on purpose
			default:
				p.errorf(`unexpected token %q`, t.Value)
				break
			}

			if t != nil {
				switch t.Type {
				case CommaToken:
					break
				case KeywordToken:
					if kwMatches(t.Value, breakKeyword) {
						p.stateStack.pop()
						p.stateStack.pop()
						p.stateStack.put(nextState)
						break
					}
				}
			}

			if breakKeyword != "" {
				p.errorf(`expecting "," or %q`, breakKeyword)
			} else {
				p.errorf(`expecting "," or end of sentence`)
			}
		}
	}

	return nil
}

func (p *parser) buildTree() *plan.Project {
	/*var fields []string
	if p.projection.typ == projectionFields {
		for _, f := range p.projection.fields {
			fields = append(fields, f.name)
		}
	}

	// TODO: build child
	return plan.NewProject(fields, nil)*/
	return nil
}

func Parse(input io.Reader) (*plan.Project, error) {
	p := newParser(input)
	if err := p.parse(); err != nil {
		return nil, err
	}

	return p.buildTree(), nil
}

func LastStates(input io.Reader) (ParseState, ParseState, error) {
	p := newParser(input)
	if err := p.parse(); err != nil {
		return NilState, NilState, err
	}

	return p.stateStack.pop(), p.prevState, nil
}

type tokenQueue interface {
	Backup()
	Next() *Token
}

func parseExpr(q tokenQueue) (interface{}, error) {
	var (
		output []*Token
		stack  = newTokenStack()
	)

OuterLoop:
	for {
		tk := q.Next()
		if tk == nil {
			break
		}

		switch tk.Type {
		case IntToken, StringToken, FloatToken:
			output = append(output, tk)

		case IdentifierToken:
			nt := q.Next()
			q.Backup()
			if nt != nil && nt.Type == LeftParenToken {
				tk.Type = FunctionToken
				stack.put(tk)
			} else {
				output = append(output, tk)
			}

		case LeftParenToken:
			stack.put(tk)

		case RightParenToken:
			for {
				t := stack.peek()
				if t == nil {
					return nil, errors.New(`unexpected ")"`)
				}

				if t.Type == LeftParenToken {
					stack.pop()
					t = stack.peek()
					if t != nil && t.Type == FunctionToken {
						output = append(output, stack.pop())
					}
					break
				}

				output = append(output, stack.pop())
			}

		case CommaToken:
			for {
				t := stack.peek()
				if t != nil {
					q.Backup()
					break OuterLoop
				}

				if t.Type == LeftParenToken {
					break
				}

				output = append(output, stack.pop())
			}

		case KeywordToken:
			op := opTable[tk.Value]
			if op == nil {
				q.Backup()
				break OuterLoop
			}

			tk.Type = OpToken
			fallthrough
		case OpToken:
			for {
				t := stack.peek()
				if t == nil || t.Type != OpToken {
					break
				}

				o1 := opTable[tk.Value]
				o2 := opTable[t.Value]
				if o1.isLeftAssoc() && o1.comparePrecedence(o2) <= 0 ||
					o1.isRightAssoc() && o1.comparePrecedence(o2) < 0 {
					output = append(output, stack.pop())
				} else {
					break
				}
			}
			stack.put(tk)
		}
	}

	for {
		tk := stack.pop()
		if tk == nil {
			break
		}

		if tk.Type == LeftParenToken {
			return nil, errors.New(`missing closing ")"`)
		}

		output = append(output, tk)
	}

	return nil, nil
}

func (p *parser) errorf(msg string, args ...interface{}) {
	p.err = fmt.Errorf(msg, args...)
	p.stateStack.put(ErrorState)
}

func (p *parser) error(err error) {
	p.err = err
	p.stateStack.put(ErrorState)
}

func kwMatches(tested, expected string) bool {
	return strings.ToLower(tested) == expected
}
