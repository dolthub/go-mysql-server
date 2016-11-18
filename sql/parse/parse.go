package parse

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"
	"github.com/gitql/gitql/sql/plan"
)

type ParseState uint

const (
	NilState ParseState = iota
	ErrorState
	SelectState
	SelectFieldList
	FromState
	FromTableState
	WhereState
	WhereClauseState
	OrderState
	OrderByState
	OrderClauseState
	LimitState
	LimitNumberState
	DoneState

	ExprState
	ExprEndState
)

type parser struct {
	prevState  ParseState
	stateStack *stateStack
	lexer      *Lexer
	output     []*Token
	opStack    *tokenStack
	err        error

	projection    []sql.Expression
	table         string
	filterClauses []sql.Expression
	sortFields    []plan.SortField
	limit         *int
}

func newParser(input io.Reader) *parser {
	state := newStateStack()
	state.put(SelectState)
	return &parser{
		lexer:      NewLexer(input),
		stateStack: state,
		opStack:    newTokenStack(),
	}
}

func (p *parser) parse() error {
	if err := p.lexer.Run(); err != nil {
		return err
	}

	for state := p.stateStack.peek(); state != DoneState && state != ErrorState; state = p.stateStack.peek() {
		p.prevState = state
		var t *Token
	OuterSwitch:
		switch state {
		case SelectState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting 'SELECT', nothing received")
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "select") {
				p.errorf("expecting 'SELECT', %q received", t.Value)
			} else {
				p.stateStack.put(SelectFieldList)
			}

		case SelectFieldList:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting select field list expression, nothing received")
			} else if t.Type == KeywordToken && kwMatches(t.Value, "from") {
				p.errorf(`unexpected "FROM", expecting select field list expression`)
			} else {
				p.lexer.Backup()
				p.stateStack.pop()
				p.stateStack.put(ExprState)
			}

		case ExprState:
			expr, err := parseExpr(p.lexer)
			if err != nil {
				p.error(err)
				break
			}

			p.stateStack.pop()
			state := p.stateStack.peek()
			switch state {
			case SelectState:
				p.projection = append(p.projection, expr)

			case WhereState:
				p.filterClauses = append(p.filterClauses, expr)
			}

			p.stateStack.put(ExprEndState)

		case ExprEndState:
			t = p.lexer.Next()
			p.stateStack.pop()
			state := p.stateStack.peek()
			var (
				breakKeywords []string
				nextState     ParseState
			)

			switch state {
			case SelectState:
				breakKeywords = []string{"from"}
				nextState = FromState
			case WhereState:
				breakKeywords = []string{"order", "limit"}
				nextState = OrderState
			default:
				p.errorf(`unexpected token %q`, t.Value)
				break
			}

			if t != nil {
				switch t.Type {
				case CommaToken:
					p.stateStack.put(ExprState)
					break OuterSwitch
				case KeywordToken:
					for _, kw := range breakKeywords {
						if kwMatches(t.Value, kw) {
							p.lexer.Backup()
							p.stateStack.pop()
							p.stateStack.put(nextState)
							break OuterSwitch
						}
					}
				case EOFToken:
					p.stateStack.pop()
					p.stateStack.put(DoneState)
					break OuterSwitch
				}
			}

			if len(breakKeywords) > 0 {
				p.errorf(`expecting "," or %q`, breakKeywords)
			} else {
				p.errorf(`expecting "," or end of sentence`)
			}

		case FromState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting 'FROM', nothing received")
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "from") {
				p.errorf("expecting 'FROM', %q received", t.Value)
			} else {
				p.stateStack.pop()
				p.stateStack.put(FromTableState)
			}

		case FromTableState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting table name, nothing received")
			} else if t.Type != IdentifierToken {
				p.errorf("expecting table name, %q received instead", t.Value)
			} else {
				p.table = t.Value
				p.stateStack.pop()
				p.stateStack.put(WhereState)
			}

		case WhereState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.stateStack.pop()
				p.stateStack.put(DoneState)
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "where") {
				p.lexer.Backup()
				p.stateStack.pop()
				p.stateStack.put(OrderState)
			} else {
				p.stateStack.put(WhereClauseState)
			}

		case WhereClauseState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting where clause, nothing received")
			} else {
				p.lexer.Backup()
				p.stateStack.pop()
				p.stateStack.put(ExprState)
			}

		case OrderState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.stateStack.pop()
				p.stateStack.put(DoneState)
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "order") {
				p.lexer.Backup()
				p.stateStack.pop()
				p.stateStack.put(LimitState)
			} else {
				p.stateStack.put(OrderByState)
			}

		case OrderByState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf(`expecting "BY", nothing received`)
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "by") {
				p.errorf("expecting 'BY', %q received", t.Value)
			} else {
				p.stateStack.put(OrderClauseState)
			}

		case OrderClauseState:
			fields, err := parseOrderClause(p.lexer)
			if err != nil {
				p.error(err)
			} else {
				p.sortFields = fields
				p.stateStack.pop()
				p.stateStack.put(LimitState)
			}

		case LimitState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.stateStack.pop()
				p.stateStack.put(DoneState)
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "limit") {
				p.errorf("expecting 'LIMIT', %q received", t.Value)
			} else {
				p.stateStack.pop()
				p.stateStack.put(LimitNumberState)
			}

		case LimitNumberState:
			t = p.lexer.Next()
			if t == nil || t.Type == EOFToken {
				p.errorf("expecting integer, nothing received")
			} else if t.Type != IntToken {
				p.errorf("expecting integer, %q received", t.Value)
			} else {
				i, err := strconv.Atoi(t.Value)
				if err != nil {
					p.errorf("error parsing integer: %q", err)
				}

				p.limit = &i
				p.stateStack.pop()
				p.stateStack.put(DoneState)
			}
		}
	}

	return nil
}

func (p *parser) buildPlan() (sql.Node, error) {
	if p.table == "" {
		return nil, errors.New("missing table name")
	}

	var node sql.Node = plan.NewUnresolvedTable(p.table)

	if len(p.filterClauses) > 0 {
		node = plan.NewFilter(p.filterClauses[0], node)
	}

	if len(p.sortFields) > 0 {
		node = plan.NewSort(p.sortFields, node)
	}

	if p.limit != nil {
		node = plan.NewLimit(int64(*p.limit), node)
	}

	node = plan.NewProject(p.projection, node)

	return node, nil
}

func Parse(input io.Reader) (sql.Node, error) {
	p := newParser(input)
	if err := p.parse(); err != nil {
		return nil, err
	}

	return p.buildPlan()
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

func parseOrderClause(q tokenQueue) ([]plan.SortField, error) {
	var (
		fields []plan.SortField
		field  *plan.SortField
	)

	for {
		tk := q.Next()
		if tk == nil {
			return nil, errors.New("unexpected end of input")
		}
		switch tk.Type {
		case IdentifierToken:
			if field != nil {
				return nil, fmt.Errorf(`expecting "DESC", "ASC" or ",", received %q`, tk.Value)
			}

			field = &plan.SortField{Column: expression.NewUnresolvedColumn(tk.Value)}
		case KeywordToken:
			if field == nil {
				return nil, fmt.Errorf(`unexpected keyword %q, expecting identifier`, tk.Value)
			}

			if kwMatches(tk.Value, "desc") {
				field.Order = plan.Descending
			} else if kwMatches(tk.Value, "asc") {
				field.Order = plan.Ascending
			} else if kwMatches(tk.Value, "limit") {
				if field == nil {
					return nil, errors.New(`unexpected LIMIT, expecting identifier`)
				}

				q.Backup()
				fields = append(fields, *field)
				return fields, nil
			} else {
				return nil, fmt.Errorf(`unexpected keyword %q, expecting "ASC", "DESC" or ","`, tk.Value)
			}
		case CommaToken:
			if field == nil {
				return nil, errors.New(`unexpected ",", expecting identifier`)
			}

			fields = append(fields, *field)
			field = nil
		case EOFToken:
			if field == nil {
				return nil, errors.New(`unexpected end of input, expecting identifier`)
			}

			fields = append(fields, *field)
			return fields, nil
		default:
			return nil, fmt.Errorf(`unexpected token %q on order by field list`, tk.Value)
		}
	}
}

func parseExpr(q tokenQueue) (sql.Expression, error) {
	var (
		output = newTokenStack()
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
			output.put(tk)

		case IdentifierToken:
			nt := q.Next()
			q.Backup()
			if nt != nil && nt.Type == LeftParenToken {
				tk.Type = FunctionToken
				stack.put(tk)
			} else {
				output.put(tk)
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
						output.put(stack.pop())
					}
					break
				}

				output.put(stack.pop())
			}

		case EOFToken:
			q.Backup()
			break OuterLoop

		case CommaToken:
			for {
				t := stack.peek()
				if t == nil {
					q.Backup()
					break OuterLoop
				}

				if t.Type == LeftParenToken {
					break
				}

				output.put(stack.pop())
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
					output.put(stack.pop())
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

		output.put(tk)
	}

	return assembleExpression(output)
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
