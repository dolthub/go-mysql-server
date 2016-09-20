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
)

type projection struct {
	typ    projectionType
	fields []*projectionField
}

type projectionType byte

const (
	projectionAll projectionType = iota
	projectionFields
)

type projectionField struct {
	parent string
	name   string
}

type parser struct {
	prevState ParseState
	state     ParseState
	lexer     *Lexer
	output    []*Token
	opStack   *stack
	err       error

	projection *projection
}

func newParser(input io.Reader) *parser {
	return &parser{
		lexer:   NewLexer(input),
		opStack: newStack(),
	}
}

func (p *parser) parse() error {
	if err := p.lexer.Run(); err != nil {
		return err
	}

	for p.state != DoneState && p.state != ErrorState {
		p.prevState = p.state
		var t *Token
		switch p.state {
		case SelectState:
			t = p.lexer.Next()
			if t == nil {
				p.errorf("expecting 'SELECT', nothing received")
			} else if t.Type != KeywordToken || !kwMatches(t.Value, "select") {
				p.errorf("expecting 'SELECT', %q received", t.Value)
			} else {
				p.state = SelectFieldList
				p.projection = &projection{}
			}
		case SelectFieldList:
			proj, err := parseSelectFields(p.lexer)
			if err != nil {
				p.error(err)
			} else {
				p.projection = proj
				p.state = FromState
			}
		}
	}

	return nil
}

func (p *parser) buildTree() *plan.Project {
	var fields []string
	if p.projection.typ == projectionFields {
		for _, f := range p.projection.fields {
			fields = append(fields, f.name)
		}
	}

	// TODO: build child
	return plan.NewProject(fields, nil)
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

	return p.state, p.prevState, nil
}

type tokenQueue interface {
	Backup()
	Next() *Token
}

func parseSelectFields(q tokenQueue) (*projection, error) {
	t := q.Next()

	if t == nil {
		return nil, errors.New("expecting fields to select, nothing found")
	}

	if t.Type == OpToken {
		if t.Value != "*" {
			return nil, fmt.Errorf("unexpected operator %q found in select field list", t.Value)
		}

		return &projection{typ: projectionAll}, nil
	}

	q.Backup()
	return parseSelectFieldList(q)
}

func parseSelectFieldList(q tokenQueue) (*projection, error) {
	var (
		projection = &projection{typ: projectionFields}
		f          *projectionField
	)

	for {
		t := q.Next()
		switch t.Type {
		case IdentifierToken:
			if f != nil && f.name != "" {
				return nil, fmt.Errorf(`expecting "," identifier %q received instead`, t.Value)
			}

			if f == nil {
				f = &projectionField{name: t.Value}
			} else {
				f.name = t.Value
			}
		case DotToken:
			if f == nil || f.name == "" {
				return nil, errors.New(`unexpected ".", expecting identifier`)
			}

			if f.parent != "" {
				return nil, errors.New(`unexpected ".", expecting ","`)
			}

			f = &projectionField{f.name, ""}
		case CommaToken:
			if f == nil || f.name == "" {
				return nil, errors.New(`unexpected ",", expecting identifier`)
			}

			projection.fields = append(projection.fields, f)
			f = nil
		default:
			q.Backup()
			if f != nil {
				projection.fields = append(projection.fields, f)
			}
			return projection, nil
		}
	}
}

func (p *parser) errorf(msg string, args ...interface{}) {
	p.err = fmt.Errorf(msg, args...)
	p.state = ErrorState
}

func (p *parser) error(err error) {
	p.err = err
	p.state = ErrorState
}

func kwMatches(tested, expected string) bool {
	return strings.ToLower(tested) == expected
}
