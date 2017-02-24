package analyzer

import (
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/sqle/sqle.v0/sql"
)

const maxAnalysisIterations = 1000

type Analyzer struct {
	Rules           []Rule
	Catalog         *sql.Catalog
	CurrentDatabase string
}

type Rule struct {
	Name  string
	Apply func(*Analyzer, sql.Node) sql.Node
}

func New(catalog *sql.Catalog) *Analyzer {
	return &Analyzer{
		Rules:   DefaultRules,
		Catalog: catalog,
	}
}

func (a *Analyzer) Analyze(n sql.Node) (sql.Node, error) {
	prev := n
	cur := a.analyzeOnce(n)
	i := 0
	for !reflect.DeepEqual(prev, cur) {
		prev = cur
		cur = a.analyzeOnce(n)
		i++
		if i >= maxAnalysisIterations {
			return cur, fmt.Errorf("exceeded max analysis iterations (%d)", maxAnalysisIterations)
		}
	}

	return cur, a.validate(cur)
}

func (a *Analyzer) analyzeOnce(n sql.Node) sql.Node {
	result := n
	for _, rule := range a.Rules {
		result = rule.Apply(a, result)
	}
	return result
}

func (a *Analyzer) validate(n sql.Node) error {
	if !n.Resolved() {
		return errors.New("plan is not resolved")
	}

	return nil
}
