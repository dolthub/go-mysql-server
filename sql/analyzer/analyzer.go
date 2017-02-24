package analyzer

import (
	"fmt"
	"reflect"

	"gopkg.in/sqle/sqle.v0/sql"
)

const maxAnalysisIterations = 1000

type Analyzer struct {
	Rules           []Rule
	ValidationRules []ValidationRule
	Catalog         *sql.Catalog
	CurrentDatabase string
}

type Rule struct {
	Name  string
	Apply func(*Analyzer, sql.Node) sql.Node
}

type ValidationRule struct {
	Name  string
	Apply func(*Analyzer, sql.Node) error
}

func New(catalog *sql.Catalog) *Analyzer {
	return &Analyzer{
		Rules:           DefaultRules,
		ValidationRules: DefaultValidationRules,
		Catalog:         catalog,
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

	// TODO improve error handling
	if errs := a.validate(cur); len(errs) != 0 {
		return cur, errs[0]
	}

	return cur, nil
}

func (a *Analyzer) analyzeOnce(n sql.Node) sql.Node {
	result := n
	for _, rule := range a.Rules {
		result = rule.Apply(a, result)
	}
	return result
}

func (a *Analyzer) validate(n sql.Node) (validationErrors []error) {
	validationErrors = append(validationErrors, a.validateOnce(n)...)

	for _, node := range n.Children() {
		validationErrors = append(validationErrors, a.validate(node)...)
	}

	return validationErrors
}

func (a *Analyzer) validateOnce(n sql.Node) (validationErrors []error) {
	for _, rule := range a.ValidationRules {
		err := rule.Apply(a, n)
		if err != nil {
			validationErrors = append(validationErrors, err)
		}
	}

	return validationErrors
}
