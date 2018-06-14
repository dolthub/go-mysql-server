package analyzer // import "gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"

import (
	"os"
	"reflect"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const maxAnalysisIterations = 1000

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// Analyzer analyzes nodes of the execution plan and applies rules and validations
// to them.
type Analyzer struct {
	Debug bool
	// Rules to apply.
	Rules []Rule
	// ValidationRules to apply.
	ValidationRules []ValidationRule
	// Catalog of databases and registered functions.
	Catalog *sql.Catalog
	// CurrentDatabase in use.
	CurrentDatabase string
}

// RuleFunc is the function to be applied in a rule.
type RuleFunc func(*sql.Context, *Analyzer, sql.Node) (sql.Node, error)

// ValidationRuleFunc is the function to be used in a validation rule.
type ValidationRuleFunc func(*sql.Context, sql.Node) error

// Rule to transform nodes.
type Rule struct {
	// Name of the rule.
	Name string
	// Apply transforms a node.
	Apply RuleFunc
}

// ValidationRule validates the given nodes.
type ValidationRule struct {
	// Name of the rule.
	Name string
	// Apply validates the given node.
	Apply ValidationRuleFunc
}

const debugAnalyzerKey = "DEBUG_ANALYZER"

// New returns a new Analyzer given a catalog.
func New(catalog *sql.Catalog) *Analyzer {
	_, debug := os.LookupEnv(debugAnalyzerKey)
	return &Analyzer{
		Debug:           debug,
		Rules:           DefaultRules,
		ValidationRules: DefaultValidationRules,
		Catalog:         catalog,
	}
}

// AddRule adds a new rule to the analyzer.
func (a *Analyzer) AddRule(name string, fn RuleFunc) {
	a.Rules = append(a.Rules, Rule{name, fn})
}

// AddValidationRule adds a new validation rule to the analyzer.
func (a *Analyzer) AddValidationRule(name string, fn ValidationRuleFunc) {
	a.ValidationRules = append(a.ValidationRules, ValidationRule{name, fn})
}

// Log prints an INFO message to stdout with the given message and args
// if the analyzer is in debug mode.
func (a *Analyzer) Log(msg string, args ...interface{}) {
	if a != nil && a.Debug {
		logrus.Infof(msg, args...)
	}
}

// Analyze the node and all its children.
func (a *Analyzer) Analyze(ctx *sql.Context, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("analyze")
	span.LogKV("plan", n.String())

	prev := n
	a.Log("starting analysis of node of type: %T", n)
	cur, err := a.analyzeOnce(ctx, n)
	defer func() {
		if cur != nil {
			span.SetTag("IsResolved", cur.Resolved())
		}
		span.Finish()
	}()

	if err != nil {
		return nil, err
	}

	for i := 0; !nodesEqual(prev, cur); {
		a.Log("previous node does not match new node, analyzing again, iteration: %d", i)
		prev = cur
		cur, err = a.analyzeOnce(ctx, cur)
		if err != nil {
			return nil, err
		}

		i++
		if i >= maxAnalysisIterations {
			return cur, ErrMaxAnalysisIters.New(maxAnalysisIterations)
		}
	}

	if errs := a.validate(ctx, cur); len(errs) != 0 {
		for _, e := range errs {
			err = multierror.Append(err, e)
		}
	}

	return cur, err
}

func (a *Analyzer) analyzeOnce(ctx *sql.Context, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("analyze_once")
	span.LogKV("plan", n.String())
	defer span.Finish()

	result := n
	for _, rule := range a.Rules {
		var err error
		result, err = rule.Apply(ctx, a, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (a *Analyzer) validate(ctx *sql.Context, n sql.Node) (validationErrors []error) {
	span, ctx := ctx.Span("validate")
	defer span.Finish()

	validationErrors = append(validationErrors, a.validateOnce(ctx, n)...)

	for _, node := range n.Children() {
		validationErrors = append(validationErrors, a.validate(ctx, node)...)
	}

	return validationErrors
}

func (a *Analyzer) validateOnce(ctx *sql.Context, n sql.Node) (validationErrors []error) {
	span, ctx := ctx.Span("validate_once")
	defer span.Finish()

	for _, rule := range a.ValidationRules {
		err := rule.Apply(ctx, n)
		if err != nil {
			validationErrors = append(validationErrors, err)
		}
	}

	return validationErrors
}

type equaler interface {
	Equal(sql.Node) bool
}

func nodesEqual(a, b sql.Node) bool {
	if e, ok := a.(equaler); ok {
		return e.Equal(b)
	}

	if e, ok := b.(equaler); ok {
		return e.Equal(a)
	}

	return reflect.DeepEqual(a, b)
}
