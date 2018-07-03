package analyzer // import "gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"

import (
	"os"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const debugAnalyzerKey = "DEBUG_ANALYZER"

const maxAnalysisIterations = 1000

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// Builder provides an easy way to generate Analyzer with custom rules and options.
type Builder struct {
	preAnalyzeRules     []Rule
	postAnalyzeRules    []Rule
	preValidationRules  []Rule
	postValidationRules []Rule
	catalog             *sql.Catalog
	debug               bool
}

// NewBuilder creates a new Builder from a specific catalog.
// This builder allow us add custom Rules and modify some internal properties.
func NewBuilder(c *sql.Catalog) *Builder {
	return &Builder{catalog: c}
}

// WithDebug activates debug on the Analyzer.
func (ab *Builder) WithDebug() *Builder {
	ab.debug = true

	return ab
}

// ReadOnly adds a rule that only allows read queries.
func (ab *Builder) ReadOnly() *Builder {
	return ab.AddPreAnalyzeRule(EnsureReadOnlyRule, EnsureReadOnly)
}

// AddPreAnalyzeRule adds a new rule to the analyze before the standard analyzer rules.
func (ab *Builder) AddPreAnalyzeRule(name string, fn RuleFunc) *Builder {
	ab.preAnalyzeRules = append(ab.preAnalyzeRules, Rule{name, fn})

	return ab
}

// AddPostAnalyzeRule adds a new rule to the analyzer after standard analyzer rules.
func (ab *Builder) AddPostAnalyzeRule(name string, fn RuleFunc) *Builder {
	ab.postAnalyzeRules = append(ab.postAnalyzeRules, Rule{name, fn})

	return ab
}

// AddPreValidationRule adds a new rule to the analyzer before standard validation rules.
func (ab *Builder) AddPreValidationRule(name string, fn RuleFunc) *Builder {
	ab.preValidationRules = append(ab.preValidationRules, Rule{name, fn})

	return ab
}

// AddPostValidationRule adds a new rule to the analyzer after standard validation rules.
func (ab *Builder) AddPostValidationRule(name string, fn RuleFunc) *Builder {
	ab.postValidationRules = append(ab.postValidationRules, Rule{name, fn})

	return ab
}

// Build creates a new Analyzer using all previous data setted to the Builder
func (ab *Builder) Build() *Analyzer {
	_, debug := os.LookupEnv(debugAnalyzerKey)
	var batches = []*Batch{
		&Batch{
			Desc:       "pre-analyzer rules",
			Iterations: maxAnalysisIterations,
			Rules:      ab.preAnalyzeRules,
		},
		&Batch{
			Desc:       "analyzer rules",
			Iterations: maxAnalysisIterations,
			Rules:      DefaultRules,
		},
		&Batch{
			Desc:       "post-analyzer rules",
			Iterations: maxAnalysisIterations,
			Rules:      ab.postAnalyzeRules,
		},
		&Batch{
			Desc:       "pre-validation rules",
			Iterations: 1,
			Rules:      ab.preValidationRules,
		},
		&Batch{
			Desc:       "validation rules",
			Iterations: 1,
			Rules:      DefaultValidationRules,
		},
		&Batch{
			Desc:       "post-validation rules",
			Iterations: 1,
			Rules:      ab.postValidationRules,
		},
	}

	return &Analyzer{
		Debug:   debug || ab.debug,
		Batches: batches,
		Catalog: ab.catalog,
	}
}

// Analyzer analyzes nodes of the execution plan and applies rules and validations
// to them.
type Analyzer struct {
	Debug bool
	// Batches of Rules to apply.
	Batches []*Batch
	// Catalog of databases and registered functions.
	Catalog *sql.Catalog
	// CurrentDatabase in use.
	CurrentDatabase string
}

// NewDefault creates a default Analyzer instance with all default Rules and configuration.
// To add custom rules, the easiest way is use the Builder.
func NewDefault(c *sql.Catalog) *Analyzer {
	return NewBuilder(c).Build()
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
	span, ctx := ctx.Span("analyze", opentracing.Tags{
		"plan": n.String(),
	})

	prev := n
	var err error
	a.Log("starting analysis of node of type: %T", n)
	for _, batch := range a.Batches {
		prev, err = batch.Eval(ctx, a, prev)
		if ErrMaxAnalysisIters.Is(err) {
			a.Log(err.Error())
			continue
		}
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if prev != nil {
			span.SetTag("IsResolved", prev.Resolved())
		}
		span.Finish()
	}()

	return prev, err
}

type equaler interface {
	Equal(sql.Node) bool
}
