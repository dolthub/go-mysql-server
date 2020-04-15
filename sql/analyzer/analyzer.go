package analyzer

import (
	"fmt"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
	"os"
	"strings"
)

const debugAnalyzerKey = "DEBUG_ANALYZER"

const maxAnalysisIterations = 1000

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// ErrInAnalysis is thrown for generic analyzer errors
var ErrInAnalysis = errors.NewKind("error in analysis: %s")

// ErrInvalidNodeType is thrown when the analyzer can't handle a particular kind of node type
var ErrInvalidNodeType = errors.NewKind("%s: invalid node of type: %T")

// Builder provides an easy way to generate Analyzer with custom rules and options.
type Builder struct {
	preAnalyzeRules     []Rule
	postAnalyzeRules    []Rule
	preValidationRules  []Rule
	postValidationRules []Rule
	catalog             *sql.Catalog
	debug               bool
	parallelism         int
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

// WithParallelism sets the parallelism level on the analyzer.
func (ab *Builder) WithParallelism(parallelism int) *Builder {
	ab.parallelism = parallelism
	return ab
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
			Desc:       "pre-analyzer",
			Iterations: maxAnalysisIterations,
			Rules:      ab.preAnalyzeRules,
		},
		&Batch{
			Desc:       "once-before",
			Iterations: 1,
			Rules:      OnceBeforeDefault,
		},
		&Batch{
			Desc:       "default-rules",
			Iterations: maxAnalysisIterations,
			Rules:      DefaultRules,
		},
		&Batch{
			Desc:       "once-after",
			Iterations: 1,
			Rules:      OnceAfterDefault,
		},
		&Batch{
			Desc:       "post-analyzer",
			Iterations: maxAnalysisIterations,
			Rules:      ab.postAnalyzeRules,
		},
		&Batch{
			Desc:       "pre-validation",
			Iterations: 1,
			Rules:      ab.preValidationRules,
		},
		&Batch{
			Desc:       "validation",
			Iterations: 1,
			Rules:      DefaultValidationRules,
		},
		&Batch{
			Desc:       "post-validation",
			Iterations: 1,
			Rules:      ab.postValidationRules,
		},
		&Batch{
			Desc:       "after-all",
			Iterations: 1,
			Rules:      OnceAfterAll,
		},
	}

	return &Analyzer{
		Debug:       debug || ab.debug,
		debugCtx:    make([]string, 0),
		Batches:     batches,
		Catalog:     ab.catalog,
		Parallelism: ab.parallelism,
	}
}

// Analyzer analyzes nodes of the execution plan and applies rules and validations
// to them.
type Analyzer struct {
	// Whether to log various debugging messages
	Debug       bool
	// Whether to output the query plan at each step of the analyzer
	Verbose     bool
	debugCtx    []string
	Parallelism int
	// Batches of Rules to apply.
	Batches []*Batch
	// Catalog of databases and registered functions.
	Catalog *sql.Catalog
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
		if len(a.debugCtx) > 0 {
			ctx := strings.Join(a.debugCtx, "/")
			logrus.Infof("%s: "+msg, append([]interface{}{ctx}, args...)...)
		} else {
			logrus.Infof(msg, args...)
		}
	}
}

// LogNode prints the node given if Verbose logging is enabled.
func (a *Analyzer) LogNode(n sql.Node) {
	if a != nil && n != nil && a.Verbose {
		if len(a.debugCtx) > 0 {
			ctx := strings.Join(a.debugCtx, "/")
			fmt.Printf("%s: %s", ctx, n.String())
		} else {
			fmt.Printf("%s", n.String())
		}
	}
}

// PushDebugContext pushes the given context string onto the context stack, to use when logging debug messages.
func (a *Analyzer) PushDebugContext(msg string) {
	if a != nil {
		a.debugCtx = append(a.debugCtx, msg)
	}
}

// PopDebugContext pops a context message off the context stack.
func (a *Analyzer) PopDebugContext() {
	if a != nil && len(a.debugCtx) > 0 {
		a.debugCtx = a.debugCtx[:len(a.debugCtx)-1]
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
		a.PushDebugContext(batch.Desc)
		prev, err = batch.Eval(ctx, a, prev)
		a.PopDebugContext()
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
