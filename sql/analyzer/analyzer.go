// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

const debugAnalyzerKey = "DEBUG_ANALYZER"

const maxAnalysisIterations = 8

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
	onceBeforeRules     []Rule
	defaultRules        []Rule
	onceAfterRules      []Rule
	validationRules     []Rule
	afterAllRules       []Rule
	catalog             *sql.Catalog
	debug               bool
	parallelism         int
}

// NewBuilder creates a new Builder from a specific catalog.
// This builder allow us add custom Rules and modify some internal properties.
func NewBuilder(c *sql.Catalog) *Builder {
	return &Builder{
		catalog:         c,
		onceBeforeRules: OnceBeforeDefault,
		defaultRules:    DefaultRules,
		onceAfterRules:  OnceAfterDefault,
		validationRules: DefaultValidationRules,
		afterAllRules:   OnceAfterAll,
	}
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

func duplicateRulesWithout(rules []Rule, excludedRuleName string) []Rule {
	newRules := make([]Rule, 0, len(rules))

	for _, rule := range rules {
		if rule.Name != excludedRuleName {
			newRules = append(newRules, rule)
		}
	}

	return newRules
}

// RemoveOnceBeforeRule removes a default rule from the analyzer which would occur before other rules
func (ab *Builder) RemoveOnceBeforeRule(name string) *Builder {
	ab.onceBeforeRules = duplicateRulesWithout(ab.onceBeforeRules, name)

	return ab
}

// RemoveDefaultRule removes a default rule from the analyzer that is executed as part of the analysis
func (ab *Builder) RemoveDefaultRule(name string) *Builder {
	ab.defaultRules = duplicateRulesWithout(ab.defaultRules, name)

	return ab
}

// RemoveOnceAfterRule removes a default rule from the analyzer which would occur just once after the default analysis
func (ab *Builder) RemoveOnceAfterRule(name string) *Builder {
	ab.onceAfterRules = duplicateRulesWithout(ab.onceAfterRules, name)

	return ab
}

// RemoveValidationRule removes a default rule from the analyzer which would occur as part of the validation rules
func (ab *Builder) RemoveValidationRule(name string) *Builder {
	ab.validationRules = duplicateRulesWithout(ab.validationRules, name)

	return ab
}

// RemoveAfterAllRule removes a default rule from the analyzer which would occur after all other rules
func (ab *Builder) RemoveAfterAllRule(name string) *Builder {
	ab.afterAllRules = duplicateRulesWithout(ab.afterAllRules, name)

	return ab
}

func init() {
	logrus.SetFormatter(simpleLogFormatter{})
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
			Rules:      ab.onceBeforeRules,
		},
		&Batch{
			Desc:       "default-rules",
			Iterations: maxAnalysisIterations,
			Rules:      ab.defaultRules,
		},
		&Batch{
			Desc:       "once-after",
			Iterations: 1,
			Rules:      ab.onceAfterRules,
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
			Rules:      ab.validationRules,
		},
		&Batch{
			Desc:       "post-validation",
			Iterations: 1,
			Rules:      ab.postValidationRules,
		},
		&Batch{
			Desc:       "after-all",
			Iterations: 1,
			Rules:      ab.afterAllRules,
		},
	}

	return &Analyzer{
		Debug:          debug || ab.debug,
		contextStack:   make([]string, 0),
		Batches:        batches,
		Catalog:        ab.catalog,
		Parallelism:    ab.parallelism,
		ProcedureCache: NewProcedureCache(),
	}
}

// Analyzer analyzes nodes of the execution plan and applies rules and validations
// to them.
type Analyzer struct {
	// Whether to log various debugging messages
	Debug bool
	// Whether to output the query plan at each step of the analyzer
	Verbose bool
	// A stack of debugger context. See PushDebugContext, PopDebugContext
	contextStack []string
	Parallelism  int
	// Batches of Rules to apply.
	Batches []*Batch
	// Catalog of databases and registered functions.
	Catalog *sql.Catalog
	// ProcedureCache is a cache of stored procedures.
	ProcedureCache *ProcedureCache
}

// NewDefault creates a default Analyzer instance with all default Rules and configuration.
// To add custom rules, the easiest way is use the Builder.
func NewDefault(c *sql.Catalog) *Analyzer {
	return NewBuilder(c).Build()
}

type simpleLogFormatter struct{}

func (s simpleLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	lvl := ""
	switch entry.Level {
	case logrus.PanicLevel:
		lvl = "PANIC"
	case logrus.FatalLevel:
		lvl = "FATAL"
	case logrus.ErrorLevel:
		lvl = "ERROR"
	case logrus.WarnLevel:
		lvl = "WARN"
	case logrus.InfoLevel:
		lvl = "INFO"
	case logrus.DebugLevel:
		lvl = "DEBUG"
	case logrus.TraceLevel:
		lvl = "TRACE"
	}

	msg := fmt.Sprintf("%s: %s\n", lvl, entry.Message)
	return ([]byte)(msg), nil
}

// Log prints an INFO message to stdout with the given message and args
// if the analyzer is in debug mode.
func (a *Analyzer) Log(msg string, args ...interface{}) {
	if a != nil && a.Debug {
		if len(a.contextStack) > 0 {
			ctx := strings.Join(a.contextStack, "/")
			logrus.Infof("%s: "+msg, append([]interface{}{ctx}, args...)...)
		} else {
			logrus.Infof(msg, args...)
		}
	}
}

// LogNode prints the node given if Verbose logging is enabled.
func (a *Analyzer) LogNode(n sql.Node) {
	if a != nil && n != nil && a.Verbose {
		if len(a.contextStack) > 0 {
			ctx := strings.Join(a.contextStack, "/")
			logrus.Infof("%s:\n%s", ctx, sql.DebugString(n))
		} else {
			logrus.Infof("%s", sql.DebugString(n))
		}
	}
}

// LogDiff logs the diff between the query plans after a transformation rules has been applied.
// Only can print a diff when the string representations of the nodes differ, which isn't always the case.
func (a *Analyzer) LogDiff(prev, next sql.Node) {
	if a.Debug && a.Verbose {
		if !reflect.DeepEqual(next, prev) {
			diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(sql.DebugString(prev)),
				B:        difflib.SplitLines(sql.DebugString(next)),
				FromFile: "Prev",
				FromDate: "",
				ToFile:   "Next",
				ToDate:   "",
				Context:  1,
			})
			if err != nil {
				panic(err)
			}
			if len(diff) > 0 {
				a.Log(diff)
			}
		}
	}
}

// PushDebugContext pushes the given context string onto the context stack, to use when logging debug messages.
func (a *Analyzer) PushDebugContext(msg string) {
	if a != nil {
		a.contextStack = append(a.contextStack, msg)
	}
}

// PopDebugContext pops a context message off the context stack.
func (a *Analyzer) PopDebugContext() {
	if a != nil && len(a.contextStack) > 0 {
		a.contextStack = a.contextStack[:len(a.contextStack)-1]
	}
}

func analyzeAll(batchName string) bool {
	return true
}

// Analyze applies the transformation rules to the node given. In the case of an error, the last successfully
// transformed node is returned along with the error.
func (a *Analyzer) Analyze(ctx *sql.Context, n sql.Node, scope *Scope) (sql.Node, error) {
	return a.analyzeWithSelector(ctx, n, scope, analyzeAll)
}

func (a *Analyzer) analyzeThroughBatch(ctx *sql.Context, n sql.Node, scope *Scope, until string) (sql.Node, error) {
	stop := false
	return a.analyzeWithSelector(ctx, n, scope, func(desc string) bool {
		if stop {
			return false
		}
		if desc == until {
			stop = true
		}
		// we return true even for the matching description; only start
		// returning false after this batch.
		return true
	})
}

func (a *Analyzer) analyzeWithSelector(ctx *sql.Context, n sql.Node, scope *Scope, selector func(d string) bool) (sql.Node, error) {
	span, ctx := ctx.Span("analyze", opentracing.Tags{
		//"plan": , n.String(),
	})

	var err error
	a.Log("starting analysis of node of type: %T", n)
	for _, batch := range a.Batches {
		if selector(batch.Desc) {
			a.PushDebugContext(batch.Desc)
			n, err = batch.Eval(ctx, a, n, scope)
			if err != nil {
				a.Log("Encountered error: %v", err)
				a.PopDebugContext()
				return n, err
			}
			a.PopDebugContext()
		}
	}

	defer func() {
		if n != nil {
			span.SetTag("IsResolved", n.Resolved())
		}
		span.Finish()
	}()

	return n, err
}

func (a *Analyzer) analyzeStartingAtBatch(ctx *sql.Context, n sql.Node, scope *Scope, startAt string) (sql.Node, error) {
	start := false
	return a.analyzeWithSelector(ctx, n, scope, func(desc string) bool {
		if desc == startAt {
			start = true
		}
		if start {
			return true
		}
		return false
	})
}
