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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

const debugAnalyzerKey = "DEBUG_ANALYZER"

const maxAnalysisIterations = 8

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// ErrInAnalysis is thrown for generic analyzer errors
var ErrInAnalysis = errors.NewKind("error in analysis: %s")

// ErrInvalidNodeType is thrown when the analyzer can't handle a particular kind of node type
var ErrInvalidNodeType = errors.NewKind("%s: invalid node of type: %T")

const disablePrepareStmtKey = "DISABLE_PREPARED_STATEMENTS"

var PreparedStmtDisabled bool

func init() {
	if v := os.Getenv(disablePrepareStmtKey); v != "" {
		PreparedStmtDisabled = true
	}
}

func SetPreparedStmts(v bool) {
	PreparedStmtDisabled = v
}

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
	provider            sql.DatabaseProvider
	debug               bool
	parallelism         int
}

// NewBuilder creates a new Builder from a specific catalog.
// This builder allow us add custom Rules and modify some internal properties.
func NewBuilder(pro sql.DatabaseProvider) *Builder {
	return &Builder{
		provider:        pro,
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
func (ab *Builder) AddPreAnalyzeRule(id RuleId, fn RuleFunc) *Builder {
	ab.preAnalyzeRules = append(ab.preAnalyzeRules, Rule{id, fn})

	return ab
}

// AddPostAnalyzeRule adds a new rule to the analyzer after standard analyzer rules.
func (ab *Builder) AddPostAnalyzeRule(id RuleId, fn RuleFunc) *Builder {
	ab.postAnalyzeRules = append(ab.postAnalyzeRules, Rule{id, fn})

	return ab
}

// AddPreValidationRule adds a new rule to the analyzer before standard validation rules.
func (ab *Builder) AddPreValidationRule(id RuleId, fn RuleFunc) *Builder {
	ab.preValidationRules = append(ab.preValidationRules, Rule{id, fn})

	return ab
}

// AddPostValidationRule adds a new rule to the analyzer after standard validation rules.
func (ab *Builder) AddPostValidationRule(id RuleId, fn RuleFunc) *Builder {
	ab.postValidationRules = append(ab.postValidationRules, Rule{id, fn})

	return ab
}

func duplicateRulesWithout(rules []Rule, excludedRuleId RuleId) []Rule {
	newRules := make([]Rule, 0, len(rules))

	for _, rule := range rules {
		if rule.Id != excludedRuleId {
			newRules = append(newRules, rule)
		}
	}

	return newRules
}

// RemoveOnceBeforeRule removes a default rule from the analyzer which would occur before other rules
func (ab *Builder) RemoveOnceBeforeRule(id RuleId) *Builder {
	ab.onceBeforeRules = duplicateRulesWithout(ab.onceBeforeRules, id)

	return ab
}

// RemoveDefaultRule removes a default rule from the analyzer that is executed as part of the analysis
func (ab *Builder) RemoveDefaultRule(id RuleId) *Builder {
	ab.defaultRules = duplicateRulesWithout(ab.defaultRules, id)

	return ab
}

// RemoveOnceAfterRule removes a default rule from the analyzer which would occur just once after the default analysis
func (ab *Builder) RemoveOnceAfterRule(id RuleId) *Builder {
	ab.onceAfterRules = duplicateRulesWithout(ab.onceAfterRules, id)

	return ab
}

// RemoveValidationRule removes a default rule from the analyzer which would occur as part of the validation rules
func (ab *Builder) RemoveValidationRule(id RuleId) *Builder {
	ab.validationRules = duplicateRulesWithout(ab.validationRules, id)

	return ab
}

// RemoveAfterAllRule removes a default rule from the analyzer which would occur after all other rules
func (ab *Builder) RemoveAfterAllRule(id RuleId) *Builder {
	ab.afterAllRules = duplicateRulesWithout(ab.afterAllRules, id)

	return ab
}

var log = logrus.New()

func init() {
	// TODO: give the option for debug analyzer logging format to match the global one
	log.SetFormatter(simpleLogFormatter{})
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

// Build creates a new Analyzer from the builder parameters
func (ab *Builder) Build() *Analyzer {
	_, debug := os.LookupEnv(debugAnalyzerKey)
	var batches = []*Batch{
		{
			Desc:       "pre-analyzer",
			Iterations: maxAnalysisIterations,
			Rules:      ab.preAnalyzeRules,
		},
		{
			Desc:       "once-before",
			Iterations: 1,
			Rules:      ab.onceBeforeRules,
		},
		{
			Desc:       "default-rules",
			Iterations: maxAnalysisIterations,
			Rules:      ab.defaultRules,
		},
		{
			Desc:       "once-after",
			Iterations: 1,
			Rules:      ab.onceAfterRules,
		},
		{
			Desc:       "post-analyzer",
			Iterations: maxAnalysisIterations,
			Rules:      ab.postAnalyzeRules,
		},
		{
			Desc:       "pre-validation",
			Iterations: 1,
			Rules:      ab.preValidationRules,
		},
		{
			Desc:       "validation",
			Iterations: 1,
			Rules:      ab.validationRules,
		},
		{
			Desc:       "post-validation",
			Iterations: 1,
			Rules:      ab.postValidationRules,
		},
		{
			Desc:       "after-all",
			Iterations: 1,
			Rules:      ab.afterAllRules,
		},
	}

	return &Analyzer{
		Debug:        debug || ab.debug,
		contextStack: make([]string, 0),
		Batches:      batches,
		Catalog:      NewCatalog(ab.provider),
		Parallelism:  ab.parallelism,
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
	Catalog *Catalog
}

// NewDefault creates a default Analyzer instance with all default Rules and configuration.
// To add custom rules, the easiest way is use the Builder.
func NewDefault(provider sql.DatabaseProvider) *Analyzer {
	return NewBuilder(provider).Build()
}

// Log prints an INFO message to stdout with the given message and args
// if the analyzer is in debug mode.
func (a *Analyzer) Log(msg string, args ...interface{}) {
	if a != nil && a.Debug {
		if len(a.contextStack) > 0 {
			ctx := strings.Join(a.contextStack, "/")
			log.Infof("%s: "+msg, append([]interface{}{ctx}, args...)...)
		} else {
			log.Infof(msg, args...)
		}
	}
}

// LogNode prints the node given if Verbose logging is enabled.
func (a *Analyzer) LogNode(n sql.Node) {
	if a != nil && n != nil && a.Verbose {
		if len(a.contextStack) > 0 {
			ctx := strings.Join(a.contextStack, "/")
			log.Infof("%s:\n%s", ctx, sql.DebugString(n))
		} else {
			log.Infof("%s", sql.DebugString(n))
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
			} else {
				a.Log("nodes are different, but no textual diff found (implement better DebugString?)")
			}
		}
	}
}

// PushDebugContext pushes the given context string onto the context stack, to use when logging debug messages.
func (a *Analyzer) PushDebugContext(msg string) {
	if a != nil && a.Debug {
		a.contextStack = append(a.contextStack, msg)
	}
}

// PopDebugContext pops a context message off the context stack.
func (a *Analyzer) PopDebugContext() {
	if a != nil && len(a.contextStack) > 0 {
		a.contextStack = a.contextStack[:len(a.contextStack)-1]
	}
}

func SelectAllBatches(string) bool { return true }

func DefaultRuleSelector(id RuleId) bool {
	switch id {
	// prepared statement rules are incompatible with default rules
	case stripDecorationsId,
		reresolveTablesId,
		resolvePreparedInsertId:
		return false
	}
	return true
}

// Analyze applies the transformation rules to the node given. In the case of an error, the last successfully
// transformed node is returned along with the error.
func (a *Analyzer) Analyze(ctx *sql.Context, n sql.Node, scope *Scope) (sql.Node, error) {
	n, _, err := a.analyzeWithSelector(ctx, n, scope, SelectAllBatches, DefaultRuleSelector)
	return n, err
}

// prePrepareRuleSelector are applied before a prepared statement before bindvars
// are applied
func prePrepareRuleSelector(id RuleId) bool {
	switch id {
	case resolvePreparedInsertId,
		insertTopNId,
		inSubqueryIndexesId,
		AutocommitId,
		TrackProcessId,
		parallelizeId,
		clearWarningsId,
		stripDecorationsId,
		reresolveTablesId,
		validateResolvedId,
		validateOrderById,
		validateGroupById,
		validateSchemaSourceId,
		validateIndexCreationId,
		validateOperandsId,
		validateCaseResultTypesId,
		validateIntervalUsageId,
		validateExplodeUsageId,
		validateSubqueryColumnsId,
		validateUnionSchemasMatchId,
		validateAggregationsId:
		return false
	default:
		return true
	}
}

// PrepareQuery applies a partial set of transformations to a prepared plan.
func (a *Analyzer) PrepareQuery(ctx *sql.Context, n sql.Node, scope *Scope) (sql.Node, error) {
	n, _, err := a.analyzeWithSelector(ctx, n, scope, SelectAllBatches, prePrepareRuleSelector)
	return n, err
}

// prePrepareRuleSelector are applied to a cached prepared statement plan
// after bindvars are applied
func postPrepareRuleSelector(id RuleId) bool {
	switch id {
	case
		// OnceBeforeDefault
		resolveDatabasesId,
		resolveTablesId,
		reresolveTablesId,
		setTargetSchemasId,
		stripDecorationsId,
		parseColumnDefaultsId,

		// DefaultRules
		resolveOrderbyLiteralsId,
		resolveFunctionsId,
		flattenTableAliasesId,
		pushdownSortId,
		pushdownGroupbyAliasesId,
		qualifyColumnsId,
		resolveColumnsId,
		resolveColumnDefaultsId,
		expandStarsId,

		// OnceAfterDefault
		pushdownFiltersId,
		subqueryIndexesId,
		inSubqueryIndexesId,
		resolvePreparedInsertId,

		// DefaultValidationRules

		// OnceAfterAll
		AutocommitId,
		TrackProcessId,
		parallelizeId,
		clearWarningsId:
		return true
	}
	return false
}

// prePrepareRuleSelector are applied to a cached prepared statement plan
// after bindvars are applied
func postPrepareInsertSourceRuleSelector(id RuleId) bool {
	switch id {
	case stripDecorationsId,
		reresolveTablesId,

		expandStarsId,
		resolveFunctionsId,
		flattenTableAliasesId,
		pushdownSortId,
		pushdownGroupbyAliasesId,
		resolveDatabasesId,
		resolveTablesId,

		resolveOrderbyLiteralsId,
		qualifyColumnsId,
		resolveColumnsId,

		pushdownFiltersId,
		subqueryIndexesId,
		inSubqueryIndexesId,
		resolveInsertRowsId,

		AutocommitId,
		TrackProcessId,
		parallelizeId,
		clearWarningsId:
		return true
	}
	return false
}

// AnalyzePrepared runs a partial rule set against a previously analyzed plan.
func (a *Analyzer) AnalyzePrepared(ctx *sql.Context, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	return a.analyzeWithSelector(ctx, n, scope, SelectAllBatches, postPrepareRuleSelector)
}

func (a *Analyzer) analyzeThroughBatch(ctx *sql.Context, n sql.Node, scope *Scope, until string, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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
	}, sel)
}

func (a *Analyzer) analyzeWithSelector(ctx *sql.Context, n sql.Node, scope *Scope, batchSelector BatchSelector, ruleSelector RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("analyze", opentracing.Tags{
		//"plan": , n.String(),
	})

	var (
		same    = transform.SameTree
		allSame = transform.SameTree
		err     error
	)
	a.Log("starting analysis of node of type: %T", n)
	for _, batch := range a.Batches {
		if batchSelector(batch.Desc) {
			a.PushDebugContext(batch.Desc)
			n, same, err = batch.Eval(ctx, a, n, scope, ruleSelector)
			allSame = allSame && same
			if err != nil {
				a.Log("Encountered error: %v", err)
				a.PopDebugContext()
				return n, transform.SameTree, err
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

	return n, allSame, err
}

func (a *Analyzer) analyzeStartingAtBatch(ctx *sql.Context, n sql.Node, scope *Scope, startAt string, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	start := false
	return a.analyzeWithSelector(ctx, n, scope, func(desc string) bool {
		if desc == startAt {
			start = true
		}
		if start {
			return true
		}
		return false
	}, sel)
}

func DeepCopyNode(node sql.Node) (sql.Node, error) {
	n, _, err := transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		e, err := transform.Clone(e)
		return e, transform.NewTree, err
	})
	return n, err
}
