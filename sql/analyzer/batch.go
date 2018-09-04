package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// RuleFunc is the function to be applied in a rule.
type RuleFunc func(*sql.Context, *Analyzer, sql.Node) (sql.Node, error)

// Rule to transform nodes.
type Rule struct {
	// Name of the rule.
	Name string
	// Apply transforms a node.
	Apply RuleFunc
}

// Batch executes a set of rules a specific number of times.
// When this number of times is reached, the actual node
// and ErrMaxAnalysisIters is returned.
type Batch struct {
	Desc       string
	Iterations int
	Rules      []Rule
}

// Eval executes the actual rules the specified number of times on the Batch.
// If max number of iterations is reached, this method will return the actual
// processed Node and ErrMaxAnalysisIters error.
func (b *Batch) Eval(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if b.Iterations == 0 {
		return n, nil
	}

	prev := n
	cur, err := b.evalOnce(ctx, a, n)
	if err != nil {
		return nil, err
	}

	if b.Iterations == 1 {
		return cur, nil
	}

	for i := 1; !nodesEqual(prev, cur); {
		prev = cur
		cur, err = b.evalOnce(ctx, a, cur)
		if err != nil {
			return nil, err
		}

		i++
		if i >= b.Iterations {
			return cur, ErrMaxAnalysisIters.New(b.Iterations)
		}
	}

	return cur, nil
}

func (b *Batch) evalOnce(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	result := n
	for _, rule := range b.Rules {
		var err error
		result, err = rule.Apply(ctx, a, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
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
