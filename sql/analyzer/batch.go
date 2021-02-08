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
	"reflect"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
)

// RuleFunc is the function to be applied in a rule.
type RuleFunc func(*sql.Context, *Analyzer, sql.Node, *Scope) (sql.Node, error)

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

// Eval executes the rules of the batch. On any error, the partially transformed node is returned along with the error.
// If the batch's max number of iterations is reached without achieving stabilization (batch evaluation no longer
// changes the node), then this method returns ErrMaxAnalysisIters.
func (b *Batch) Eval(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if b.Iterations == 0 {
		return n, nil
	}

	prev := n
	a.PushDebugContext("0")
	cur, err := b.evalOnce(ctx, a, n, scope)
	a.PopDebugContext()
	if err != nil {
		return cur, err
	}

	if b.Iterations == 1 {
		return cur, nil
	}

	for i := 1; !nodesEqual(prev, cur); {
		a.Log("Nodes not equal, re-running batch")
		if i >= b.Iterations {
			return cur, ErrMaxAnalysisIters.New(b.Iterations)
		}

		prev = cur
		a.PushDebugContext(strconv.Itoa(i))
		cur, err = b.evalOnce(ctx, a, cur, scope)
		a.PopDebugContext()
		if err != nil {
			return cur, err
		}

		i++
	}

	return cur, nil
}

// evalOnce returns the result of evaluating a batch of rules on the node given. In the result of an error, the result
// of the last successful transformation is returned along with the error. If no transformation was successful, the
// input node is returned as-is.
func (b *Batch) evalOnce(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	prev := n
	for _, rule := range b.Rules {
		var err error
		a.Log("Evaluating rule %s", rule.Name)
		a.PushDebugContext(rule.Name)
		next, err := rule.Apply(ctx, a, prev, scope)
		if next != nil {
			a.LogDiff(prev, next)
			prev = next
			a.LogNode(prev)
		}
		a.PopDebugContext()
		if err != nil {
			// Returning the last node before the error is important. This is non-idiomatic, but in the case of partial
			// resolution before an error we want the last successful transformation result. Very important for resolving
			// subqueries.
			return prev, err
		}
	}

	return prev, nil
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

type equaler interface {
	Equal(sql.Node) bool
}
