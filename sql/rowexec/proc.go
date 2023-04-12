package rowexec

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strings"
	"sync"
)

func (b *builder) buildCaseStatement(ctx *sql.Context, n *plan.CaseStatement, row sql.Row) (sql.RowIter, error) {
	caseValue, err := n.Expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	for _, ifConditional := range n.IfElse.IfConditionals {
		whenValue, err := ifConditional.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		comparison, err := n.Expr.Type().Compare(caseValue, whenValue)
		if err != nil {
			return nil, err
		}
		if comparison != 0 {
			continue
		}

		return b.buildCaseIter(ctx, row, ifConditional, ifConditional.Body)
	}

	// All conditions failed so we run the else
	return b.buildCaseIter(ctx, row, n.IfElse.Else, n.IfElse.Else)
}

func (b *builder) buildCaseIter(ctx *sql.Context, row sql.Row, iterNode sql.Node, bodyNode sql.Node) (sql.RowIter, error) {
	// All conditions failed so we run the else
	branchIter, err := b.buildNodeExec(ctx, iterNode, row)
	if err != nil {
		return nil, err
	}
	// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
	// node and schema will be a better representation of the iterated rows.
	if blockRowIter, ok := branchIter.(plan.BlockRowIter); ok {
		return blockRowIter, nil
	}
	return &ifElseIter{
		branchIter: branchIter,
		sch:        bodyNode.Schema(),
		branchNode: bodyNode,
	}, nil
}

func (b *builder) buildIfElseBlock(ctx *sql.Context, n *plan.IfElseBlock, row sql.Row) (sql.RowIter, error) {
	var branchIter sql.RowIter

	var err error
	for _, ifConditional := range n.IfConditionals {
		condition, err := ifConditional.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		var passedCondition bool
		if condition != nil {
			passedCondition, err = types.ConvertToBool(condition)
			if err != nil {
				return nil, err
			}
		}
		if !passedCondition {
			continue
		}

		branchIter, err = b.buildNodeExec(ctx, ifConditional, row)
		if err != nil {
			return nil, err
		}
		// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
		// node and schema will be a better representation of the iterated rows.
		if blockRowIter, ok := branchIter.(*plan.BlockRowIter); ok {
			return blockRowIter, nil
		}
		return &ifElseIter{
			branchIter: branchIter,
			sch:        ifConditional.Body.Schema(),
			branchNode: ifConditional.Body,
		}, nil
	}

	// All conditions failed so we run the else
	branchIter, err = b.buildNodeExec(ctx, n.Else, row)
	if err != nil {
		return nil, err
	}
	// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
	// node and schema will be a better representation of the iterated rows.
	if blockRowIter, ok := branchIter.(*plan.BlockRowIter); ok {
		return blockRowIter, nil
	}
	return &ifElseIter{
		branchIter: branchIter,
		sch:        n.Else.Schema(),
		branchNode: n.Else,
	}, nil
}

func (b *builder) buildBeginEndBlock(ctx *sql.Context, n *plan.BeginEndBlock, row sql.Row) (sql.RowIter, error) {
	n.Pref.PushScope()
	rowIter, err := b.buildNodeExec(ctx, n.Block, row)
	if err != nil {
		if exitErr, ok := err.(expression.ProcedureBlockExitError); ok && n.Pref.CurrentHeight() == int(exitErr) {
			err = nil
		} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(n.Label) {
			if controlFlow.IsExit {
				err = nil
			} else {
				err = fmt.Errorf("encountered ITERATE on BEGIN...END, which should should have been caught by the analyzer")
			}
		}
		if nErr := n.Pref.PopScope(ctx); err == nil && nErr != nil {
			err = nErr
		}
		return sql.RowsToRowIter(), err
	}
	return &beginEndIter{
		BeginEndBlock: n,
		rowIter:       rowIter,
	}, nil
}

func (b *builder) buildIfConditional(ctx *sql.Context, n *plan.IfConditional, row sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n.Body, row)
}

func (b *builder) buildProcedureResolvedTable(ctx *sql.Context, n *plan.ProcedureResolvedTable, row sql.Row) (sql.RowIter, error) {
	rt, err := n.NewestTable(ctx)
	if err != nil {
		return nil, err
	}
	return b.buildResolvedTable(ctx, rt, row)
}

func (b *builder) buildCall(ctx *sql.Context, n *plan.Call, row sql.Row) (sql.RowIter, error) {
	for i, paramExpr := range n.Params {
		val, err := paramExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		paramName := n.Procedure.Params[i].Name
		paramType := n.Procedure.Params[i].Type
		err = n.Pref.InitializeVariable(paramName, paramType, val)
		if err != nil {
			return nil, err
		}
	}
	n.Pref.PushScope()
	innerIter, err := b.buildNodeExec(ctx, n.Procedure, row)
	if err != nil {
		return nil, err
	}
	return &callIter{
		call:      n,
		innerIter: innerIter,
	}, nil
}

func (b *builder) buildLoop(ctx *sql.Context, n *plan.Loop, row sql.Row) (sql.RowIter, error) {
	var blockIter sql.RowIter
	// Currently, acquiring the RowIter will actually run through the loop once, so we abuse this by grabbing the iter
	// only if we're supposed to run through the iter once before evaluating the condition
	if n.OnceBeforeEval {
		var err error
		blockIter, err = b.loopAcquireRowIter(ctx, row, n.Label, n.Block, true)
		if err != nil {
			return nil, err
		}
	}
	iter := &loopIter{
		block:         n.Block,
		label:         strings.ToLower(n.Label),
		condition:     n.Condition,
		once:          sync.Once{},
		blockIter:     blockIter,
		row:           row,
		loopIteration: 0,
	}
	return iter, nil
}

func (b *builder) buildElseCaseError(ctx *sql.Context, n plan.ElseCaseError, row sql.Row) (sql.RowIter, error) {
	return elseCaseErrorIter{}, nil
}

func (b *builder) buildOpen(ctx *sql.Context, n *plan.Open, row sql.Row) (sql.RowIter, error) {
	return &openIter{pRef: n.Pref, name: n.Name, row: row}, nil
}

func (b *builder) buildClose(ctx *sql.Context, n *plan.Close, row sql.Row) (sql.RowIter, error) {
	return &closeIter{pRef: n.Pref, name: n.Name}, nil
}

func (b *builder) buildLeave(ctx *sql.Context, n *plan.Leave, row sql.Row) (sql.RowIter, error) {
	return &leaveIter{n.Label}, nil
}
