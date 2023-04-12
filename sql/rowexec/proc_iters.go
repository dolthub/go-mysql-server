package rowexec

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"io"
	"strings"
	"sync"
)

// ifElseIter is the row iterator for *IfElseBlock.
type ifElseIter struct {
	branchIter sql.RowIter
	sch        sql.Schema
	branchNode sql.Node
}

var _ plan.BlockRowIter = (*ifElseIter)(nil)

// Next implements the sql.RowIter interface.
func (i *ifElseIter) Next(ctx *sql.Context) (sql.Row, error) {
	return i.branchIter.Next(ctx)
}

// Close implements the sql.RowIter interface.
func (i *ifElseIter) Close(ctx *sql.Context) error {
	return i.branchIter.Close(ctx)
}

// RepresentingNode implements the sql.BlockRowIter interface.
func (i *ifElseIter) RepresentingNode() sql.Node {
	return i.branchNode
}

// Schema implements the sql.BlockRowIter interface.
func (i *ifElseIter) Schema() sql.Schema {
	return i.sch
}

// beginEndIter is the sql.RowIter of *BeginEndBlock.
type beginEndIter struct {
	*plan.BeginEndBlock
	rowIter sql.RowIter
}

var _ sql.RowIter = (*beginEndIter)(nil)

// Next implements the interface sql.RowIter.
func (b *beginEndIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := b.rowIter.Next(ctx)
	if err != nil {
		if exitErr, ok := err.(expression.ProcedureBlockExitError); ok && b.Pref.CurrentHeight() == int(exitErr) {
			err = io.EOF
		} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(b.Label) {
			if controlFlow.IsExit {
				err = nil
			} else {
				err = fmt.Errorf("encountered ITERATE on BEGIN...END, which should should have been caught by the analyzer")
			}
		}
		if nErr := b.Pref.PopScope(ctx); nErr != nil && err == io.EOF {
			err = nErr
		}
		return nil, err
	}
	return row, nil
}

// Close implements the interface sql.RowIter.
func (b *beginEndIter) Close(ctx *sql.Context) error {
	return b.rowIter.Close(ctx)
}

// callIter is the row iterator for *Call.
type callIter struct {
	call      *plan.Call
	innerIter sql.RowIter
}

// Next implements the sql.RowIter interface.
func (iter *callIter) Next(ctx *sql.Context) (sql.Row, error) {
	return iter.innerIter.Next(ctx)
}

// Close implements the sql.RowIter interface.
func (iter *callIter) Close(ctx *sql.Context) error {
	err := iter.innerIter.Close(ctx)
	if err != nil {
		return err
	}
	err = iter.call.Pref.CloseAllCursors(ctx)
	if err != nil {
		return err
	}

	// Set all user and system variables from INOUT and OUT params
	for i, param := range iter.call.Procedure.Params {
		if param.Direction == plan.ProcedureParamDirection_Inout ||
			(param.Direction == plan.ProcedureParamDirection_Out && iter.call.Pref.VariableHasBeenSet(param.Name)) {
			val, err := iter.call.Pref.GetVariableValue(param.Name)
			if err != nil {
				return err
			}

			typ := iter.call.Pref.GetVariableType(param.Name)

			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, val, typ)
				if err != nil {
					return err
				}
			case *expression.SystemVar:
				// This should have been caught by the analyzer, so a major bug exists somewhere
				return fmt.Errorf("unable to set `%s` as it is a system variable", callParam.Name)
			case *expression.ProcedureParam:
				err = callParam.Set(val, param.Type)
				if err != nil {
					return err
				}
			}
		} else if param.Direction == plan.ProcedureParamDirection_Out { // VariableHasBeenSet was false
			// For OUT only, if a var was not set within the procedure body, then we set the vars to nil.
			// If the var had a value before the call then it is basically removed.
			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, nil, iter.call.Pref.GetVariableType(param.Name))
				if err != nil {
					return err
				}
			case *expression.SystemVar:
				// This should have been caught by the analyzer, so a major bug exists somewhere
				return fmt.Errorf("unable to set `%s` as it is a system variable", callParam.Name)
			case *expression.ProcedureParam:
				err := callParam.Set(nil, param.Type)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type elseCaseErrorIter struct{}

var _ sql.RowIter = elseCaseErrorIter{}

// Next implements the interface sql.RowIter.
func (e elseCaseErrorIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, mysql.NewSQLError(1339, "20000", "Case not found for CASE statement")
}

// Close implements the interface sql.RowIter.
func (e elseCaseErrorIter) Close(context *sql.Context) error {
	return nil
}

// openIter is the sql.RowIter of *Open.
type openIter struct {
	pRef *expression.ProcedureReference
	name string
	row  sql.Row
}

var _ sql.RowIter = (*openIter)(nil)

// Next implements the interface sql.RowIter.
func (o *openIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := o.pRef.OpenCursor(ctx, o.name, o.row); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (o *openIter) Close(ctx *sql.Context) error {
	return nil
}

// closeIter is the sql.RowIter of *Close.
type closeIter struct {
	pRef *expression.ProcedureReference
	name string
}

var _ sql.RowIter = (*closeIter)(nil)

// Next implements the interface sql.RowIter.
func (c *closeIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := c.pRef.CloseCursor(ctx, c.name); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (c *closeIter) Close(ctx *sql.Context) error {
	return nil
}

// loopIter is the sql.RowIter of *Loop.
type loopIter struct {
	block         *plan.Block
	label         string
	condition     sql.Expression
	once          sync.Once
	blockIter     sql.RowIter
	row           sql.Row
	loopIteration uint64
}

var _ sql.RowIter = (*loopIter)(nil)

// Next implements the interface sql.RowIter.
func (l *loopIter) Next(ctx *sql.Context) (sql.Row, error) {
	// It's technically valid to make an infinite loop, but we don't want to actually allow that
	const maxIterationCount = 10_000_000_000
	l.loopIteration++
	for ; l.loopIteration < maxIterationCount; l.loopIteration++ {
		// If the condition is false, then we stop evaluation
		condition, err := l.condition.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		conditionBool, err := types.ConvertToBool(condition)
		if err != nil {
			return nil, err
		}
		if !conditionBool {
			return nil, io.EOF
		}

		if l.blockIter == nil {
			var err error
			b := &builder{}
			l.blockIter, err = b.loopAcquireRowIter(ctx, nil, l.label, l.block, false)
			if err != nil {
				return nil, err
			}
		}

		nextRow, err := l.blockIter.Next(ctx)
		if err != nil {
			restart := false
			if err == io.EOF {
				restart = true
			} else if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == l.label {
				if controlFlow.IsExit {
					return nil, io.EOF
				} else {
					restart = true
				}
			}

			if restart {
				err = l.blockIter.Close(ctx)
				if err != nil {
					return nil, err
				}
				l.blockIter = nil
				continue
			}
			return nil, err
		}
		return nextRow, nil
	}
	if l.loopIteration >= maxIterationCount {
		return nil, fmt.Errorf("infinite LOOP detected")
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (l *loopIter) Close(ctx *sql.Context) error {
	if l.blockIter != nil {
		return l.blockIter.Close(ctx)
	}
	return nil
}

// loopError is an error used to control a loop's flow.
type loopError struct {
	Label  string
	IsExit bool
}

var _ error = loopError{}

// Error implements the interface error. As long as the analysis step is implemented correctly, this should never be seen.
func (l loopError) Error() string {
	option := "exited"
	if !l.IsExit {
		option = "continued"
	}
	return fmt.Sprintf("should have %s the loop `%s` but it was somehow not found in the call stack", option, l.Label)
}

// loopAcquireRowIter is a helper function for LOOP that conditionally acquires a new sql.RowIter. If a loop exit is
// encountered, `exitIter` determines whether to return an empty iterator or an io.EOF error.
func (b *builder) loopAcquireRowIter(ctx *sql.Context, row sql.Row, label string, block *plan.Block, exitIter bool) (sql.RowIter, error) {
	blockIter, err := b.buildBlock(ctx, block, row)
	if controlFlow, ok := err.(loopError); ok && strings.ToLower(controlFlow.Label) == strings.ToLower(label) {
		if controlFlow.IsExit {
			if exitIter {
				return sql.RowsToRowIter(), nil
			} else {
				return nil, io.EOF
			}
		} else {
			err = io.EOF
		}
	}
	if err == io.EOF {
		blockIter = sql.RowsToRowIter()
		err = nil
	}
	return blockIter, err
}

// leaveIter is the sql.RowIter of *Leave.
type leaveIter struct {
	Label string
}

var _ sql.RowIter = (*leaveIter)(nil)

// Next implements the interface sql.RowIter.
func (l *leaveIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, loopError{
		Label:  l.Label,
		IsExit: true,
	}
}

// Close implements the interface sql.RowIter.
func (l *leaveIter) Close(ctx *sql.Context) error {
	return nil
}
