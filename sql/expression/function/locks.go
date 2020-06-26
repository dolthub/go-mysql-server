package function

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"gopkg.in/src-d/go-errors.v1"
	"strings"
	"time"
)

/*
GET_LOCK(str,timeout)	Get a named lock
IS_FREE_LOCK(str)	Whether the named lock is free
IS_USED_LOCK(str)	Whether the named lock is in use; return connection identifier if true
RELEASE_ALL_LOCKS()	Release all current named locks
RELEASE_LOCK(str)	Release the named lock
*/

var ErrIllegalLockNameArgType = errors.NewKind("Illegal parameter data type %s for operation '%s'")

func ReleaseAllLocksForLS(ls *sql.LockSubsystem) sql.EvalLogic {
	return func(ctx *sql.Context, _ sql.Row) (interface{}, error) {
		return ls.ReleaseAll(ctx)
	}
}

type NamedLockFuncLogic func(ctx *sql.Context, ls *sql.LockSubsystem, lockName string) (interface{}, error)

type NamedLockFunction struct {
	expression.UnaryExpression
	ls *sql.LockSubsystem
	funcName string
	retType sql.Type
	logic NamedLockFuncLogic
}

func NewNamedLockFunc(ls *sql.LockSubsystem, funcName string, retType sql.Type, logic NamedLockFuncLogic) sql.Function1 {
	fn := func(e sql.Expression) sql.Expression {
		return &NamedLockFunction{expression.UnaryExpression{Child: e}, ls, funcName, retType, logic}
	}

	return sql.Function1{Name: funcName, Fn: fn}
}

// Eval implements the Expression interface.
func (nl *NamedLockFunction) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if nl.Child == nil {
		return nil, nil
	}

	val, err := nl.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	lockName, ok := val.(string)

	if !ok {
		return nil, ErrIllegalLockNameArgType.New(nl.Child.Type().String(), nl.funcName)
	}

	return nl.logic(ctx, nl.ls, lockName)
}

// String implements the Stringer interface.
func (nl *NamedLockFunction) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(nl.funcName), nl.Child.String())
}

// IsNullable implements the Expression interface.
func (nl *NamedLockFunction) IsNullable() bool {
	return nl.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (nl *NamedLockFunction) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(nl, len(children), 1)
	}

	return &NamedLockFunction{expression.UnaryExpression{Child:children[0]}, nl.ls, nl.funcName, nl.retType, nl.logic}, nil
}

// Type implements the Expression interface.
func (nl *NamedLockFunction) Type() sql.Type {
	return nl.retType
}


func ReleaseLockFunc(ctx *sql.Context, ls *sql.LockSubsystem, lockName string) (interface{}, error) {
	err := ls.Unlock(ctx, lockName)

	if err != nil {
		if sql.ErrLockDoesNotExist.Is(err) {
			return nil, nil
		} else if sql.ErrLockNotOwned.Is(err) {
			return int8(0), nil
		}

		return nil, err
	}

	return int8(1), nil
}

func IsFreeLockFunc(_ *sql.Context, ls *sql.LockSubsystem, lockName string) (interface{}, error) {
	state, _ := ls.GetLockState(lockName)

	switch state {
	case sql.LockInUse:
		return int8(0), nil
	default: // return 1 if the lock is free.  If the lock doesn't exist yet it is free
		return int8(1), nil
	}
}

func IsUsedLockFunc(ctx *sql.Context, ls *sql.LockSubsystem, lockName string) (interface{}, error) {
	state, owner := ls.GetLockState(lockName)

	switch state {
	case sql.LockInUse:
		return owner, nil
	default:
		return nil, nil
	}
}

type GetLock struct {
	expression.BinaryExpression
	ls *sql.LockSubsystem
}

func CreateNewGetLock(ls *sql.LockSubsystem) func(e1, e2 sql.Expression) sql.Expression {
	return func(e1, e2 sql.Expression) sql.Expression {
		return &GetLock{expression.BinaryExpression{e1, e2}, ls}
	}
}

// Eval implements the Expression interface.
func (gl *GetLock) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if gl.Left == nil {
		return nil, nil
	}

	val0, err := gl.Left.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val0 == nil {
		return nil, nil
	}

	if gl.Right == nil {
		return nil, nil
	}

	val1, err := gl.Right.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val1 == nil {
		return nil, nil
	}

	lockName, ok := val0.(string)

	if !ok {
		return nil, ErrIllegalLockNameArgType.New(gl.Left.Type().String(), "get_lock")
	}

	timeout, err := sql.Int64.Convert(val1)

	if err != nil {
		return nil, fmt.Errorf("illegal value for timeeout %v", timeout)
	}

	err = gl.ls.Lock(ctx, lockName, time.Second * time.Duration(timeout.(int64)))

	if err != nil {
		if sql.ErrLockTimeout.Is(err) {
			return int8(0), nil
		}

		return nil, err
	}

	return int8(1), nil
}

// String implements the Stringer interface.
func (gl *GetLock) String() string {
	return fmt.Sprintf("get_lock(%s, %s)", gl.Left.String(), gl.Right.String())
}

// IsNullable implements the Expression interface.
func (gl *GetLock) IsNullable() bool {
	return false
}

// WithChildren implements the Expression interface.
func (gl *GetLock) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(gl, len(children), 1)
	}

	return &GetLock{expression.BinaryExpression{Left: children[0], Right: children[1]}, gl.ls}, nil
}

// Type implements the Expression interface.
func (gl *GetLock) Type() sql.Type {
	return sql.Int8
}
