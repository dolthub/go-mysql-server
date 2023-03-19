// Copyright 2021 Dolthub, Inc.
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

package expression

import (
	"fmt"
	"io"
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

// ProcedureReference contains the state for a single CALL statement of a stored procedure.
type ProcedureReference struct {
	innermostScope *procedureScope
	height         int
}
type procedureScope struct {
	parent    *procedureScope
	variables map[string]*procedureVariableReferenceValue
	cursors   map[string]*procedureCursorReferenceValue
	handlers  []*procedureHandlerReferenceValue
}
type procedureVariableReferenceValue struct {
	Name       string
	Value      interface{}
	SqlType    sql.Type
	HasBeenSet bool
}
type procedureCursorReferenceValue struct {
	Name       string
	SelectStmt sql.Node
	RowIter    sql.RowIter
}
type procedureHandlerReferenceValue struct {
	Stmt        sql.Node
	IsExit      bool
	ScopeHeight int
	//TODO: support more than just NOT FOUND
}

// ProcedureReferencable indicates that a sql.Node takes a *ProcedureReference returns a new copy with the reference set.
type ProcedureReferencable interface {
	WithParamReference(pRef *ProcedureReference) sql.Node
}

// InitializeVariable sets the initial value for the variable.
func (ppr *ProcedureReference) InitializeVariable(name string, sqlType sql.Type, val interface{}) error {
	convertedVal, err := sqlType.Convert(val)
	if err != nil {
		return err
	}
	lowerName := strings.ToLower(name)
	ppr.innermostScope.variables[lowerName] = &procedureVariableReferenceValue{
		Name:       lowerName,
		Value:      convertedVal,
		SqlType:    sqlType,
		HasBeenSet: false,
	}
	return nil
}

// InitializeCursor sets the initial state for the cursor.
func (ppr *ProcedureReference) InitializeCursor(name string, selectStmt sql.Node) {
	lowerName := strings.ToLower(name)
	ppr.innermostScope.cursors[lowerName] = &procedureCursorReferenceValue{
		Name:       lowerName,
		SelectStmt: selectStmt,
		RowIter:    nil,
	}
}

// InitializeHandler sets the given handler's statement.
func (ppr *ProcedureReference) InitializeHandler(stmt sql.Node, returnsExitError bool) {
	ppr.innermostScope.handlers = append(ppr.innermostScope.handlers, &procedureHandlerReferenceValue{
		Stmt:        stmt,
		IsExit:      returnsExitError,
		ScopeHeight: ppr.height,
	})
}

// GetVariableValue returns the value of the given parameter.
func (ppr *ProcedureReference) GetVariableValue(name string) (interface{}, error) {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if varRefVal, ok := scope.variables[lowerName]; ok {
			return varRefVal.Value, nil
		}
		scope = scope.parent
	}
	return nil, fmt.Errorf("cannot find value for parameter `%s`", name)
}

// GetVariableType returns the type of the given parameter. Returns the NULL type if the type cannot be found.
func (ppr *ProcedureReference) GetVariableType(name string) sql.Type {
	if ppr == nil {
		return types.Null
	}
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if varRefVal, ok := scope.variables[lowerName]; ok {
			return varRefVal.SqlType
		}
		scope = scope.parent
	}
	return types.Null
}

// SetVariable updates the value of the given parameter.
func (ppr *ProcedureReference) SetVariable(name string, val interface{}, valType sql.Type) error {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if varRefVal, ok := scope.variables[lowerName]; ok {
			//TODO: do some actual type checking using the given value's type
			val, err := varRefVal.SqlType.Convert(val)
			if err != nil {
				return err
			}
			varRefVal.Value = val
			varRefVal.HasBeenSet = true
			return nil
		}
		scope = scope.parent
	}
	return fmt.Errorf("cannot find value for parameter `%s`", name)
}

// VariableHasBeenSet returns whether the parameter has had its value altered from the initial value.
func (ppr *ProcedureReference) VariableHasBeenSet(name string) bool {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if varRefVal, ok := scope.variables[lowerName]; ok {
			return varRefVal.HasBeenSet
		}
		scope = scope.parent
	}
	return false
}

// HandleError handles the given error by passing to a relevant HANDLER, if one has been declared. If no HANDLER has
// been declared, then returns the given error. Otherwise, returns a new error that was created from the HANDLER, or a
// nil error if it was a CONTINUE HANDLER.
func (ppr *ProcedureReference) HandleError(ctx *sql.Context, incomingErr error) error {
	scope := ppr.innermostScope
	for scope != nil {
		for i := len(scope.handlers) - 1; i >= 0; i-- {
			//TODO: handle more than NOT FOUND handlers, handlers should check if the error applies to them first
			originalScope := ppr.innermostScope
			defer func() {
				ppr.innermostScope = originalScope
			}()
			ppr.innermostScope = scope
			handlerRefVal := scope.handlers[i]

			handlerRowIter, err := handlerRefVal.Stmt.RowIter(ctx, nil)
			if err != nil {
				return err
			}
			defer handlerRowIter.Close(ctx)

			for {
				_, err := handlerRowIter.Next(ctx)
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
			if handlerRefVal.IsExit {
				return ProcedureBlockExitError(handlerRefVal.ScopeHeight)
			}
			return io.EOF
		}
		scope = scope.parent
	}
	return incomingErr
}

// OpenCursor sets the designated cursor to open.
func (ppr *ProcedureReference) OpenCursor(ctx *sql.Context, name string, row sql.Row) error {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if cursorRefVal, ok := scope.cursors[lowerName]; ok {
			if cursorRefVal.RowIter != nil {
				return sql.ErrCursorAlreadyOpen.New(name)
			}
			var err error
			cursorRefVal.RowIter, err = cursorRefVal.SelectStmt.RowIter(ctx, row)
			return err
		}
		scope = scope.parent
	}
	return fmt.Errorf("cannot find cursor `%s`", name)
}

// CloseCursor closes the designated cursor.
func (ppr *ProcedureReference) CloseCursor(ctx *sql.Context, name string) error {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if cursorRefVal, ok := scope.cursors[lowerName]; ok {
			if cursorRefVal.RowIter == nil {
				return sql.ErrCursorNotOpen.New(name)
			}
			err := cursorRefVal.RowIter.Close(ctx)
			cursorRefVal.RowIter = nil
			return err
		}
		scope = scope.parent
	}
	return fmt.Errorf("cannot find cursor `%s`", name)
}

// FetchCursor returns the next row from the designated cursor.
func (ppr *ProcedureReference) FetchCursor(ctx *sql.Context, name string) (sql.Row, sql.Schema, error) {
	lowerName := strings.ToLower(name)
	scope := ppr.innermostScope
	for scope != nil {
		if cursorRefVal, ok := scope.cursors[lowerName]; ok {
			if cursorRefVal.RowIter == nil {
				return nil, nil, sql.ErrCursorNotOpen.New(name)
			}
			row, err := cursorRefVal.RowIter.Next(ctx)
			return row, cursorRefVal.SelectStmt.Schema(), err
		}
		scope = scope.parent
	}
	return nil, nil, fmt.Errorf("cannot find cursor `%s`", name)
}

// PushScope creates a new scope inside the current one.
func (ppr *ProcedureReference) PushScope() {
	ppr.innermostScope = &procedureScope{
		parent:    ppr.innermostScope,
		variables: make(map[string]*procedureVariableReferenceValue),
		cursors:   make(map[string]*procedureCursorReferenceValue),
		handlers:  nil,
	}
	ppr.height++
}

// PopScope removes the innermost scope, returning to its parent. Also closes all open cursors.
func (ppr *ProcedureReference) PopScope(ctx *sql.Context) error {
	var err error
	if ppr.innermostScope == nil {
		return fmt.Errorf("attempted to pop an empty scope")
	}
	for _, cursorRefVal := range ppr.innermostScope.cursors {
		if cursorRefVal.RowIter != nil {
			nErr := cursorRefVal.RowIter.Close(ctx)
			cursorRefVal.RowIter = nil
			if err == nil {
				err = nErr
			}
		}
	}
	ppr.innermostScope = ppr.innermostScope.parent
	ppr.height--
	return nil
}

// CloseAllCursors closes all cursors that are still open.
func (ppr *ProcedureReference) CloseAllCursors(ctx *sql.Context) error {
	var err error
	scope := ppr.innermostScope
	for scope != nil {
		for _, cursorRefVal := range scope.cursors {
			if cursorRefVal.RowIter != nil {
				nErr := cursorRefVal.RowIter.Close(ctx)
				cursorRefVal.RowIter = nil
				if err == nil {
					err = nErr
				}
			}
		}
		scope = scope.parent
	}
	return err
}

// CurrentHeight returns the current height of the scope stack.
func (ppr *ProcedureReference) CurrentHeight() int {
	return ppr.height
}

func NewProcedureReference() *ProcedureReference {
	return &ProcedureReference{
		innermostScope: &procedureScope{
			parent:    nil,
			variables: make(map[string]*procedureVariableReferenceValue),
			cursors:   make(map[string]*procedureCursorReferenceValue),
			handlers:  nil,
		},
		height: 0,
	}
}

// ProcedureParam represents the parameter of a stored procedure or stored function.
type ProcedureParam struct {
	name       string
	pRef       *ProcedureReference
	hasBeenSet bool
}

// NewProcedureParam creates a new ProcedureParam expression.
func NewProcedureParam(name string) *ProcedureParam {
	return &ProcedureParam{name: strings.ToLower(name)}
}

// Children implements the sql.Expression interface.
func (*ProcedureParam) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
func (*ProcedureParam) Resolved() bool {
	return true
}

// IsNullable implements the sql.Expression interface.
func (*ProcedureParam) IsNullable() bool {
	return false
}

// Type implements the sql.Expression interface.
func (pp *ProcedureParam) Type() sql.Type {
	return pp.pRef.GetVariableType(pp.name)
}

// Name implements the Nameable interface.
func (pp *ProcedureParam) Name() string {
	return pp.name
}

// String implements the sql.Expression interface.
func (pp *ProcedureParam) String() string {
	return pp.name
}

// Eval implements the sql.Expression interface.
func (pp *ProcedureParam) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	return pp.pRef.GetVariableValue(pp.name)
}

// WithChildren implements the sql.Expression interface.
func (pp *ProcedureParam) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(pp, len(children), 0)
	}
	return pp, nil
}

// WithParamReference returns a new *ProcedureParam containing the given *ProcedureReference.
func (pp *ProcedureParam) WithParamReference(pRef *ProcedureReference) *ProcedureParam {
	npp := *pp
	npp.pRef = pRef
	return &npp
}

// Set sets the value of this procedure parameter to the given value.
func (pp *ProcedureParam) Set(val interface{}, valType sql.Type) error {
	return pp.pRef.SetVariable(pp.name, val, valType)
}

// UnresolvedProcedureParam represents an unresolved parameter of a stored procedure or stored function.
type UnresolvedProcedureParam struct {
	name string
}

// NewUnresolvedProcedureParam creates a new UnresolvedProcedureParam expression.
func NewUnresolvedProcedureParam(name string) *UnresolvedProcedureParam {
	return &UnresolvedProcedureParam{name: strings.ToLower(name)}
}

// Children implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Resolved() bool {
	return false
}

// IsNullable implements the sql.Expression interface.
func (*UnresolvedProcedureParam) IsNullable() bool {
	return false
}

// Type implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Type() sql.Type {
	return types.Null
}

// Name implements the Nameable interface.
func (upp *UnresolvedProcedureParam) Name() string {
	return upp.name
}

// String implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) String() string {
	return upp.name
}

// Eval implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	return nil, fmt.Errorf("attempted to use unresolved procedure param '%s'", upp.name)
}

// WithChildren implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(upp, len(children), 0)
	}
	return upp, nil
}

// ProcedureBlockExitError contains the scope height that should exit.
type ProcedureBlockExitError int

var _ error = ProcedureBlockExitError(0)

// Error implements the error interface.
func (b ProcedureBlockExitError) Error() string {
	return "Block that EXIT handler was declared in could somehow not be found"
}
