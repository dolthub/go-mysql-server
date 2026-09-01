// Copyright 2026 Dolthub, Inc.
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

package sql

import (
	"fmt"
	"strings"
	"sync/atomic"
)

var SystemVariables SystemVariableRegistry

// SystemVariableRegistry is a registry of system variables. Each session gets its own copy of all values via the
// SessionMap() method.
type SystemVariableRegistry interface {
	// AddSystemVariables adds the given system variables to this registry
	AddSystemVariables(sysVars []SystemVariable)
	// AssignValues assigns the given values to the system variables in this registry
	AssignValues(vals map[string]interface{}) error
	// NewSessionMap returns a map of system variables values that can be used by a session
	NewSessionMap() map[string]SystemVarValue
	// GetGlobal returns the current global value of the system variable with the given name
	GetGlobal(name string) (SystemVariable, interface{}, bool)
	// SetGlobal sets the global value of the system variable with the given name
	SetGlobal(ctx *Context, name string, val interface{}) error
	// GetAllGlobalVariables returns a copy of all global variable values.
	GetAllGlobalVariables() map[string]interface{}
}

// SystemVariable is used to system variables.
type SystemVariable interface {
	// GetName returns the name of the sv. Case-sensitive.
	GetName() string
	// GetType returns the type of the sv.
	GetType() Type
	// GetSessionScope returns SESSION scope of the sv.
	GetSessionScope() SystemVariableScope
	// GetLocalScope returns the scope to use when the sv is set with transaction-local scope (Postgres's
	// SET LOCAL), or nil if the sv cannot be set with transaction-local scope.
	GetLocalScope() SystemVariableScope
	// SetDefault sets the default value of the sv.
	SetDefault(any)
	// GetDefault returns the defined default value of the sv.
	// This is used for resetting some variables to initial default/reset value.
	GetDefault() any
	// InitValue sets value without validation.
	// This is used for setting the initial values internally
	// using pre-defined variables or for test-purposes.
	InitValue(ctx *Context, val any, global bool) (SystemVarValue, error)
	// SetValue sets the value of the sv of given scope, global or session
	// It validates setting value of correct scope,
	// converts the given value to appropriate value depending on the sv
	// and it returns the SystemVarValue with the updated value.
	SetValue(ctx *Context, val any, global bool) (SystemVarValue, error)
	// IsReadOnly checks whether the variable is read only.
	// It returns false if variable can be set to a value.
	IsReadOnly() bool
	// IsGlobalOnly checks whether the scope of the variable is global only.
	IsGlobalOnly() bool
	// DisplayString gets 'specified scope' prefix and
	// returns the name with the prefix, if applicable.
	DisplayString(string) string
}

// MysqlSystemVariable represents a mysql system variable.
type MysqlSystemVariable struct {
	// Type defines the type of the system variable. This may be a special type not accessible to standard MySQL operations.
	Type Type
	// Default defines the default value of the system variable.
	Default interface{}
	// Scope defines the scope of the system variable, which is either Global, Session, or Both.
	Scope *MysqlScope
	// NotifyChanged is called by the engine if the value of this variable
	// changes during runtime.  It is typically |nil|, but can be used for
	// system variables which control the behavior of the running server.
	// For example, replication threads might need to be started or stopped
	// when replication is enabled or disabled. This provides a scalable
	// alternative to polling.
	//
	// Calls to NotifyChanged are serialized for a given system variable in
	// the global context and in a particular session. They should never
	// block.  NotifyChanged is not called when a new system variable is
	// registered.
	NotifyChanged func(*Context, SystemVariableScope, SystemVarValue) error
	// ValueFunction defines an optional function that is executed to provide
	// the value of this system variable whenever it is requested. System variables
	// that provide a ValueFunction should also set Dynamic to false, since they
	// cannot be assigned a value and will return a read-only error if tried.
	ValueFunction func() (interface{}, error)
	// Name is the name of the system variable.
	Name string
	// Dynamic defines whether the variable may be written to during runtime. Variables with this set to `false` will
	// return an error if a user attempts to set a value.
	Dynamic bool
	// SetVarHintApplies defines if the variable may be set for a single query using SET_VAR().
	// https://dev.mysql.com/doc/refman/8.0/en/optimizer-hints.html#optimizer-hints-set-var
	SetVarHintApplies bool
}

// GetName implements SystemVariable.
func (m *MysqlSystemVariable) GetName() string {
	return m.Name
}

// GetType implements SystemVariable.
func (m *MysqlSystemVariable) GetType() Type {
	return m.Type
}

// GetSessionScope implements SystemVariable.
func (m *MysqlSystemVariable) GetSessionScope() SystemVariableScope {
	return GetMysqlScope(SystemVariableScope_Session)
}

// GetLocalScope implements SystemVariable
func (m *MysqlSystemVariable) GetLocalScope() SystemVariableScope {
	// in MySQL, LOCAL is a synonym for SESSION
	return m.GetSessionScope()
}

// SetDefault implements SystemVariable.
func (m *MysqlSystemVariable) SetDefault(a any) {
	m.Default = a
}

// GetDefault implements SystemVariable.
func (m *MysqlSystemVariable) GetDefault() any {
	return m.Default
}

// InitValue implements SystemVariable.
func (m *MysqlSystemVariable) InitValue(ctx *Context, val any, global bool) (SystemVarValue, error) {
	convertedVal, _, err := m.Type.Convert(ctx, val)
	if err != nil {
		return SystemVarValue{}, err
	}
	svv := SystemVarValue{
		Var: m,
		Val: convertedVal,
	}
	scope := GetMysqlScope(SystemVariableScope_Session)
	if global {
		scope = GetMysqlScope(SystemVariableScope_Global)
	}
	if m.NotifyChanged != nil {
		err = m.NotifyChanged(ctx, scope, svv)
		if err != nil {
			return SystemVarValue{}, err
		}
	}
	return svv, nil
}

// SetValue implements SystemVariable.
func (m *MysqlSystemVariable) SetValue(ctx *Context, val any, global bool) (SystemVarValue, error) {
	if global && m.Scope.Type == SystemVariableScope_Session {
		return SystemVarValue{}, ErrSystemVariableSessionOnly.New(m.Name)
	}
	if !global && m.Scope.Type == SystemVariableScope_Global {
		return SystemVarValue{}, ErrSystemVariableGlobalOnly.New(m.Name)
	}
	if !m.Dynamic || m.ValueFunction != nil {
		return SystemVarValue{}, ErrSystemVariableReadOnly.New(m.Name)
	}
	return m.InitValue(ctx, val, global)
}

// IsReadOnly implements SystemVariable.
func (m *MysqlSystemVariable) IsReadOnly() bool {
	return !m.Dynamic || m.ValueFunction != nil
}

// IsGlobalOnly implements SystemVariable.
func (m *MysqlSystemVariable) IsGlobalOnly() bool {
	return m.Scope.IsGlobalOnly()
}

// DisplayString implements SystemVariable.
func (m *MysqlSystemVariable) DisplayString(specifiedScope string) string {
	// If the scope wasn't explicitly provided, then don't include it in the string representation
	if specifiedScope == "" {
		return fmt.Sprintf("@@%s", m.Name)
	} else {
		return fmt.Sprintf("@@%s.%s", specifiedScope, m.Name)
	}
}

// SystemVariableScope represents the scope of a system variable
// and handles SV values depending on its scope.
type SystemVariableScope interface {
	// SetValue sets an appropriate value to the given SV name depending on the scope.
	SetValue(*Context, string, any) error
	// GetValue returns appropriate value of the given SV name depending on the scope.
	GetValue(*Context, string, CollationID) (any, error)
	// IsGlobalOnly returns true if SV is of SystemVariableScope_Global scope.
	IsGlobalOnly() bool
	// IsSessionOnly returns true if SV is of SystemVariableScope_Session scope.
	IsSessionOnly() bool
}

// MysqlScope represents the scope of a MySQL system variable.
type MysqlScope struct {
	Type MysqlSVScopeType
}

func GetMysqlScope(t MysqlSVScopeType) *MysqlScope {
	return &MysqlScope{Type: t}
}

func (m *MysqlScope) SetValue(ctx *Context, name string, val any) error {
	switch m.Type {
	case SystemVariableScope_Global:
		err := SystemVariables.SetGlobal(ctx, name, val)
		if err != nil {
			return err
		}
	case SystemVariableScope_Session:
		err := ctx.SetSessionVariable(ctx, name, val)
		if err != nil {
			return err
		}
	case SystemVariableScope_Persist:
		persistSess, ok := ctx.Session.(PersistableSession)
		if !ok {
			return ErrSessionDoesNotSupportPersistence.New()
		}
		err := persistSess.PersistGlobal(ctx, name, val)
		if err != nil {
			return err
		}
		err = SystemVariables.SetGlobal(ctx, name, val)
		if err != nil {
			return err
		}
	case SystemVariableScope_PersistOnly:
		persistSess, ok := ctx.Session.(PersistableSession)
		if !ok {
			return ErrSessionDoesNotSupportPersistence.New()
		}
		err := persistSess.PersistGlobal(ctx, name, val)
		if err != nil {
			return err
		}
	case SystemVariableScope_ResetPersist:
		// TODO: add parser support for RESET PERSIST
		persistSess, ok := ctx.Session.(PersistableSession)
		if !ok {
			return ErrSessionDoesNotSupportPersistence.New()
		}
		if name == "" {
			err := persistSess.RemoveAllPersistedGlobals()
			if err != nil {
				return err
			}
		}
		err := persistSess.RemovePersistedGlobal(name)
		if err != nil {
			return err
		}
	default: // should never be hit
		return fmt.Errorf("unable to set `%s` due to unknown scope `%v`", name, m.Type)
	}
	return nil
}

func (m *MysqlScope) GetValue(ctx *Context, name string, collation CollationID) (any, error) {
	switch m.Type {
	case SystemVariableScope_Global:
		_, val, ok := SystemVariables.GetGlobal(name)
		if !ok {
			return nil, ErrUnknownSystemVariable.New(name)
		}
		return val, nil
	case SystemVariableScope_Session:
		// "character_set_database" and "collation_database" are special system variables, in that they're set whenever
		// the current database is changed. Rather than attempting to synchronize the session variables of all
		// outstanding contexts whenever a database's collation is updated, we just pull the values from the database
		// directly. MySQL also plans to make these system variables immutable (from the user's perspective). This isn't
		// exactly the same as MySQL's behavior, but this is the intent of their behavior, which is also way easier to
		// implement.
		switch strings.ToLower(name) {
		case "character_set_database":
			return collation.CharacterSet().String(), nil
		case "collation_database":
			return collation.String(), nil
		default:
			val, err := ctx.GetSessionVariable(ctx, name)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	default:
		return nil, fmt.Errorf("unknown scope `%v` on system variable `%s`", m.Type, name)
	}
}

func (m *MysqlScope) IsGlobalOnly() bool {
	return m.Type == SystemVariableScope_Global
}

func (m *MysqlScope) IsSessionOnly() bool {
	return m.Type == SystemVariableScope_Session
}

var _ SystemVariableScope = (*MysqlScope)(nil)

// MysqlSVScopeType represents the scope of a system variable.
type MysqlSVScopeType byte

const (
	// SystemVariableScope_Global is set when the system variable exists only in the global context.
	SystemVariableScope_Global MysqlSVScopeType = iota
	// SystemVariableScope_Session is set when the system variable exists only in the session context.
	SystemVariableScope_Session
	// SystemVariableScope_Both is set when the system variable exists in both the global and session contexts.
	SystemVariableScope_Both
	// SystemVariableScope_Persist is set when the system variable is global and persisted.
	SystemVariableScope_Persist
	// SystemVariableScope_PersistOnly is set when the system variable is persisted outside of server context.
	SystemVariableScope_PersistOnly
	// SystemVariableScope_ResetPersist is used to remove a persisted variable
	SystemVariableScope_ResetPersist
)

// String returns the scope as an uppercase string.
func (s MysqlSVScopeType) String() string {
	switch s {
	case SystemVariableScope_Global:
		return "GLOBAL"
	case SystemVariableScope_Session:
		return "SESSION"
	case SystemVariableScope_Persist:
		return "GLOBAL, PERSIST"
	case SystemVariableScope_PersistOnly:
		return "PERSIST"
	case SystemVariableScope_ResetPersist:
		return "RESET PERSIST"
	case SystemVariableScope_Both:
		return "GLOBAL, SESSION"
	default:
		return "UNKNOWN_SYSTEM_SCOPE"
	}
}

type SystemVarValue struct {
	Var SystemVariable
	Val interface{}
}

var StatusVariables StatusVariableRegistry

// StatusVariableRegistry is a registry of status variables.
type StatusVariableRegistry interface {
	// NewSessionMap returns a deep copy of the status variables that are
	// not GlobalOnly scope (i.e. SessionOnly or Both)
	NewSessionMap() map[string]StatusVarValue
	// NewGlobalMap returns a deep copy of the status variables of every scope
	NewGlobalMap() map[string]StatusVarValue
	// GetGlobal returns the current global value of the status variable with the given name
	GetGlobal(name string) (StatusVariable, interface{}, bool)
	// SetGlobal sets the global value of the status variable with the given
	// name, returns an error if the variable is SessionOnly scope
	SetGlobal(name string, val interface{}) error
	// IncrementGlobal increments the value of the status variable by the
	// given integer value. Noop if the variable is session-only scoped.
	IncrementGlobal(name string, val int)
}

// StatusVariableScope represents the scope of a status variable.
type StatusVariableScope byte

const (
	StatusVariableScope_Global StatusVariableScope = iota
	StatusVariableScope_Session
	StatusVariableScope_Both
)

type StatusVariable interface {
	GetName() string
	GetScope() StatusVariableScope
	GetType() Type
	GetDefault() interface{}
}

// MySQLStatusVariable represents a mysql status variable.
type MySQLStatusVariable struct {
	Type    Type
	Default interface{}
	Name    string
	Scope   StatusVariableScope
}

var _ StatusVariable = (*MySQLStatusVariable)(nil)

// GetName implements StatusVariable.
func (m *MySQLStatusVariable) GetName() string {
	return m.Name
}

// GetScope implements StatusVariable.
func (m *MySQLStatusVariable) GetScope() StatusVariableScope {
	return m.Scope
}

// GetType implements StatusVariable.
func (m *MySQLStatusVariable) GetType() Type {
	return m.Type
}

// GetDefault implements StatusVariable.
func (m *MySQLStatusVariable) GetDefault() interface{} {
	return m.Default
}

type StatusVarValue interface {
	Increment(uint64) error
	Set(interface{}) error
	Value() interface{}
	Variable() StatusVariable
	Copy() StatusVarValue
}

// MutableStatusVarValue is a StatusVariable with a value.
type MutableStatusVarValue struct {
	Var StatusVariable
	Val *atomic.Uint64
}

func (s *MutableStatusVarValue) Increment(v uint64) error {
	s.Val.Add(v)
	return nil
}

func (s *MutableStatusVarValue) Set(v interface{}) error {
	typedVal, ok := v.(uint64)
	if !ok {
		return fmt.Errorf("expected uint64")
	}
	s.Val.Store(typedVal)
	return nil
}

func (s *MutableStatusVarValue) Variable() StatusVariable {
	return s.Var
}

func (s *MutableStatusVarValue) Value() interface{} {
	return s.Val.Load()
}

func (s *MutableStatusVarValue) Copy() StatusVarValue {
	ret := *s
	ret.Val = &atomic.Uint64{}
	ret.Val.Add(s.Val.Load())
	return &ret
}

type ImmutableStatusVarValue struct {
	Var StatusVariable
	Val interface{}
}

func (s *ImmutableStatusVarValue) Increment(uint64) error {
	return fmt.Errorf("status variable %s is not a uint64", s.Variable().GetName())
}

func (s *ImmutableStatusVarValue) Set(v interface{}) error {
	s.Val = v
	return nil
}

func (s *ImmutableStatusVarValue) Variable() StatusVariable {
	return s.Var
}

func (s *ImmutableStatusVarValue) Value() interface{} {
	return s.Val
}

// IncrementStatusVariable increments the value of the status variable by integer val.
// |name| is case-sensitive.
func IncrementStatusVariable(ctx *Context, name string, val int) {
	StatusVariables.IncrementGlobal(name, val)
	ctx.Session.IncrementStatusVariable(ctx, name, val)
}
