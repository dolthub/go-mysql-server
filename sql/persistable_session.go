package sql

import (
	"github.com/dolthub/go-mysql-server/sql/config"
	"sync"
)

type PersistableSession interface {
	Session
	// PersistVariable writes to the system variable defaults file
	PersistVariable(ctx *Context, sysVarName string, value interface{}) error
	// ResetPersistVariable deletes from the system variable defaults file
	ResetPersistVariable(ctx *Context, sysVarName string) error
	// ResetPersistAll empties the contents of the defaults file
	ResetPersistAll(ctx *Context) error
}

type PersistedSession struct {
	Session
	defaultsConf config.ReadWriteConfig
	mu sync.Mutex
}

// NewBaseSession creates a new empty session.
func NewPersistedSession(sess Session, defaults config.ReadWriteConfig) *PersistedSession {
	return &PersistedSession{sess, defaults,sync.Mutex{}}
}

// PersistVariable implements the PersistableSession interface.
func (s *PersistedSession) PersistVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}
	if !sysVar.Dynamic {
		return ErrSystemVariableReadOnly.New(sysVarName)
	}
	convertedVal, err := sysVar.Type.Convert(value)
	if err != nil {
		return err
	}

	//convertedVal, ok := value.(string)
	t, ok := sysVar.Type.(SystemVariableType)
	if !ok {
		return ErrInvalidSystemVariableType.New(convertedVal, sysVar.Type)
	}

	encoded, err := t.EncodeValue(convertedVal)
	if err != nil {
		return err
	}

	//var encoded string = sysVar.Type.Enco
	//switch  {
	//case IsTextOnly(sysVar.Type):
	//	encodedS
	//case IsInteger(sysVar.Type):
	//case IsTrue(sysVar.Type), IsFalse(sysVar.Type):
	//case IsFloat(sysVar.Type):
	//
	//	convertedVal = v
	//case int16:
	//	convertedVal = strconv.FormatInt(int64(v), 10)
	//case bool:
	//	convertedVal = strconv.FormatBool(v)
	//default:
	//	return errors.New(fmt.Sprintf("invalid variable value: %s: %s", value, v))
	//
	//}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultsConf.SetStrings(map[string]string{sysVarName: encoded})
	return nil
}

// ResetPersistVariable implements the PersistableSession interface.
func (s *PersistedSession) ResetPersistVariable(ctx *Context, sysVarName string) error {
	// if sysVarName = "" remove all
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}
	if !sysVar.Dynamic {
		return ErrSystemVariableReadOnly.New(sysVarName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultsConf.Unset([]string{sysVar.Name})
	return nil
}

// ResetPersistAll implements the PersistableSession interface.
func (s *PersistedSession) ResetPersistAll(ctx *Context) error {
	allVars := make([]string, 0, len(systemVars))
	i := 0
	for _, v := range systemVars {
		allVars[i] = v.Name
		i++
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultsConf.Unset(allVars)
	return nil
}