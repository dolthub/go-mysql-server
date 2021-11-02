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

package memory

import "github.com/dolthub/go-mysql-server/sql"

type GlobalsMap = map[string]interface{}
type InMemoryPersistedSession struct {
	sql.Session
	persistedGlobals GlobalsMap
}

// NewInMemoryPersistedSession is a sql.PersistableSession that writes global variables to an im-memory map
func NewInMemoryPersistedSession(sess sql.Session, persistedGlobals GlobalsMap) *InMemoryPersistedSession {
	return &InMemoryPersistedSession{Session: sess, persistedGlobals: persistedGlobals}
}

// PersistGlobal implements sql.PersistableSession
func (s *InMemoryPersistedSession) PersistGlobal(sysVarName string, value interface{}) error {
	sysVar, _, ok := sql.SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	val, err := sysVar.Type.Convert(value)
	if err != nil {
		return err
	}
	s.persistedGlobals[sysVarName] = val
	return nil
}

// RemovePersistedGlobal implements sql.PersistableSession
func (s *InMemoryPersistedSession) RemovePersistedGlobal(sysVarName string) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	delete(s.persistedGlobals, sysVarName)
	return nil
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *InMemoryPersistedSession) RemoveAllPersistedGlobals() error {
	s.persistedGlobals = GlobalsMap{}
	return nil
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *InMemoryPersistedSession) GetPersistedValue(k string) (interface{}, error) {
	return s.persistedGlobals[k], nil
}
