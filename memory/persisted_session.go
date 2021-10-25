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
type MapPersistedSession struct {
	*sql.BaseSession
	persistedGlobals GlobalsMap
}

// NewPersistedSession wraps a session and can write system variables to a defaults config
func NewMapPersistedSession(sess *sql.BaseSession, persistedGlobals GlobalsMap) *MapPersistedSession {
	return &MapPersistedSession{BaseSession: sess, persistedGlobals: persistedGlobals}
}

// PersistGlobal implements the PersistableSession interface.
func (s *MapPersistedSession) PersistGlobal(sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	s.persistedGlobals[sysVarName] = value
	return nil
}

// RemovePersistedGlobal implements the PersistableSession interface.
func (s *MapPersistedSession) RemovePersistedGlobal(sysVarName string) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	delete(s.persistedGlobals, sysVarName)
	return nil
}

// RemoveAllPersistedGlobals implements the PersistableSession interface.
func (s *MapPersistedSession) RemoveAllPersistedGlobals() error {
	s.persistedGlobals = GlobalsMap{}
	return nil
}
