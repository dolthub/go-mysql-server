// Copyright 2022 Dolthub, Inc.
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

package variables

import (
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type UserVars struct {
	userVars map[string]interface{}
	mu       *sync.RWMutex
}

var _ sql.SessionUserVariables = (*UserVars)(nil)

func NewUserVars() sql.SessionUserVariables {
	return &UserVars{
		userVars: make(map[string]interface{}),
		mu:       &sync.RWMutex{},
	}
}

func (u *UserVars) SetUserVariable(ctx *sql.Context, varName string, value interface{}) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.userVars[strings.ToLower(varName)] = value
	return nil
}

// GetUserVariable implements the Session interface.
func (s *UserVars) GetUserVariable(ctx *sql.Context, varName string) (sql.Type, interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.userVars[strings.ToLower(varName)]
	if !ok {
		return types.Null, nil, nil
	}
	return types.ApproximateTypeFromValue(val), val, nil
}

// TODO: this is a gross hack to prevent a cycle between sql and this package, all to prevent cycles between the types
//  and sql packages.
func init() {
	sql.NewUserVariables = NewUserVars
}