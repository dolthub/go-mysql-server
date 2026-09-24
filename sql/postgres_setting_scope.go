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

import "fmt"

// PostgresSettingScope is an integrator-only SET target for custom PostgreSQL
// parameters. It is never produced by the MySQL parser.
type PostgresSettingScope struct{ Local bool }

func (s PostgresSettingScope) SetValue(ctx *Context, name string, value any) error {
	setter, ok := ctx.Session.(interface {
		SetPostgresSetting(*Context, string, any, bool) error
	})
	if !ok {
		return fmt.Errorf("PostgreSQL settings require a supporting session")
	}
	return setter.SetPostgresSetting(ctx, name, value, s.Local)
}

func (s PostgresSettingScope) GetValue(ctx *Context, name string, _ CollationID) (any, error) {
	reader, ok := ctx.Session.(interface {
		GetPostgresSetting(*Context, string) (any, bool, error)
	})
	if !ok {
		return nil, fmt.Errorf("PostgreSQL settings require a supporting session")
	}
	value, _, err := reader.GetPostgresSetting(ctx, name)
	return value, err
}

func (PostgresSettingScope) IsGlobalOnly() bool  { return false }
func (PostgresSettingScope) IsSessionOnly() bool { return true }
