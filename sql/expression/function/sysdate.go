// Copyright 2020-2021 Dolthub, Inc.
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

package function

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// NewSysdate returns a new SYSDATE() function, using the supplied |args| for an
// optional value for fractional second precision. The SYSDATE() function is a synonym
// for NOW(), but does NOT use the query's cached start time, and instead always returns
// the current time, even when executed multiple times in a query or stored procedure.
// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_sysdate
func NewSysdate(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	n, err := NewNow(ctx, args...)
	n.(*Now).alwaysUseExactTime = true
	return n, err
}
