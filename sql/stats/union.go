// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package stats

import "github.com/dolthub/go-mysql-server/sql"

func Union(s1, s2 sql.Statistic) sql.Statistic {
	return s1
}

func Intersect(s1, s2 sql.Statistic) sql.Statistic {
	return s1
}

func PrefixKey(s1 sql.Statistic, key []interface{}, nullable []bool) sql.Statistic {
	idxCols := s1.ColSet()
	var constant sql.ColSet
	var notNull sql.ColSet
	var i sql.ColumnId
	for _, null := range nullable[:len(key)] {
		i, _ = idxCols.Next(i + 1)
		constant.Add(i)
		if !null {
			notNull.Add(i)
		}
	}

	old := s1.FuncDeps()
	new := sql.NewFilterFDs(old, old.NotNull().Union(notNull), old.Constants().Union(constant), nil)
	s1.SetFuncDeps(new)
	return s1
}

func PrefixLt(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func PrefixGt(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func PrefixLte(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func PrefixGte(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func PrefixIsNull(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func PrefixIsNotNull(s1 sql.Statistic, val interface{}) sql.Statistic {
	return s1
}

func McvIndexGt(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}

func McvIndexLt(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}

func McvIndexGte(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}

func McvIndexLte(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}

func McvIndexIsNull(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}

func McvIndexIsNotNull(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return s
}
