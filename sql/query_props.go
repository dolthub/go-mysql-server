// Copyright 2024 Dolthub, Inc.
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

const (
	qpNullFlag int = iota
	QPropShowWarnings
	QPropInsert
	QPropUpdate
	QPropDelete
	QPropScalarSubquery
	QPropRelSubquery
	QPropNotExpr
	QPropCount
	QPropCountStar
	QPropAlterTable
	QPropCrossJoin
	QPropSort
	QPropFilter
	QPropAggregation
	QPropSetDatabase
	QPropStar
	QPropInnerJoin
	QPropLimit
	QPropInterval
	QPropMax1Row
)

type QueryProps struct {
	Flags FastIntSet
}

func (qp *QueryProps) Set(flag int) {
	if qp == nil {
		return
	}
	qp.Flags.Add(flag)
}

func (qp *QueryProps) IsSet(flag int) bool {
	return qp.Flags.Contains(flag)
}

var DmlFlags = NewFastIntSet(QPropDelete, QPropUpdate, QPropInsert)

func (qp *QueryProps) DmlIsSet() bool {
	return qp.Flags.Intersects(DmlFlags)
}

var SubqueryFlags = NewFastIntSet(QPropScalarSubquery, QPropRelSubquery)

func (qp *QueryProps) SubqueryIsSet() bool {
	return qp.Flags.Intersects(SubqueryFlags)
}

var JoinFlags = NewFastIntSet(QPropInnerJoin, QPropCrossJoin)

func (qp *QueryProps) JoinIsSet() bool {
	return qp.Flags.Intersects(JoinFlags)
}
