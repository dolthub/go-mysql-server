// Copyright 2024 Dolthub, Inc.
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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type DistanceType interface {
	String() string
	Eval(left []float64, right []float64) (float64, error)
	CanEval(distanceType DistanceType) bool
}

type DistanceL2Squared struct{}

var _ fmt.Stringer = DistanceL2Squared{}
var _ DistanceType = DistanceL2Squared{}

func (d DistanceL2Squared) String() string {
	return "VEC_DISTANCE_L2_SQUARED"
}

func (d DistanceL2Squared) Eval(left []float64, right []float64) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}
	var total float64 = 0
	for i, l := range left {
		r := right[i]
		total += (l - r) * (l - r)
	}
	return total, nil
}

func (d DistanceL2Squared) CanEval(other DistanceType) bool {
	return other == DistanceL2Squared{}
}

type Distance struct {
	DistanceType DistanceType
	BinaryExpressionStub
}

var _ sql.Expression = (*Distance)(nil)
var _ sql.CollationCoercible = (*Distance)(nil)

// NewDistance creates a new Distance expression.
func NewDistance(distanceType DistanceType, left sql.Expression, right sql.Expression) sql.Expression {
	return &Distance{DistanceType: distanceType, BinaryExpressionStub: BinaryExpressionStub{LeftChild: left, RightChild: right}}
}

func (d Distance) CollationCoercibility(_ *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (d Distance) String() string {
	return fmt.Sprintf("%s(%s, %s)", d.DistanceType, d.LeftChild, d.RightChild)
}

func (d Distance) Type() sql.Type {
	return types.Float64
}

func (d Distance) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewDistance(d.DistanceType, children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (d Distance) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, err := d.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if lval == nil {
		return nil, nil
	}
	rval, err := d.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if rval == nil {
		return nil, nil
	}

	return MeasureDistance(lval, rval, d.DistanceType)
}

func MeasureDistance(left, right interface{}, distanceType DistanceType) (interface{}, error) {
	leftVec, err := sql.ConvertToVector(left)
	if err != nil {
		return nil, err
	}
	if leftVec == nil {
		return nil, nil
	}
	rightVec, err := sql.ConvertToVector(right)
	if err != nil {
		return nil, err
	}
	if rightVec == nil {
		return nil, nil
	}

	return distanceType.Eval(leftVec, rightVec)
}
