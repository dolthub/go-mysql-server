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

package vector

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DistanceType is an alias for sql.DistanceType so that integrators can access it without having to import the `sql`
// package.
type DistanceType = sql.DistanceType

// OrderableDistance is implemented by expressions whose ascending sort order is a vector distance between an indexable
// column expression and a row-independent query vector. The analyzer uses it to replace an ORDER BY ... LIMIT over a
// matching expression with a vector index lookup.
type OrderableDistance interface {
	sql.Expression
	// DistanceMetric returns the distance metric that this expression evaluates.
	DistanceMetric() sql.DistanceType
	// TargetAndQuery returns the column-side expression and the row-independent query-vector expression. `ok` is false
	// when the expression cannot be split that way, such as when neither argument is a row-independent query vector,
	// meaning a vector index cannot serve the ordering.
	TargetAndQuery() (target sql.Expression, query sql.Expression, ok bool)
}

type DistanceL2Squared struct{}

var _ fmt.Stringer = DistanceL2Squared{}
var _ sql.DistanceType = DistanceL2Squared{}

func (d DistanceL2Squared) String() string {
	return "VEC_DISTANCE_L2_SQUARED"
}

func (d DistanceL2Squared) Eval(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}
	var total float64 = 0
	for i, l := range left {
		r := right[i]
		total += float64(l-r) * float64(l-r)
	}
	return total, nil
}

func (d DistanceL2Squared) CanEval(other sql.DistanceType) bool {
	// DistanceEuclidean is a monotone transform of DistanceL2Squared, so both produce the same ordering
	return other == DistanceL2Squared{} || other == DistanceEuclidean{}
}

func (d DistanceL2Squared) FunctionName() string {
	return "vec_distance_l2_squared"
}

func (d DistanceL2Squared) Description() string {
	return "returns the squared l2 norm (euclidian distance) between two vectors"
}

type DistanceEuclidean struct{}

var _ fmt.Stringer = DistanceEuclidean{}
var _ sql.DistanceType = DistanceEuclidean{}

func (d DistanceEuclidean) String() string {
	return "VEC_DISTANCE_EUCLIDEAN"
}

func (d DistanceEuclidean) Eval(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}
	var total float64 = 0
	for i, l := range left {
		r := right[i]
		total += float64(l-r) * float64(l-r)
	}
	return math.Sqrt(total), nil
}

func (d DistanceEuclidean) CanEval(other sql.DistanceType) bool {
	// DistanceL2Squared is a monotone transform of DistanceEuclidean, so both produce the same ordering
	return other == DistanceEuclidean{} || other == DistanceL2Squared{}
}

func (d DistanceEuclidean) FunctionName() string {
	return "vec_distance_euclidean"
}

func (d DistanceEuclidean) Description() string {
	return "returns the euclidean (l2) distance between two vectors"
}

type DistanceCosine struct{}

var _ fmt.Stringer = DistanceCosine{}
var _ sql.DistanceType = DistanceCosine{}

func (d DistanceCosine) String() string {
	return "VEC_DISTANCE_COSINE"
}

func (d DistanceCosine) Eval(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}

	var dotProduct float64 = 0
	var leftMagnitudeSquared float64 = 0
	var rightMagnitudeSquared float64 = 0

	for i, l := range left {
		r := right[i]
		dotProduct += float64(l * r)
		leftMagnitudeSquared += float64(l * l)
		rightMagnitudeSquared += float64(r * r)
	}

	leftMagnitude := math.Sqrt(leftMagnitudeSquared)
	rightMagnitude := math.Sqrt(rightMagnitudeSquared)

	if leftMagnitude == 0 || rightMagnitude == 0 {
		return 0, nil
	}

	// Cosine similarity = dot product / (magnitude1 * magnitude2)
	// Cosine distance = 1 - cosine similarity
	cosineSimilarity := dotProduct / (leftMagnitude * rightMagnitude)
	return 1 - cosineSimilarity, nil
}

func (d DistanceCosine) CanEval(other sql.DistanceType) bool {
	return other == DistanceCosine{}
}

func (d DistanceCosine) FunctionName() string {
	return "vec_distance_cosine"
}

func (d DistanceCosine) Description() string {
	return "returns the cosine distance between two vectors"
}

// DistanceInnerProduct is the negative inner product between two vectors, so that ascending order means most similar first
type DistanceInnerProduct struct{}

var _ sql.DistanceType = DistanceInnerProduct{}

func (d DistanceInnerProduct) String() string {
	return "VEC_DISTANCE_INNER_PRODUCT"
}

func (d DistanceInnerProduct) Eval(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}
	var total float64 = 0
	for i, l := range left {
		r := right[i]
		total -= float64(l) * float64(r)
	}
	return total, nil
}

func (d DistanceInnerProduct) CanEval(other sql.DistanceType) bool {
	return other == DistanceInnerProduct{}
}

func (d DistanceInnerProduct) FunctionName() string {
	return "vec_distance_inner_product"
}

func (d DistanceInnerProduct) Description() string {
	return "returns the negative inner product between two vectors"
}

// DistanceL1 is the L1 (Manhattan / taxicab) distance between two vectors
type DistanceL1 struct{}

var _ sql.DistanceType = DistanceL1{}

func (d DistanceL1) String() string {
	return "VEC_DISTANCE_L1"
}

func (d DistanceL1) Eval(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("attempting to find distance between vectors of different lengths: %d vs %d", len(left), len(right))
	}
	var total float64 = 0
	for i, l := range left {
		r := right[i]
		total += math.Abs(float64(l) - float64(r))
	}
	return total, nil
}

func (d DistanceL1) CanEval(other sql.DistanceType) bool {
	return other == DistanceL1{}
}

func (d DistanceL1) FunctionName() string {
	return "vec_distance_l1"
}

func (d DistanceL1) Description() string {
	return "returns the l1 (manhattan) distance between two vectors"
}

type Distance struct {
	DistanceType sql.DistanceType
	expression.BinaryExpressionStub
}

func (d Distance) FunctionName() string {
	return d.DistanceType.FunctionName()
}

func (d Distance) Description() string {
	return d.DistanceType.Description()
}

var _ sql.Expression = (*Distance)(nil)
var _ sql.FunctionExpression = (*Distance)(nil)
var _ sql.CollationCoercible = (*Distance)(nil)

// NewDistance creates a new Distance expression.
func NewDistance(ctx *sql.Context, distanceType sql.DistanceType, left sql.Expression, right sql.Expression) sql.Expression {
	return &Distance{DistanceType: distanceType, BinaryExpressionStub: expression.BinaryExpressionStub{LeftChild: left, RightChild: right}}
}

var _ sql.CreateFunc2Args = NewL2SquaredDistance

func NewL2SquaredDistance(ctx *sql.Context, left, right sql.Expression) sql.Expression {
	return NewDistance(ctx, DistanceL2Squared{}, left, right)
}

var _ sql.CreateFunc2Args = NewEuclideanDistance

func NewEuclideanDistance(ctx *sql.Context, left, right sql.Expression) sql.Expression {
	return NewDistance(ctx, DistanceEuclidean{}, left, right)
}

var _ sql.CreateFunc2Args = NewCosineDistance

func NewCosineDistance(ctx *sql.Context, left, right sql.Expression) sql.Expression {
	return NewDistance(ctx, DistanceCosine{}, left, right)
}

var _ sql.CreateFunc2Args = NewInnerProductDistance

func NewInnerProductDistance(ctx *sql.Context, left, right sql.Expression) sql.Expression {
	return NewDistance(ctx, DistanceInnerProduct{}, left, right)
}

var _ sql.CreateFunc2Args = NewL1Distance

func NewL1Distance(ctx *sql.Context, left, right sql.Expression) sql.Expression {
	return NewDistance(ctx, DistanceL1{}, left, right)
}

func (d Distance) CollationCoercibility(_ *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (d Distance) String() string {
	return fmt.Sprintf("%s(%s, %s)", d.DistanceType, d.LeftChild, d.RightChild)
}

func (d Distance) Type(ctx *sql.Context) sql.Type {
	return types.Float64
}

func (d Distance) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewDistance(ctx, d.DistanceType, children[0], children[1]), nil
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

	return MeasureDistance(ctx, lval, rval, d.DistanceType)
}

var _ OrderableDistance = (*Distance)(nil)

// DistanceMetric implements the interface OrderableDistance.
func (d Distance) DistanceMetric() sql.DistanceType {
	return d.DistanceType
}

// TargetAndQuery implements the interface OrderableDistance. The query vector must be a constant
// value that does not depend on the row.
func (d Distance) TargetAndQuery() (sql.Expression, sql.Expression, bool) {
	isConstantQueryVector := func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.Literal:
			// A NULL query vector has no meaningful nearest-neighbor ordering
			return e.Value() != nil
		case *expression.UserVar:
			return true
		}
		return false
	}
	if isConstantQueryVector(d.LeftChild) {
		return d.RightChild, d.LeftChild, true
	}
	if isConstantQueryVector(d.RightChild) {
		return d.LeftChild, d.RightChild, true
	}
	return nil, nil, false
}

func MeasureDistance(ctx context.Context, left, right interface{}, distanceType sql.DistanceType) (interface{}, error) {
	leftVec, err := sql.ConvertToVector(ctx, left)
	if err != nil {
		return nil, err
	}
	if leftVec == nil {
		return nil, nil
	}
	rightVec, err := sql.ConvertToVector(ctx, right)
	if err != nil {
		return nil, err
	}
	if rightVec == nil {
		return nil, nil
	}

	return distanceType.Eval(leftVec, rightVec)
}

// GenericDistance is the DISTANCE function that takes a parameter to determine the distance metric
type GenericDistance struct {
	expression.NaryExpression
}

var _ sql.Expression = (*GenericDistance)(nil)
var _ sql.FunctionExpression = (*GenericDistance)(nil)
var _ sql.CollationCoercible = (*GenericDistance)(nil)

func NewGenericDistance(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("DISTANCE", "3", len(args))
	}
	return &GenericDistance{NaryExpression: expression.NaryExpression{ChildExpressions: args}}, nil
}

func (g *GenericDistance) FunctionName() string {
	return "distance"
}

func (g *GenericDistance) Description() string {
	return "returns the distance between two vectors using the specified metric (EUCLIDEAN or COSINE)"
}

func (g *GenericDistance) Type(ctx *sql.Context) sql.Type {
	return types.Float64
}

func (g *GenericDistance) CollationCoercibility(_ *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (g *GenericDistance) String() string {
	children := g.Children()
	return fmt.Sprintf("DISTANCE(%s, %s, %s)", children[0], children[1], children[2])
}

func (g *GenericDistance) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 3)
	}
	newDist, err := NewGenericDistance(ctx, children...)
	return newDist, err
}

func (g *GenericDistance) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	children := g.Children()

	lval, err := children[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if lval == nil {
		return nil, nil
	}

	rval, err := children[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if rval == nil {
		return nil, nil
	}

	metricVal, err := children[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if metricVal == nil {
		return nil, fmt.Errorf(`DISTANCE must be "EUCLIDEAN", "L2_SQUARED", or "COSINE", got NULL`)
	}

	metricStr, ok := metricVal.(string)
	if !ok {
		return nil, fmt.Errorf(`DISTANCE must be "EUCLIDEAN", "L2_SQUARED", or "COSINE", got %T`, metricVal)
	}

	var distanceType sql.DistanceType
	switch strings.ToUpper(metricStr) {
	case "EUCLIDEAN":
		distanceType = DistanceEuclidean{}
	case "COSINE":
		distanceType = DistanceCosine{}
	case "L2_SQUARED":
		distanceType = DistanceL2Squared{}
	default:
		return nil, fmt.Errorf(`DISTANCE must be "EUCLIDEAN", "L2_SQUARED", or "COSINE", got %s`, metricStr)
	}

	return MeasureDistance(ctx, lval, rval, distanceType)
}
