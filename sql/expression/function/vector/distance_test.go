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

package vector

import (
	"math"
	"testing"

	assert "github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/types/jsontests"
)

func jsonExpression(t *testing.T, val interface{}) sql.Expression {
	return expression.NewLiteral(jsontests.ConvertToJson(t, val), types.JSON)
}

func TestDistance(t *testing.T) {
	ctx := sql.NewEmptyContext()
	distance := NewDistance(ctx, DistanceL2Squared{}, jsonExpression(t, "[0.0, 0.0]"), jsonExpression(t, "[3.0, 4.0]"))
	result, err := distance.Eval(ctx, nil)
	assert.NoError(t, err)
	assert.InEpsilon(t, 25.0, result, 0.1)
}

func TestDistanceMetrics(t *testing.T) {
	tests := []struct {
		distanceType sql.DistanceType
		left         string
		right        string
		expected     float64
	}{
		{DistanceL2Squared{}, "[0.0, 0.0]", "[3.0, 4.0]", 25.0},
		{DistanceEuclidean{}, "[0.0, 0.0]", "[3.0, 4.0]", 5.0},
		{DistanceCosine{}, "[1.0, 0.0]", "[0.0, 1.0]", 1.0},
		{DistanceCosine{}, "[1.0, 2.0]", "[2.0, 4.0]", 0.0},
		{DistanceCosine{}, "[1.0, 0.0]", "[-1.0, 0.0]", 2.0},
		{DistanceCosine{}, "[1.0, 0.0]", "[1.0, 1.0]", 1.0 - math.Sqrt(2)/2},
		{DistanceInnerProduct{}, "[1.0, 2.0]", "[3.0, 4.0]", -11.0},
		{DistanceInnerProduct{}, "[-1.0, 2.0]", "[3.0, 1.0]", 1.0},
		{DistanceL1{}, "[1.0, 2.0]", "[3.0, 5.0]", 5.0},
		{DistanceL1{}, "[3.0, 5.0]", "[1.0, 2.0]", 5.0},
	}
	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		t.Run(test.distanceType.String(), func(t *testing.T) {
			distance := NewDistance(ctx, test.distanceType, jsonExpression(t, test.left), jsonExpression(t, test.right))
			result, err := distance.Eval(ctx, nil)
			assert.NoError(t, err)
			assert.InDelta(t, test.expected, result, 0.0001)
		})
	}
}

func TestCanEval(t *testing.T) {
	metrics := []sql.DistanceType{DistanceL2Squared{}, DistanceEuclidean{}, DistanceCosine{}, DistanceInnerProduct{}, DistanceL1{}}
	// DistanceL2Squared and DistanceEuclidean produce the same ordering, so an index built on
	// either can evaluate the other. All other metrics only evaluate themselves.
	for _, indexMetric := range metrics {
		for _, exprMetric := range metrics {
			expected := indexMetric == exprMetric ||
				(indexMetric == DistanceL2Squared{} && exprMetric == DistanceEuclidean{}) ||
				(indexMetric == DistanceEuclidean{} && exprMetric == DistanceL2Squared{})
			assert.Equal(t, expected, indexMetric.CanEval(exprMetric), "%s.CanEval(%s)", indexMetric, exprMetric)
		}
	}
}
