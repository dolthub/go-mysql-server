// Copyright 2023 Dolthub, Inc.
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

type Explainable interface {
	GetAnalyzeString() string
	SetExplainStats(stats ExplainStats)
	GetExplainStats() *ExplainStats
}

type ExplainStats struct {
	HasStats           bool
	EstimatedRowCount  uint64
	ActualRowCount     uint64
	NumberOfIterations uint64
	Cost               float64
}

// GetEstimatedRowCount implements Explainable
func (e ExplainStats) GetEstimatedRowCount() uint64 {
	return e.EstimatedRowCount
}

// GetEstimatedCost implements Explainable
func (e ExplainStats) GetEstimatedCost() float64 {
	return e.Cost
}

// GetAnalyzeString implements Explainable
func (e *ExplainStats) GetAnalyzeString() string {
	if !e.HasStats {
		return "(No stats)"
	}
	estimatedStats := fmt.Sprintf("(estimated cost=%v rows=%v)", e.Cost, e.EstimatedRowCount)
	if e.NumberOfIterations == 0 {
		return estimatedStats
	}
	averageRowCount := float64(e.ActualRowCount) / float64(e.NumberOfIterations)
	actualStats := fmt.Sprintf("(actual rows=%v loops=%v)", averageRowCount, e.NumberOfIterations)
	return fmt.Sprintf("%s %s", estimatedStats, actualStats)
}

func (e *ExplainStats) SetExplainStats(newStats ExplainStats) {
	*e = newStats
	e.HasStats = true
}

func (e *ExplainStats) GetExplainStats() *ExplainStats {
	return e
}
