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

package memory

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/go-mysql-server/sql"
)

func NewStatsProv() *StatsProv {
	return &StatsProv{
		colStats: make(map[statsKey]sql.Statistic),
	}
}

type statsKey string

type StatsProv struct {
	colStats map[statsKey]sql.Statistic
}

var _ sql.StatsProvider = (*StatsProv)(nil)

func (s *StatsProv) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	// non-Dolt would sample the table to get estimate of unique and histogram
	iat, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return err
	}

	ordinals := make(map[string]int)
	for i, c := range table.Schema() {
		ordinals[strings.ToLower(c.Name)] = i
	}

	newStats := make(map[statsKey][]int)
	tablePrefix := fmt.Sprintf("%s.", strings.ToLower(table.Name()))
	for _, idx := range indexes {
		cols := make([]string, len(idx.Expressions()))
		for i, c := range idx.Expressions() {
			cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
		}
		for i := 1; i < len(cols)+1; i++ {
			pref := cols[:i]
			key := statsKey(fmt.Sprintf("%s.%s.%s.(%s)", strings.ToLower(db), strings.ToLower(idx.Table()), strings.ToLower(idx.ID()), strings.Join(pref, ",")))
			if _, ok := newStats[key]; !ok {
				ords := make([]int, len(pref))
				for i, c := range pref {
					ords[i] = ordinals[c]
				}
				newStats[key] = ords
			}
		}
	}
	return s.estimateStats(ctx, table, newStats)
}

func (s *StatsProv) estimateStats(ctx *sql.Context, table sql.Table, keys map[statsKey][]int) error {
	sample, err := s.reservoirSample(ctx, table)
	if err != nil {
		return err
	}

	var dataLen uint64
	var rowCount uint64
	if statsTab, ok := table.(sql.StatisticsTable); ok {
		rowCount, _, err = statsTab.RowCount(ctx)
		if err != nil {
			return err
		}
		dataLen, err = statsTab.DataLength(ctx)
		if err != nil {
			return err
		}
	}

	sch := table.Schema()
	for key, ordinals := range keys {
		keyVals := make([]sql.Row, len(sample))
		for i, row := range sample {
			for _, ord := range ordinals {
				keyVals[i] = append(keyVals[i], row[ord])
			}
		}
		sort.Slice(keyVals, func(i, j int) bool {
			k := 0
			for k < len(ordinals) && keyVals[i][k] == keyVals[j][k] {
				k++
			}
			if k == len(ordinals) {
				return true
			}
			col := sch[ordinals[k]]
			cmp, _ := col.Type.Compare(keyVals[i][k], keyVals[j][k])
			return cmp <= 0
		})

		// quick and dirty histogram buckets
		bucketCnt := 10
		if len(keyVals) < bucketCnt {
			bucketCnt = len(keyVals)
		}
		offset := len(keyVals) / bucketCnt
		buckets := make([]*stats.Bucket, bucketCnt)
		for i := range buckets {
			var upperBound []interface{}
			for _, v := range keyVals[i*offset] {
				upperBound = append(upperBound, v)
			}
			buckets[i] = stats.NewHistogramBucket(uint64(offset), uint64(offset), 0, 1, upperBound, nil, nil)
		}

		var cols []string
		var types []sql.Type
		for _, i := range ordinals {
			cols = append(cols, sch[i].Name)
			types = append(types, sch[i].Type)
		}

		qual, err := sql.NewQualifierFromString(string(key))
		if err != nil {
			return err
		}

		s.colStats[key] = stats.NewStatistic(rowCount, rowCount, 0, dataLen, time.Now(), qual, cols, types, buckets)
	}
	return nil
}

// reservoirSample selects a random subset of values from the table.
// Algorithm L from: https://dl.acm.org/doi/pdf/10.1145/198429.198435
func (s *StatsProv) reservoirSample(ctx *sql.Context, table sql.Table) ([]sql.Row, error) {
	// read through table
	var maxQueue float64 = 100
	var queue []sql.Row
	partIter, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	updateW := func(w float64) float64 {
		return w * math.Exp(math.Log(rand.Float64())/maxQueue)
	}
	updateI := func(i int, w float64) int {
		return i + int(math.Floor(math.Log(rand.Float64())/math.Log(1-w))) + 1
	}
	w := updateW(1)
	i := updateI(0, w)
	j := 0
	for {
		part, err := partIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		rowIter, err := table.PartitionRows(ctx, part)
		if err != nil {
			return nil, err
		}
		for {
			row, err := rowIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			if len(queue) < int(maxQueue) {
				queue = append(queue, row)
				j++
				continue
			}

			if j == i {
				// random swap
				pos := rand.Intn(int(maxQueue))
				queue[pos] = row
				// update
				w = updateW(w)
				i = updateI(i, w)
			}
			j++
		}
	}
	return queue, nil
}

func (s *StatsProv) GetTableStats(ctx *sql.Context, db, table string) ([]sql.Statistic, error) {
	pref := fmt.Sprintf("%s.%s", strings.ToLower(db), strings.ToLower(table))
	var ret []sql.Statistic
	for key, stats := range s.colStats {
		if strings.HasPrefix(string(key), pref) {
			ret = append(ret, stats)
		}
	}
	return ret, nil
}

func (s *StatsProv) SetStats(ctx *sql.Context, stats sql.Statistic) error {
	key := statsKey(fmt.Sprintf("%s.(%s)", stats.Qualifier(), strings.Join(stats.Columns(), ",")))
	s.colStats[key] = stats
	return nil
}

func (s *StatsProv) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	key := statsKey(fmt.Sprintf("%s.(%s)", qual, strings.Join(cols, ",")))
	if stats, ok := s.colStats[key]; ok {
		return stats, false
	}
	return nil, false
}

func (s *StatsProv) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	colsSuff := strings.Join(cols, ",") + ")"
	for key, _ := range s.colStats {
		if strings.HasPrefix(string(key), qual.String()) && strings.HasSuffix(string(key), colsSuff) {
			delete(s.colStats, key)
		}
	}
	return nil
}

func (s *StatsProv) RowCount(ctx *sql.Context, db, table string) (uint64, error) {
	pref := fmt.Sprintf("%s.%s", strings.ToLower(db), strings.ToLower(table))
	var cnt uint64
	for key, stats := range s.colStats {
		if strings.HasPrefix(string(key), pref) {
			if stats.RowCount() > cnt {
				cnt = stats.RowCount()
			}
		}
	}
	return cnt, nil
}

func (s *StatsProv) DataLength(ctx *sql.Context, db, table string) (uint64, error) {
	pref := fmt.Sprintf("%s.%s", db, table)
	var size uint64
	for key, stats := range s.colStats {
		if strings.HasPrefix(string(key), pref) {
			if stats.AvgSize() > size {
				size = stats.AvgSize()
			}
		}
	}
	return size, nil
}
