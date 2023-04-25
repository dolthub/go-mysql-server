package index

import "github.com/dolthub/go-mysql-server/sql"

type cardinalityIndex struct {
    table string
    index string
    stats *sql.IndexStats
}

func NewCardinalityIndex(table, index string, stats *sql.IndexStats) sql.StatisticsIndex {
    return &cardinalityIndex{table: table, index: index, stats: stats}
}

func (i *cardinalityIndex) RowCount() (int64, error) {
    return i.stats.Cardinality()
}

