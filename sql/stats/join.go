package stats

import "github.com/dolthub/go-mysql-server/sql"

// TODO align two histograms

// TODO join two histograms

func Join(s1, s2 sql.Statistic) sql.Statistic {
	// alignment algo
	// find the minimum buckets that overlap
	// either 1) stepwise bucket alignment and comparison, subtract mcvs from distinct estimates
	// or     2) direct compare first, do MCV card separately, and then range card as third
}
