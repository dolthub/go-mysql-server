package aggregation

import "github.com/dolthub/go-mysql-server/optgen/cmd/support"

//go:generate optgen -out nary_aggs.og.go -pkg aggregation aggs nary_aggs.go

var NaryAggDefs support.GenDefs = []support.AggDef{ // alphabetically sorted'
	{
		Name:    "CountDistinct",
		Desc:    "returns the number of distinct values in a result set.",
		RetType: "sql.Int64",
		IsNary:  true,
	},
}
