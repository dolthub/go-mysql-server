package aggregation

import "github.com/dolthub/go-mysql-server/optgen/cmd/support"

//go:generate go run ../../../../optgen/cmd/optgen/main.go -out unary_aggs.og.go -pkg aggregation aggs unary_aggs.go

var UnaryAggDefs support.GenDefs = []support.AggDef{ // alphabetically sorted
	{
		Name:     "AnyValue",
		Desc:     "returns any single value in the grouped rows",
		Nullable: true,
	},
	{
		Name:     "Avg",
		Desc:     "returns the average value of expr in all rows.",
		Nullable: true,
	},
	{
		Name:    "BitAnd",
		Desc:    "returns the bitwise AND of all bits in expr.",
		RetType: "sql.Uint64",
	},
	{
		Name:    "BitOr",
		Desc:    "returns the bitwise OR of all bits in expr.",
		RetType: "sql.Uint64",
	},
	{
		Name:    "BitXor",
		Desc:    "returns the bitwise XOR of all bits in expr.",
		RetType: "sql.Uint64",
	},
	{
		Name:    "Count",
		Desc:    "returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.",
		RetType: "sql.Int64",
	},
	{
		Name: "First",
		Desc: "returns the first value in a sequence of elements of an aggregation.",
	},
	{
		Name:    "JsonArray",
		SqlName: "json_arrayagg",
		Desc:    "returns result set as a single JSON array.",
		RetType: "sql.JSON",
	},
	{
		Name: "Last",
		Desc: "returns the last value in a sequence of elements of an aggregation.",
	},
	{
		Name: "Max",
		Desc: "returns the maximum value of expr in all rows.",
	},
	{
		Name: "Min",
		Desc: "returns the minimum value of expr in all rows.",
	},
	{
		Name:     "Sum",
		Desc:     "returns the sum of expr in all rows",
		Nullable: false,
	},
}
