package queries

type ParallelismTest struct {
	Query    string
	Parallel bool
}

var ParallelismTests = []ParallelismTest{
	{
		Query:    "SELECT /*+ JOIN_ORDER(scalarSubq0,xy) LOOKUP_JOIN(xy,scalarSubq0) */ count(*) from xy where y in (select distinct v from uv)",
		Parallel: true,
	},
	{
		Query:    "SELECT /*+ JOIN_ORDER(scalarSubq0,xy) LOOKUP_JOIN(xy,scalarSubq0) */ count(*) from xy where y in (select distinct u from uv)",
		Parallel: false,
	},
}
