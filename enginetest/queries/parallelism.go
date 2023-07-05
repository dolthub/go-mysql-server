package queries

type ParallelismTest struct {
	Query    string
	Parallel bool
}

var ParallelismTests = []ParallelismTest{
	{
		Query:    "SELECT /*+ JOIN_ORDER(xy,scalarSubq0) LOOKUP_JOIN(xy,scalarSubq0) */ count(*) from xy where y in (select distinct v from uv)",
		Parallel: false,
	},
}
