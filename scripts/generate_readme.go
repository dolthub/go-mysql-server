package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

type Entry struct {
	Name string
	Desc string
}

func main() {
	// Hold in a list of Entry
	var entries []Entry

	// Combine and iterate through all functions
	numSupported := 0
	var funcs []sql.Function
	funcs = append(function.BuiltIns, function.GetLockingFuncs(nil)...)
	for _, f := range funcs {
		var numArgs int
		switch f.(type) {
		case sql.Function0:
			numArgs = 0
		case sql.Function1:
			numArgs = 1
		case sql.Function2:
			numArgs = 2
		case sql.Function3:
			numArgs = 3
		// TODO: there are no sql.Function 4,5,6,7 yet
		case sql.FunctionN:
			// try with no args to get error
			_, err := f.NewInstance([]sql.Expression{})
			// use error to get correct arg number
			if err != nil {
				numArgs = int(strings.Split(err.Error(), " ")[3][0]) - '0'
			} else {
				numArgs = 0
			}
		default:
			panic("Encountered unknown function type; probably b/c missing Function 4,5,6,7")
		}

		// Fill with appropriate number of arguments
		args := make([]sql.Expression, numArgs)
		for i := 0; i < numArgs; i++ {
			args[i] = expression.NewStar()
		}

		// special case for date_add and date_sub
		if f.FunctionName() == "date_add" || f.FunctionName() == "date_sub" {
			args = []sql.Expression{expression.NewStar(), expression.NewInterval(expression.NewStar(), "hi")}
		}

		// Create new instance
		f, err := f.NewInstance(args)
		if err != nil {
			if strings.Contains(err.Error(), "unsupported") {
				continue
			}
			panic(err)
		}
		fn := f.(sql.FunctionExpression)
		//fmt.Println(fn.FunctionName(), fn.Description())
		entries = append(entries, Entry{fn.FunctionName(), fn.Description()})
		numSupported++
	}

	// Sort entries
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	// Open/Create README.md
	file, err := os.Create("../README.md")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Define string constants, good luck making changes to this
	preTableString := "# go-mysql-server\n\n**go-mysql-server** is a SQL engine which parses standard SQL (based\non MySQL syntax) and executes queries on data sources of your\nchoice. A simple in-memory database and table implementation are\nprovided, and you can query any data source you want by implementing a\nfew interfaces.\n\n**go-mysql-server** also provides a server implementation compatible\nwith the MySQL wire protocol. That means it is compatible with MySQL\nODBC, JDBC, or the default MySQL client shell interface.\n\n[Dolt](https://www.doltdb.com), a SQL database with Git-style \nversioning, is the main database implementation of this package. \nCheck out that project for reference implementations. Or, hop into the Dolt discord [here](https://discord.com/invite/RFwfYpu)\nif you want to talk to the core developers behind GMS.\n\n## Scope of this project\n\nThese are the goals of **go-mysql-server**:\n\n- Be a generic extensible SQL engine that performs queries on your\n  data sources.\n- Provide a simple database implementation suitable for use in tests.\n- Define interfaces you can implement to query your own data sources.\n- Provide a runnable server speaking the MySQL wire protocol,\n  connected to data sources of your choice.\n- Optimize query plans.\n- Allow implementors to add their own analysis steps and\n  optimizations.\n- Support indexed lookups and joins on data tables that support them.\n- Support external index driver implementations such as pilosa.\n- With few caveats and using a full database implementation, be a\n  drop-in MySQL database replacement.\n\nNon-goals of **go-mysql-server**:\n\n- Be an application/server you can use directly.\n- Provide any kind of backend implementation (other than the `memory`\n  one used for testing) such as json, csv, yaml. That's for clients to\n  implement and use.\n\nWhat's the use case of **go-mysql-server**?\n\n**go-mysql-server** has two primary uses case:\n\n1. Stand-in for MySQL in a golang test environment, using the built-in\n   `memory` database implementation.\n\n2. Providing access to aribtrary data sources with SQL queries by\n   implementing a handful of interfaces. The most complete real-world\n   implementation is [Dolt](https://github.com/dolthub/dolt).\n\n## Installation\n\nThe import path for the package is `github.com/dolthub/go-mysql-server`.\n\nTo install it, run:\n\n```\ngo get github.com/dolthub/go-mysql-server\n```\n\n## Go Documentation\n\n* [go-mysql-server godoc](https://godoc.org/github.com/dolthub/go-mysql-server)\n\n## SQL syntax\n\nThe goal of **go-mysql-server** is to support 100% of the statements\nthat MySQL does. We are continuously adding more functionality to the\nengine, but not everything is supported yet. To see what is currently\nincluded check the [SUPPORTED](./SUPPORTED.md) file.\n\n## Third-party clients\n\nWe support and actively test against certain third-party clients to\nensure compatibility between them and go-mysql-server. You can check\nout the list of supported third party clients in the\n[SUPPORTED_CLIENTS](./SUPPORTED_CLIENTS.md) file along with some\nexamples on how to connect to go-mysql-server using them.\n## Available functions\n\n<!-- BEGIN FUNCTIONS -->\n"
	tableColumns := "|     Name     |                                               Description                                                                      |\n|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|\n"
	postTableString := "<!-- END FUNCTIONS -->\n\n## Configuration\n\nThe behaviour of certain parts of go-mysql-server can be configured\nusing either environment variables or session variables.\n\nSession variables are set using the following SQL queries:\n\n```sql\nSET <variable name> = <value>\n```\n\n<!-- BEGIN CONFIG -->\n| Name | Type | Description |\n|:-----|:-----|:------------|\n|`INMEMORY_JOINS`|environment|If set it will perform all joins in memory. Default is off.|\n|`inmemory_joins`|session|If set it will perform all joins in memory. Default is off. This has precedence over `INMEMORY_JOINS`.|\n|`MAX_MEMORY`|environment|The maximum number of memory, in megabytes, that can be consumed by go-mysql-server. Any in-memory caches or computations will no longer try to use memory when the limit is reached. Note that this may cause certain queries to fail if there is not enough memory available, such as queries using DISTINCT, ORDER BY or GROUP BY with groupings.|\n|`DEBUG_ANALYZER`|environment|If set, the analyzer will print debug messages. Default is off.|\n<!-- END CONFIG -->\n\n## Example\n\n`go-mysql-server` contains a SQL engine and server implementation. So,\nif you want to start a server, first instantiate the engine and pass\nyour `sql.Database` implementation.\n\nIt will be in charge of handling all the logic to retrieve the data\nfrom your source. Here you can see an example using the in-memory\ndatabase implementation:\n\n```go\npackage main\n\nimport (\n\t\"time\"\n\n\tsqle \"github.com/dolthub/go-mysql-server\"\n\t\"github.com/dolthub/go-mysql-server/auth\"\n\t\"github.com/dolthub/go-mysql-server/memory\"\n\t\"github.com/dolthub/go-mysql-server/server\"\n\t\"github.com/dolthub/go-mysql-server/sql\"\n\t\"github.com/dolthub/go-mysql-server/sql/information_schema\"\n)\n\n// Example of how to implement a MySQL server based on a Engine:\n//\n// ```\n// > mysql --host=127.0.0.1 --port=5123 -u user -ppass db -e \"SELECT * FROM mytable\"\n// +----------+-------------------+-------------------------------+---------------------+\n// | name     | email             | phone_numbers                 | created_at          |\n// +----------+-------------------+-------------------------------+---------------------+\n// | John Doe | john@doe.com      | [\"555-555-555\"]               | 2018-04-18 09:41:13 |\n// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |\n// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |\n// | Evil Bob | evilbob@gmail.com | [\"555-666-555\",\"666-666-666\"] | 2018-04-18 09:41:13 |\n// +----------+-------------------+-------------------------------+---------------------+\n// ```\nfunc main() {\n\tengine := sqle.NewDefault(\n\t\tsql.NewDatabaseProvider(\n\t\t\tcreateTestDatabase(),\n\t\t\tinformation_schema.NewInformationSchemaDatabase(),\n\t\t))\n\n\tconfig := server.Config{\n\t\tProtocol: \"tcp\",\n\t\tAddress:  \"localhost:3306\",\n\t\tAuth:     auth.NewNativeSingle(\"root\", \"\", auth.AllPermissions),\n\t}\n\n\ts, err := server.NewDefaultServer(config, engine)\n\tif err != nil {\n\t\tpanic(err)\n\t}\n\n\ts.Start()\n}\n\nfunc createTestDatabase() *memory.Database {\n\tconst (\n\t\tdbName    = \"mydb\"\n\t\ttableName = \"mytable\"\n\t)\n\n\tdb := memory.NewDatabase(dbName)\n\ttable := memory.NewTable(tableName, sql.Schema{\n\t\t{Name: \"name\", Type: sql.Text, Nullable: false, Source: tableName},\n\t\t{Name: \"email\", Type: sql.Text, Nullable: false, Source: tableName},\n\t\t{Name: \"phone_numbers\", Type: sql.JSON, Nullable: false, Source: tableName},\n\t\t{Name: \"created_at\", Type: sql.Timestamp, Nullable: false, Source: tableName},\n\t})\n\n\tdb.AddTable(tableName, table)\n\tctx := sql.NewEmptyContext()\n\ttable.Insert(ctx, sql.NewRow(\"John Doe\", \"john@doe.com\", []string{\"555-555-555\"}, time.Now()))\n\ttable.Insert(ctx, sql.NewRow(\"John Doe\", \"johnalt@doe.com\", []string{}, time.Now()))\n\ttable.Insert(ctx, sql.NewRow(\"Jane Doe\", \"jane@doe.com\", []string{}, time.Now()))\n\ttable.Insert(ctx, sql.NewRow(\"Evil Bob\", \"evilbob@gmail.com\", []string{\"555-666-555\", \"666-666-666\"}, time.Now()))\n\treturn db\n}\n\n```\n\nThen, you can connect to the server with any MySQL client:\n\n```bash\n> mysql --host=127.0.0.1 --port=3306 -u user -ppass test -e \"SELECT * FROM mytable\"\n+----------+-------------------+-------------------------------+---------------------+\n| name     | email             | phone_numbers                 | created_at          |\n+----------+-------------------+-------------------------------+---------------------+\n| John Doe | john@doe.com      | [\"555-555-555\"]               | 2018-04-18 10:42:58 |\n| John Doe | johnalt@doe.com   | []                            | 2018-04-18 10:42:58 |\n| Jane Doe | jane@doe.com      | []                            | 2018-04-18 10:42:58 |\n| Evil Bob | evilbob@gmail.com | [\"555-666-555\",\"666-666-666\"] | 2018-04-18 10:42:58 |\n+----------+-------------------+-------------------------------+---------------------+\n```\n\nSee the complete example [here](_example/main.go).\n\n### Queries examples\n\n```\nSELECT count(name) FROM mytable\n+---------------------+\n| COUNT(mytable.name) |\n+---------------------+\n|                   4 |\n+---------------------+\n\nSELECT name,year(created_at) FROM mytable\n+----------+--------------------------+\n| name     | YEAR(mytable.created_at) |\n+----------+--------------------------+\n| John Doe |                     2018 |\n| John Doe |                     2018 |\n| Jane Doe |                     2018 |\n| Evil Bob |                     2018 |\n+----------+--------------------------+\n\nSELECT email FROM mytable WHERE name = 'Evil Bob'\n+-------------------+\n| email             |\n+-------------------+\n| evilbob@gmail.com |\n+-------------------+\n```\n\n## Custom data source implementation\n\nTo create your own data source implementation you need to implement\nthe following interfaces:\n\n- `sql.Database` interface. This interface will provide tables from\n  your data source. You can also implement other interfaces on your\n  database to unlock additional functionality:\n  - `sql.TableCreator` to support creating new tables\n  - `sql.TableDropper` to support dropping  tables\n  - `sql.TableRenamer` to support renaming tables\n  - `sql.ViewCreator` to support creating persisted views on your tables\n  - `sql.ViewDropper` to support dropping persisted views\n\n- `sql.Table` interface. This interface will provide rows of values\n  from your data source. You can also implement other interfaces on\n  your table to unlock additional functionality:\n  - `sql.InsertableTable` to allow your data source to be updated with\n    `INSERT` statements.\n  - `sql.UpdateableTable` to allow your data source to be updated with\n    `UPDATE` statements. \n  - `sql.DeletableTable` to allow your data source to be updated with\n    `DELETE` statements. \n  - `sql.ReplaceableTable` to allow your data source to be updated with\n    `REPLACE` statements.\n  - `sql.AlterableTable` to allow your data source to have its schema\n    modified by adding, dropping, and altering columns.\n  - `sql.IndexedTable` to declare your table's native indexes to speed\n    up query execution.\n  - `sql.IndexAlterableTable` to accept the creation of new native\n    indexes.\n  - `sql.ForeignKeyAlterableTable` to signal your support of foreign\n    key constraints in your table's schema and data.\n  - `sql.ProjectedTable` to return rows that only contain a subset of\n    the columns in the table. This can make query execution faster.\n  - `sql.FilteredTable` to filter the rows returned by your table to\n    those matching a given expression. This can make query execution\n    faster (if your table implementation can filter rows more\n    efficiently than checking an expression on every row in a table).\n\nYou can see a really simple data source implementation in the `memory`\npackage.\n\n## Testing your data source implementation\n\n**go-mysql-server** provides a suite of engine tests that you can use\nto validate that your implementation works as expected. See the\n`enginetest` package for details and examples.\n\n## Indexes\n\n`go-mysql-server` exposes a series of interfaces to allow you to\nimplement your own indexes so you can speed up your queries.\n\n## Native indexes\n\nTables can declare that they support native indexes, which means that\nthey support efficiently returning a subset of their rows that match\nan expression. The `memory` package contains an example of this\nbehavior, but please note that it is only for example purposes and\ndoesn't actually make queries faster (although we could change this in\nthe future).\n\nIntegrators should implement the `sql.IndexedTable` interface to\ndeclare which indexes their tables support and provide a means of\nreturning a subset of the rows based on an `sql.IndexLookup` provided\nby their `sql.Index` implementation. There are a variety of extensions\nto `sql.Index` that can be implemented, each of which unlocks\nadditional capabilities:\n\n- `sql.Index`. Base-level interface, supporting equality lookups for\n  an index.\n- `sql.AscendIndex`. Adds support for `>` and `>=` indexed lookups.\n- `sql.DescendIndex`. Adds support for `<` and `<=` indexed lookups.\n- `sql.NegateIndex`. Adds support for negating other index lookups.\n- `sql.MergeableIndexLookup`. Adds support for merging two\n  `sql.IndexLookup`s together to create a new one, representing `AND`\n  and `OR` expressions on indexed columns.\n\n## Custom index driver implementation\n\nIndex drivers provide different backends for storing and querying\nindexes, without the need for a table to store and query its own\nnative indexes. To implement a custom index driver you need to\nimplement a few things:\n\n- `sql.IndexDriver` interface, which will be the driver itself. Not\n  that your driver must return an unique ID in the `ID` method. This\n  ID is unique for your driver and should not clash with any other\n  registered driver. It's the driver's responsibility to be fault\n  tolerant and be able to automatically detect and recover from\n  corruption in indexes.\n- `sql.Index` interface, returned by your driver when an index is\n  loaded or created.\n- `sql.IndexValueIter` interface, which will be returned by your\n  `sql.IndexLookup` and should return the values of the index.\n- Don't forget to register the index driver in your `sql.Context`\n  using `context.RegisterIndexDriver(mydriver)` to be able to use it.\n\nTo create indexes using your custom index driver you need to use\nextension syntax `USING driverid` on the index creation statement. For\nexample:\n\n```sql\nCREATE INDEX foo ON table USING driverid (col1, col2)\n```\n\ngo-mysql-server does not provide a production index driver\nimplementation. We previously provided a pilosa implementation, but\nremoved it due to the difficulty of supporting it on all platforms\n(pilosa doesn't work on Windows).\n\nYou can see an example of a driver implementation in the memory\npackage.\n\n### Metrics\n\n`go-mysql-server` utilizes `github.com/go-kit/kit/metrics` module to\nexpose metrics (counters, gauges, histograms) for certain packages (so\nfar for `engine`, `analyzer`, `regex`). If you already have\nmetrics server (prometheus, statsd/statsite, influxdb, etc.) and you\nwant to gather metrics also from `go-mysql-server` components, you\nwill need to initialize some global variables by particular\nimplementations to satisfy following interfaces:\n\n```go\n// Counter describes a metric that accumulates values monotonically.\ntype Counter interface {\n\tWith(labelValues ...string) Counter\n\tAdd(delta float64)\n}\n\n// Gauge describes a metric that takes specific values over time.\ntype Gauge interface {\n\tWith(labelValues ...string) Gauge\n\tSet(value float64)\n\tAdd(delta float64)\n}\n\n// Histogram describes a metric that takes repeated observations of the same\n// kind of thing, and produces a statistical summary of those observations,\n// typically expressed as quantiles or buckets.\ntype Histogram interface {\n\tWith(labelValues ...string) Histogram\n\tObserve(value float64)\n}\n```\n\nYou can use one of `go-kit` implementations or try your own.  For\ninstance, we want to expose metrics for _prometheus_ server. So,\nbefore we start _mysql engine_, we have to set up the following\nvariables:\n\n```go\n\nimport(\n    \"github.com/go-kit/kit/metrics/prometheus\"\n    promopts \"github.com/prometheus/client_golang/prometheus\"\n    \"github.com/prometheus/client_golang/prometheus/promhttp\"\n)\n\n//....\n\n// engine metrics\nsqle.QueryCounter = prometheus.NewCounterFrom(promopts.CounterOpts{\n\t\tNamespace: \"go_mysql_server\",\n\t\tSubsystem: \"engine\",\n\t\tName:      \"query_counter\",\n\t}, []string{\n\t\t\"query\",\n\t})\nsqle.QueryErrorCounter = prometheus.NewCounterFrom(promopts.CounterOpts{\n    Namespace: \"go_mysql_server\",\n    Subsystem: \"engine\",\n    Name:      \"query_error_counter\",\n}, []string{\n    \"query\",\n    \"error\",\n})\nsqle.QueryHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{\n    Namespace: \"go_mysql_server\",\n    Subsystem: \"engine\",\n    Name:      \"query_histogram\",\n}, []string{\n    \"query\",\n    \"duration\",\n})\n\n// analyzer metrics\nanalyzer.ParallelQueryCounter = prometheus.NewCounterFrom(promopts.CounterOpts{\n    Namespace: \"go_mysql_server\",\n    Subsystem: \"analyzer\",\n    Name:      \"parallel_query_counter\",\n}, []string{\n    \"parallelism\",\n})\n\n// regex metrics\nregex.CompileHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{\n    Namespace: \"go_mysql_server\",\n    Subsystem: \"regex\",\n    Name:      \"compile_histogram\",\n}, []string{\n    \"regex\",\n    \"duration\",\n})\nregex.MatchHistogram = prometheus.NewHistogramFrom(promopts.HistogramOpts{\n    Namespace: \"go_mysql_server\",\n    Subsystem: \"regex\",\n    Name:      \"match_histogram\",\n}, []string{\n    \"string\",\n    \"duration\",\n})\n```\n\nOne _important note_ - internally we set some _labels_ for metrics,\nthat's why have to pass those keys like \"duration\", \"query\", \"driver\",\n... when we register metrics in `prometheus`. Other systems may have\ndifferent requirements.\n\n## Powered by go-mysql-server\n\n* [dolt](https://github.com/dolthub/dolt)\n* [gitbase](https://github.com/src-d/gitbase) (defunct)\n\n## Acknowledgements\n\n**go-mysql-server** was originally developed by the {source-d} organzation, and this repository was originally forked from [src-d](https://github.com/src-d/go-mysql-server). We want to thank the entire {source-d} development team for their work on this project, especially Miguel Molina (@erizocosmico) and Juanjo Álvarez Martinez (@juanjux).\n\n## License\n\nApache License 2.0, see [LICENSE](/LICENSE)\n"

	// Write to README
	file.WriteString(preTableString)
	file.WriteString(tableColumns)
	for _, e := range entries {
		// TODO: need to include argument types somehow
		file.WriteString("|`" + strings.ToUpper(e.Name) + "`| " + e.Desc + "|\n")
	}
	file.WriteString(postTableString)

	// Might be useful for dolt docs
	fmt.Println("num defined:", len(funcs))
	fmt.Println("num supported: ", numSupported)
}
