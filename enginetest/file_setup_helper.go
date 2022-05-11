package enginetest

var (
	mytable        []string
	keylessSetup   []string
	versionedSetup []string
	specialSetup   []string
	simpleSetup    []string
	ordinalSetup   []string
	spatialSetup   []string
	jsonSetup      []string
	fooSetup       []string
	graphSetup     []string
	reservedSetup  []string
	checksSetup    []string
	nullsSetup     []string
)

func init() {
	keylessSetup = []string{"mydb", "keyless"}
	versionedSetup = []string{"mydb", "myhistorytable"}
	specialSetup = []string{
		"mydb",
		"autoincrement",
		"bigtable",
		"datetimetable",
		"emptytable",
		"fk_tbl",
		"floattable",
		"newlinetable",
		"niltable",
		"othertable",
		"specialtable",
		"stringandtable",
		"tabletest",
		"typestable",
		"people",
		"reserved_keywords",
	}
	ordinalSetup = []string{"mydb", "invert_pk"}
	fooSetup = []string{"mydb", "foo"}
	jsonSetup = []string{"mydb", "jsontable"}
	spatialSetup = []string{"mydb", "spatial"}
	pksSetup := []string{"mydb", "pk_tables"}
	graphSetup = []string{"mydb", "graph_tables"}
	reservedSetup = []string{"mydb", "reserved"}
	mytable = []string{"mydb", "mytable"}
	checksSetup = []string{"mydb", "check_constraints"}
	nullsSetup = []string{"mydb", "null_ranges"}
	simpleSetup = concatenateSetupSources(
		mytable,
		specialSetup,
		pksSetup,
		ordinalSetup,
		jsonSetup,
		versionedSetup,
		keylessSetup,
		fooSetup,
		graphSetup,
	)

}

func concatenateSetupSources(in ...[]string) []string {
	out := make([]string, 0)
	for i := range in {
		out = append(out, in[i]...)
	}
	return out
}
