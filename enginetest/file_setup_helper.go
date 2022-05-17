package enginetest

var (
	mytable           [][]Testdata
	keylessSetup      [][]Testdata
	versionedSetup    [][]Testdata
	specialSetup      [][]Testdata
	simpleSetup       [][]Testdata
	ordinalSetup      [][]Testdata
	spatialSetup      [][]Testdata
	jsonSetup         [][]Testdata
	fooSetup          [][]Testdata
	graphSetup        [][]Testdata
	reservedSetup     [][]Testdata
	checksSetup       [][]Testdata
	nullsSetup        [][]Testdata
	complexIndexSetup [][]Testdata
	loadDataSetup     [][]Testdata
)

func init() {
	keylessSetup = [][]Testdata{MydbData, KeylessData}
	versionedSetup = [][]Testdata{MydbData, MyhistorytableData}
	specialSetup = [][]Testdata{
		MydbData,
		AutoincrementData,
		BigtableData,
		DatetimetableData,
		EmptytableData,
		Fk_tblData,
		FloattableData,
		NewlinetableData,
		NiltableData,
		OthertableData,
		SpecialtableData,
		StringandtableData,
		TabletestData,
		TypestableData,
		PeopleData,
		Reserved_keywordsData,
	}
	ordinalSetup = [][]Testdata{MydbData, Invert_pkData, Ordinals_ddlData}
	fooSetup = [][]Testdata{MydbData, FooData}
	jsonSetup = [][]Testdata{MydbData, JsontableData}
	spatialSetup = [][]Testdata{MydbData, SpatialData}
	pksSetup := [][]Testdata{MydbData, Pk_tablesData}
	graphSetup = [][]Testdata{MydbData, Graph_tablesData}
	reservedSetup = [][]Testdata{MydbData, Reserved_keywordsData}
	mytable = [][]Testdata{MydbData, MytableData}
	checksSetup = [][]Testdata{MydbData, Check_constraintData}
	nullsSetup = [][]Testdata{MydbData, Null_rangesData}
	complexIndexSetup = [][]Testdata{MydbData, Comp_index_tablesData}
	loadDataSetup = [][]Testdata{MydbData, LoadtableData}
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

func concatenateSetupSources(in ...[][]Testdata) [][]Testdata {
	out := make([][]Testdata, 0)
	for i := range in {
		out = append(out, in[i]...)
	}
	return out
}
