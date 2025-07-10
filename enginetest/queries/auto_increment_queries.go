// TODO: MOVE THESE TO SCRIPT QUERIES

package queries

var AutoIncTests = []ScriptTest{
	// Char tests
	{
		//Skip:        true,
		Name:    "decimal with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent(d decimal(3,1) primary key);",
			"insert into parent values (1.23), (4.56), (78.9);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (d decimal primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},
}
