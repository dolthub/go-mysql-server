package enginetest

type ConcurrentTransactionTest struct {
	Name string

	SetUpScript []string

	ConcurrentTransactions [][]string

	Assertion []ScriptTestAssertion

	// TODO: Add a top level timeout
}

var ConcurrentTests = []ConcurrentTransactionTest{
	{
		Name: "Two tests concurrently acquiring locks does not deadlock",
		SetUpScript: []string{
			"CREATE TABLE keyed(pk int primary key, val int)",
			"INSERT INTO keyed values (1, 1), (2, 2), (3, 3), (4, 4), (5,5)",
		},
		ConcurrentTransactions: [][]string{
			{
				"/* client a */ SELECT SLEEP(3)",
				"/* client a */ SET AUTOCOMMIT = 0",
				"/* client a */ BEGIN",
				"/* client a */ SELECT * from keyed FOR UPDATE",
				"/* client a */ COMMIT",
			},
			{
				"/* client b */ SET AUTOCOMMIT = 0",
				"/* client b */ BEGIN",
				"/* client b */ SELECT * from keyed FOR UPDATE",
				"/* client b */ COMMIT",
			},
		},
	},
}

// Failing concurrent tests
// multi table update, multi db updates, ddl statements, etc
