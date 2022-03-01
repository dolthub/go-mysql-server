package enginetest

import "github.com/dolthub/go-mysql-server/sql"

type ConcurrentTransactionTest struct {
	Name string

	SetUpScript []string

	ConcurrentTransactions [][]string

	Assertion []ScriptTestAssertion

	// TODO: Add a top level timeout
}

// TODO: Add a test where a select for update node tries select for update on itself
// TODO: Add a test where a bad insert hapens and the things doesn't deadoclk
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
	{
		Name: "One transaction grabs locks, the other waits to insert",
		SetUpScript: []string{
			"CREATE TABLE keyed(pk int primary key, val int)",
			"INSERT INTO keyed values (1, 1), (2, 2), (3, 3), (4, 4), (5,5)",
		},
		ConcurrentTransactions: [][]string{
			{
				"/* client a */ SET AUTOCOMMIT = 0",
				"/* client a */ BEGIN",
				"/* client a */ SELECT * from keyed FOR UPDATE",
				"/* client a */ SLEEP(5)",
				"/* client a */ COMMIT",
			},
			{
				"/* client b */ SELECT SLEEP(5)",
				"/* client b */ SET AUTOCOMMIT = 0",
				"/* client b */ BEGIN",
				"/* client b */ INSERT INTO keyed VALUES (6, 6)",
				"/* client b */ COMMIT",
			},
		},
		Assertion: []ScriptTestAssertion{
			{
				Query:    "/* client b */ begin",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */  SELECT COUNT(*) from keyed where pk = 6",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ begin",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */  SELECT COUNT(*) from keyed where pk = 6",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Calling select for update twice on the same client doesn't block",
		SetUpScript: []string{
			"CREATE TABLE keyed(pk int primary key, val int)",
			"INSERT INTO keyed values (1, 1), (2, 2), (3, 3), (4, 4), (5,5)",
		},
		ConcurrentTransactions: [][]string{
			{
				"/* client a */ SET AUTOCOMMIT = 0",
				"/* client a */ BEGIN",
				"/* client a */ SELECT * from keyed FOR UPDATE",
				"/* client a */ SELECT * from keyed FOR UPDATE",
				"/* client a */ COMMIT",
			},
		},
	},
	//{
	//	Name: "One transaction grabs locks, the other waits and inserts a bad row. The first transaction can eventually grab the lock",
	//	SetUpScript: []string{
	//		"CREATE TABLE keyed(pk int primary key, val int)",
	//		"ALTER TABLE keyed ADD CONSTRAINT idx CHECK (val < 4)",
	//		"INSERT INTO keyed values (1, 1), (2, 2), (3, 3)",
	//	},
	//	ConcurrentTransactions: [][]string{
	//		{
	//			"/* client a */ SET AUTOCOMMIT = 0",
	//			"/* client a */ BEGIN",
	//			"/* client a */ SELECT * from keyed FOR UPDATE",
	//			"/* client a */ SLEEP(2)",
	//			"/* client a */ COMMIT",
	//			"/* client a */ SLEEP(2)",
	//			"/* client a */ SELECT * from keyed FOR UPDATE",
	//		},
	//		{
	//			"/* client b */ SELECT SLEEP(2)",
	//			"/* client b */ SET AUTOCOMMIT = 0",
	//			"/* client b */ BEGIN",
	//			"/* client b */ INSERT INTO keyed VALUES (6, 6)",
	//			"/* client b */ COMMIT",
	//		},
	//	},
	//},
}

// Failing concurrent tests
// multi table update, multi db updates, ddl statements, etc
