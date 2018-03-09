package main

import (
	"time"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Example of how to implement a MySQL server based on a Engine:
//
// ```
// > mysql --host=127.0.0.1 --port=5123 -u user1 -ppassword1 db -e "SELECT * FROM mytable"
// +----------+-------------------+---------------------+
// | name     | email             | created_at          |
// +----------+-------------------+---------------------+
// | John Doe | john@doe.com      | 2018-02-14 01:15:40 |
// | John Doe | johnalt@doe.com   | 2018-02-14 01:15:40 |
// | Jane Doe | jane@doe.com      | 2018-02-14 01:15:40 |
// | Evil Bob | evilbob@gmail.com | 2018-02-14 01:15:40 |
// +----------+-------------------+---------------------+
// ```
func main() {
	driver := sqle.New()
	driver.AddDatabase(createTestDatabase())

	auth := mysql.NewAuthServerStatic()
	auth.Entries["user1"] = []*mysql.AuthServerStaticEntry{{
		Password: "password1",
	}}

	s, err := server.NewDefaultServer("tcp", "localhost:5123", auth, driver)
	if err != nil {
		panic(err)
	}

	s.Start()
}

func createTestDatabase() *mem.Database {
	db := mem.NewDatabase("test")
	table := mem.NewTable("mytable", sql.Schema{
		{Name: "name", Type: sql.Text},
		{Name: "email", Type: sql.Text},
		{Name: "phone_numbers", Type: sql.JSON},
		{Name: "created_at", Type: sql.Timestamp},
	})
	db.AddTable("mytable", table)
	table.Insert(sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()))
	table.Insert(sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()))
	return db
}
