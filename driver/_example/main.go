// Copyright 2020-2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
)

func main() {
	sql.Register("sqle", driver.New(factory{}, nil))

	db, err := sql.Open("sqle", "")
	must(err)

	_, err = db.Exec("USE mydb")
	must(err)

	rows, err := db.Query("SELECT * FROM mytable")
	must(err)
	dump(rows)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func dump(rows *sql.Rows) {
	var name, email string
	var phoneNumbers string
	var created_at time.Time

	for rows.Next() {
		must(rows.Scan(&name, &email, &phoneNumbers, &created_at))
		fmt.Println(name, email, phoneNumbers, created_at)
	}
}
