# Supported clients

These are the clients we actively test against to check are compatible with go-mysql-server. Other clients may also work, but we don't check on every build if we remain compatible with them.

- Python
  - [pymysql](#pymysql)
  - [mysql-connector](#python-mysql-connector)
- Ruby
  - [ruby-mysql](#ruby-mysql)
- [PHP](#php)
- Node.js
  - [mysqljs/mysql](#mysqljs)
- .NET Core
  - [MysqlConnector](#mysqlconnector)
- Java/JVM
  - [mariadb-java-client](#mariadb-java-client)
- Go
  - [go-mysql-driver/mysql](#go-mysql-driver-mysql)
- Grafana
- Tableau Desktop

## Example client usage

### pymysql

```python
import pymysql.cursors

connection = pymysql.connect(host='127.0.0.1',
                             user='user',
                             password='pass',
                             db='db',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        sql = "SELECT foo FROM bar"
        cursor.execute(sql)
        rows = cursor.fetchall()

        # use rows
finally:
    connection.close()
```

### Python mysql-connector

```python
import mysql.connector

connection = mysql.connector.connect(host='127.0.0.1',
                                user='user',
                                passwd='pass',
                                port=3306,
                                database='dbname')

try:
    cursor = connection.cursor()
    sql = "SELECT foo FROM bar"
    cursor.execute(sql)
    rows = cursor.fetchall()

    # use rows
finally:
    connection.close()
```

### ruby-mysql

```ruby
require "mysql"

conn = Mysql::new("127.0.0.1", "user", "pass", "dbname")
resp = conn.query "SELECT foo FROM bar"

# use resp

conn.close()
```

### php

```php
try {
    $conn = new PDO("mysql:host=127.0.0.1:3306;dbname=dbname", "user", "pass");
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $stmt = $conn->query('SELECT foo FROM bar');
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // use result
} catch (PDOException $e) {
    // handle error
}
```

### mysqljs

```js
import mysql from 'mysql';

const connection = mysql.createConnection({
    host: '127.0.0.1',
    port: 3306,
    user: 'user',
    password: 'pass',
    database: 'dbname'
});
connection.connect();

const query = 'SELECT foo FROM bar';
connection.query(query, function (error, results, _) {
    if (error) throw error;

    // use results
});

connection.end();
```

### MysqlConnector

```csharp
using MySql.Data.MySqlClient;
using System.Threading.Tasks;

namespace something
{
    public class Something
    {
        public async Task DoQuery()
        {
            var connectionString = "server=127.0.0.1;user id=user;password=pass;port=3306;database=dbname;";

            using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                var sql = "SELECT foo FROM bar";

                using (var cmd = new MySqlCommand(sql, conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync()) {
                    // use reader
                }
            }
        }
    }
}
```

### mariadb-java-client

```java
package org.testing.mariadbjavaclient;

import java.sql.*;

class Main {
    public static void main(String[] args) {
        String dbUrl = "jdbc:mariadb://127.0.0.1:3306/dbname?user=user&password=pass";
        String query = "SELECT foo FROM bar";

        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        // use rs
                    }
                }
            }
        } catch (SQLException e) {
            // handle failure
        }
    }
}
```

### go-sql-driver/mysql

```go
package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
    db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/test")
	if err != nil {
		// handle error
	}

	rows, err := db.Query("SELECT foo FROM bar")
	if err != nil {
		// handle error
    }

    // use rows
}
```