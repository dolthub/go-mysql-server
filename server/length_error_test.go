package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/server"
	vsql "github.com/dolthub/vitess/go/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Check the wire metadata that drivers use to distinguish data errors from
// operational errors, through both COM_QUERY and COM_STMT_EXECUTE.
func TestLengthErrorProtocol(t *testing.T) {
	_, host, port := startWatcherTestServer(t, server.Config{})
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/mydb", host, port))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()
	for _, mode := range []string{"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} {
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prepared=%t", mode, prepared), func(t *testing.T) {
				exec := func(query string) error {
					if prepared {
						stmt, err := conn.PrepareContext(ctx, query)
						if err != nil {
							return err
						}
						defer stmt.Close()
						_, err = stmt.ExecContext(ctx)
						return err
					}
					_, err := conn.ExecContext(ctx, query)
					return err
				}
				require.NoError(t, exec("SET SESSION sql_mode = '"+mode+"'"))
				require.NoError(t, exec("DROP TABLE IF EXISTS length_test"))
				require.NoError(t, exec("CREATE TABLE length_test (id INT PRIMARY KEY, value VARCHAR(2))"))
				require.NoError(t, exec("INSERT INTO length_test VALUES (1, 'ok'), (2, 'é日')"))
				for _, query := range []string{
					"INSERT INTO length_test VALUES (3, 'long')",
					"INSERT INTO length_test SELECT 3, 'long'",
					"UPDATE length_test SET value = 'long' WHERE id = 1",
					"INSERT INTO length_test VALUES (1, 'ok') ON DUPLICATE KEY UPDATE value = 'long'",
				} {
					err := exec(query)
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr, query)
					require.Equal(t, uint16(vsql.ERDataTooLong), mysqlErr.Number, query)
					require.Equal(t, vsql.SSDataTooLong, string(mysqlErr.SQLState[:]), query)
					require.Contains(t, mysqlErr.Message, "string 'long' is too large for column 'value'", query)
				}
				if prepared {
					stmt, err := conn.PrepareContext(ctx, "INSERT INTO length_test VALUES (?, ?)")
					require.NoError(t, err)
					_, err = stmt.ExecContext(ctx, 3, "long")
					require.NoError(t, stmt.Close())
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					require.Equal(t, uint16(vsql.ERDataTooLong), mysqlErr.Number)
					require.Equal(t, vsql.SSDataTooLong, string(mysqlErr.SQLState[:]))
				}
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM length_test").Scan(&count))
				require.Equal(t, 2, count)
				var value string
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT value FROM length_test WHERE id = 1").Scan(&value))
				require.Equal(t, "ok", value)
				for _, query := range []string{
					"INSERT IGNORE INTO length_test VALUES (3, 'long')",
					"UPDATE IGNORE length_test SET value = 'long' WHERE id = 1",
				} {
					require.NoError(t, exec(query))
					var level, message string
					var code int
					require.NoError(t, conn.QueryRowContext(ctx, "SHOW WARNINGS").Scan(&level, &code, &message))
					require.Equal(t, "Warning", level)
					require.Equal(t, vsql.ERWarnDataTruncated, code)
				}
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT value FROM length_test WHERE id = 1").Scan(&value))
				require.Equal(t, "lo", value)
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT value FROM length_test WHERE id = 3").Scan(&value))
				require.Equal(t, "lo", value)
			})
		}
	}
}
