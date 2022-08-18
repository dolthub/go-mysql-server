package e2etests

import (
	"testing"
	"time"

	connector "database/sql"

	_ "github.com/go-sql-driver/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/stretchr/testify/require"
)

func Test_TimestampBindings_CanBeConverted(t *testing.T) {
	provider := sql.NewDatabaseProvider(
		memory.NewDatabase("mydb"),
	)
	engine := sqle.New(analyzer.NewDefault(provider), &sqle.Config{
		IncludeRootAccount: true,
	})
	cfg := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
	}
	srv, err := server.NewDefaultServer(cfg, engine)
	require.NoError(t, err)
	go srv.Start()
	defer srv.Close()

	db, err := connector.Open("mysql", "root:@tcp(localhost:3306)/mydb")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE mytable (t TIMESTAMP)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO mytable (t) VALUES (?)", time.Now())
	require.NoError(t, err)
}
