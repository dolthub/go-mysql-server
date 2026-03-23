package variables

import (
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestInitStatusVariablesRace reproduces a data race in InitStatusVariables.
//
// InitStatusVariables guards initialization with a nil check on the global
// sql.StatusVariables, but that check is not synchronized. When multiple
// goroutines call InitStatusVariables concurrently (e.g. parallel test
// processes each creating a new sql engine), they race on the read and
// write of sql.StatusVariables.
//
// NewBaseSession also reads sql.StatusVariables without synchronization,
// so an engine init racing with session creation triggers the same race.
//
// Run with: go test -race -run TestInitStatusVariablesRace ./sql/variables/
func TestInitStatusVariablesRace(t *testing.T) {
	// Reset global state so InitStatusVariables takes the nil path.
	sql.StatusVariables = nil

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Half the goroutines call InitStatusVariables (engine creation path),
	// half call NewBaseSession (connection path). This mirrors what happens
	// when parallel tests each open an embedded dolt database.
	for i := 0; i < goroutines; i++ {
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				InitStatusVariables()
			}()
		} else {
			go func() {
				defer wg.Done()
				_ = sql.NewBaseSession()
			}()
		}
	}

	wg.Wait()
}
