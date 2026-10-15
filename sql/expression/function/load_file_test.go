package function

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

// createTempDirAndFile returns the temporary directory, as well creates a new file (var filename) that lives in it.
func createTempDirAndFile(fileName string) (string, *os.File, error) {
	dir := os.TempDir()

	file, err := os.CreateTemp(dir, fileName)
	if err != nil {
		return "", nil, err
	}

	return dir, file, nil
}

func TestLoadFileNoSecurePriv(t *testing.T) {
	ctx := sql.NewContextWithVariables(t.Context(), variables.NewEngineVariables())
	// Create a valid temp file and temp directory
	_, file, err := createTempDirAndFile("myfile.txt")
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("my data"))
	assert.NoError(t, err)

	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(ctx, fileName)

	// Assert that Load File returns the regardless since secure_file_priv is set to an empty directory
	res, err := fn.Eval(ctx, sql.Row{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("my data"), res)
}

func TestLoadFileBadDir(t *testing.T) {
	ctx := sql.NewContextWithVariables(t.Context(), variables.NewEngineVariables())
	// Create a valid temp file and temp directory
	_, file, err := createTempDirAndFile("myfile.txt")
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	// Set the secure_file_priv var but make it different than the file directory
	vars := make(map[string]interface{})
	vars["secure_file_priv"] = "/not/a/real/directory"
	err = ctx.SystemVariables().AssignValues(vars)
	assert.NoError(t, err)
	ctx = sql.NewContextWithVariables(t.Context(), ctx.EngineVariables())

	_, err = file.Write([]byte("my data"))
	assert.NoError(t, err)

	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(ctx, fileName)

	// Assert that Load File returns nil since the file is not in secure_file_priv directory
	res, err := fn.Eval(ctx, sql.Row{})
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
}

type loadFileTestCase struct {
	name     string
	fileData []byte
	fileName string
}

func TestLoadFile(t *testing.T) {
	ctx := sql.NewContextWithVariables(t.Context(), variables.NewEngineVariables())
	testCases := []loadFileTestCase{
		{
			"simple example",
			[]byte("important test case"),
			"myfile.txt",
		},
		{
			"blob",
			[]byte("\\xFF\\xD8\\xFF\\xE1\\x00"),
			"myfile.jpg",
		},
	}

	// create the temp dir
	dir := os.TempDir()

	// Set the secure_file_priv var
	vars := make(map[string]interface{})
	vars["secure_file_priv"] = dir
	err := ctx.SystemVariables().AssignValues(vars)
	assert.NoError(t, err)
	ctx = sql.NewContextWithVariables(t.Context(), ctx.EngineVariables())

	for _, tt := range testCases {
		runLoadFileTest(t, ctx, tt, dir)
	}

	t.Run("releases file and directory handles", func(t *testing.T) {
		// Cleanup must not depend on finalizers closing leaked handles.
		gcPercent := debug.SetGCPercent(-1)
		t.Cleanup(func() { debug.SetGCPercent(gcPercent) })
		for _, mode := range []string{"allowed", "different directory", "missing directory"} {
			t.Run(mode, func(t *testing.T) {
				dir := t.TempDir()
				other := t.TempDir()
				path := filepath.Join(dir, "data")
				require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
				if mode == "different directory" {
					path = filepath.Join(other, "data")
					require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
				} else if mode == "missing directory" {
					path = filepath.Join(other, "missing", "data")
				}
				require.NoError(t, ctx.SystemVariables().AssignValues(map[string]interface{}{"secure_file_priv": dir}))
				evalCtx := sql.NewContextWithVariables(t.Context(), ctx.EngineVariables())
				fn := NewLoadFile(evalCtx, expression.NewLiteral(path, types.Text))
				res, err := fn.Eval(evalCtx, nil)
				require.NoError(t, err)
				if mode == "allowed" {
					assert.Equal(t, []byte("data"), res)
				} else {
					assert.Nil(t, res)
				}
				// Windows rejects deletion while a file or directory handle is open.
				require.NoError(t, os.RemoveAll(dir))
				require.NoError(t, os.RemoveAll(other))
			})
		}
	})

}

// runLoadFileTest takes in a loadFileTestCase and its relevant directory and validates whether LOAD_FILE is reading
// the file accordingly.
func runLoadFileTest(t *testing.T, ctx *sql.Context, tt loadFileTestCase, dir string) {
	file, err := os.CreateTemp(dir, tt.fileName)
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	// Write some data to the file
	_, err = file.Write(tt.fileData)
	assert.NoError(t, err)

	// Setup the file data
	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(ctx, fileName)

	// Load the file in
	res, err := fn.Eval(ctx, sql.Row{})
	assert.NoError(t, err)
	assert.Equal(t, tt.fileData, res)
}
