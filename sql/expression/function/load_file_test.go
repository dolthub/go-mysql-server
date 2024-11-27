package function

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	// Create a valid temp file and temp directory
	_, file, err := createTempDirAndFile("myfile.txt")
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("my data"))
	assert.NoError(t, err)

	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(fileName)

	// Assert that Load File returns the regardless since secure_file_priv is set to an empty directory
	res, err := fn.Eval(sql.NewEmptyContext(), sql.UntypedSqlRow{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("my data"), res)
}

func TestLoadFileBadDir(t *testing.T) {
	// Create a valid temp file and temp directory
	_, file, err := createTempDirAndFile("myfile.txt")
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	// Set the secure_file_priv var but make it different than the file directory
	vars := make(map[string]interface{})
	vars["secure_file_priv"] = "/not/a/real/directory"
	err = sql.SystemVariables.AssignValues(vars)
	assert.NoError(t, err)

	_, err = file.Write([]byte("my data"))
	assert.NoError(t, err)

	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(fileName)

	// Assert that Load File returns nil since the file is not in secure_file_priv directory
	res, err := fn.Eval(sql.NewEmptyContext(), sql.UntypedSqlRow{})
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
}

type loadFileTestCase struct {
	name     string
	fileData []byte
	fileName string
}

func TestLoadFile(t *testing.T) {
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
	err := sql.SystemVariables.AssignValues(vars)
	assert.NoError(t, err)

	for _, tt := range testCases {
		runLoadFileTest(t, tt, dir)
	}
}

// runLoadFileTest takes in a loadFileTestCase and its relevant directory and validates whether LOAD_FILE is reading
// the file accordingly.
func runLoadFileTest(t *testing.T, tt loadFileTestCase, dir string) {
	file, err := os.CreateTemp(dir, tt.fileName)
	assert.NoError(t, err)

	defer file.Close()
	defer os.Remove(file.Name())

	// Write some data to the file
	_, err = file.Write(tt.fileData)
	assert.NoError(t, err)

	// Setup the file data
	fileName := expression.NewLiteral(file.Name(), types.Text)
	fn := NewLoadFile(fileName)

	// Load the file in
	res, err := fn.Eval(sql.NewEmptyContext(), sql.UntypedSqlRow{})
	assert.NoError(t, err)
	assert.Equal(t, tt.fileData, res)
}
