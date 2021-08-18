package function

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func createTempDirAndFile(fileName string) (string, *os.File, error) {
	dir := os.TempDir()

	file, err := ioutil.TempFile(dir, fileName)
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

	fileName := expression.NewLiteral(file.Name(), sql.Text)
	fn := NewLoadFile(sql.NewEmptyContext(), fileName)

	// Assert that Load File returns the regardless since secure_file_priv is set to an empty directory
	res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
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

	fileName := expression.NewLiteral(file.Name(), sql.Text)
	fn := NewLoadFile(sql.NewEmptyContext(), fileName)

	// Assert that Load File returns nil since the file is not in secure_file_priv directory
	res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
}

func TestLoadFile(t *testing.T) {
	testCases := []struct {
		name      string
		fileData  []byte
		fileName  string
		expectNil bool
	}{
		{
			"simple example",
			[]byte("important test case"),
			"myfile.txt",
			false,
		},
		{
			"blob",
			[]byte("\\xFF\\xD8\\xFF\\xE1\\x00"),
			"myfile.jpg",
			false,
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
		file, err := ioutil.TempFile(dir, tt.fileName)
		assert.NoError(t, err)

		defer file.Close()
		defer os.Remove(file.Name())

		// Write some data to the file
		_, err = file.Write(tt.fileData)
		assert.NoError(t, err)

		// Setup the file data
		fileName := expression.NewLiteral(file.Name(), sql.Text)
		fn := NewLoadFile(sql.NewEmptyContext(), fileName)

		// Load the file in
		res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
		assert.NoError(t, err)
		assert.Equal(t, tt.fileData, res)
	}
}
