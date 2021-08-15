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
	dir, file, err := createTempDirAndFile("myfile.txt")
	assert.NoError(t, err)

	defer os.RemoveAll(dir)

	fileName := expression.NewLiteral(file.Name(), sql.Text)
	fn := NewLoadFile(sql.NewEmptyContext(), fileName)

	// Assert that Load File returns nil since we never set secure file priv
	res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
}

func TestLoadFile(t *testing.T) {
	testCases := []struct {
		name      string
		fileData  []byte
		expectNil bool
	}{
		{
			"simple example",
			[]byte("important test case"),
			false,
		},
		{
			"blob",
			[]byte("\\xFF\\xD8\\xFF\\xE1\\x00"),
			false,
		},
	}

	for _, tt := range testCases {
		// Create a valid temp file and temp directory
		dir, file, err := createTempDirAndFile("myfile.txt")
		assert.NoError(t, err)

		defer os.RemoveAll(dir)

		// Set the secure_file_priv var
		vars := make(map[string]interface{})
		vars["secure_file_priv"] = dir
		err = sql.SystemVariables.AssignValues(vars)
		assert.NoError(t, err)

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
