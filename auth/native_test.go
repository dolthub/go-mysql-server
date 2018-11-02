package auth_test

import (
	"io/ioutil"
	"os"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/auth"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
)

const (
	baseConfig = `
[
	{
		"name": "root",
		"password": "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F",
		"permissions": ["read", "write"]
	},
	{
		"name": "user",
		"password": "password",
		"permissions": ["read"]
	},
	{
		"name": "no_password"
	},
	{
		"name": "empty_password",
		"password": ""
	},
	{
		"name": "no_permissions",
		"permissions": []
	}
]`
	duplicateUser = `
[
	{ "name": "user" },
	{ "name": "user" }
]`
	badPermission = `
[
	{ "permissions": ["read", "write", "admin"] }
]`
	badJSON = "I,am{not}JSON"
)

func writeConfig(config string) (string, error) {
	tmp, err := ioutil.TempFile("", "native-config")
	if err != nil {
		return "", err
	}

	_, err = tmp.WriteString(config)
	if err != nil {
		os.Remove(tmp.Name())
		return "", err
	}

	return tmp.Name(), nil
}

var nativeSingleTests = []authenticationTest{
	{"root", "", false},
	{"root", "password", false},
	{"root", "mysql_password", false},
	{"user", "password", true},
	{"user", "other_password", false},
	{"user", "", false},
	{"", "", false},
	{"", "password", false},
}

func TestNativeAuthenticationSingle(t *testing.T) {
	a := auth.NewNativeSingle("user", "password", auth.AllPermissions)
	testAuthentication(t, a, nativeSingleTests, nil)
}

func TestNativeAuthentication(t *testing.T) {
	req := require.New(t)

	conf, err := writeConfig(baseConfig)
	req.NoError(err)
	defer os.Remove(conf)

	a, err := auth.NewNativeFile(conf)
	req.NoError(err)

	tests := []authenticationTest{
		{"root", "", false},
		{"root", "password", false},
		{"root", "mysql_password", true},
		{"user", "password", true},
		{"user", "other_password", false},
		{"user", "", false},
		{"no_password", "", true},
		{"no_password", "password", false},
		{"empty_password", "", true},
		{"empty_password", "password", false},
		{"nonexistent", "", false},
		{"nonexistent", "password", false},
	}

	testAuthentication(t, a, tests, nil)
}

func TestNativeAuthorizationSingleAll(t *testing.T) {
	a := auth.NewNativeSingle("user", "password", auth.AllPermissions)

	tests := []authorizationTest{
		{"user", queries["select"], true},
		{"root", queries["select"], false},
		{"", queries["select"], false},

		{"user", queries["create_index"], true},
		{"root", queries["create_index"], false},
		{"", queries["create_index"], false},

		{"user", queries["drop_index"], true},
		{"root", queries["drop_index"], false},
		{"", queries["drop_index"], false},

		{"user", queries["insert"], true},
		{"root", queries["insert"], false},
		{"", queries["insert"], false},

		{"user", queries["lock"], true},
		{"root", queries["lock"], false},
		{"", queries["lock"], false},

		{"user", queries["unlock"], true},
		{"root", queries["unlock"], false},
		{"", queries["unlock"], false},
	}

	testAuthorization(t, a, tests, nil)
}

func TestNativeAuthorizationSingleRead(t *testing.T) {
	a := auth.NewNativeSingle("user", "password", auth.ReadPerm)

	tests := []authorizationTest{
		{"user", queries["select"], true},
		{"root", queries["select"], false},
		{"", queries["select"], false},

		{"user", queries["create_index"], false},
		{"root", queries["create_index"], false},
		{"", queries["create_index"], false},

		{"user", queries["drop_index"], false},
		{"root", queries["drop_index"], false},
		{"", queries["drop_index"], false},

		{"user", queries["insert"], false},
		{"root", queries["insert"], false},
		{"", queries["insert"], false},

		{"user", queries["lock"], false},
		{"root", queries["lock"], false},
		{"", queries["lock"], false},

		{"user", queries["unlock"], false},
		{"root", queries["unlock"], false},
		{"", queries["unlock"], false},
	}

	testAuthorization(t, a, tests, nil)
}

func TestNativeAuthorization(t *testing.T) {
	require := require.New(t)

	conf, err := writeConfig(baseConfig)
	require.NoError(err)
	defer os.Remove(conf)

	a, err := auth.NewNativeFile(conf)
	require.NoError(err)

	tests := []authorizationTest{
		{"", queries["select"], false},
		{"user", queries["select"], true},
		{"no_password", queries["select"], true},
		{"no_permissions", queries["select"], true},
		{"root", queries["select"], true},

		{"", queries["create_index"], false},
		{"user", queries["create_index"], false},
		{"no_password", queries["create_index"], false},
		{"no_permissions", queries["create_index"], false},
		{"root", queries["create_index"], true},

		{"", queries["drop_index"], false},
		{"user", queries["drop_index"], false},
		{"no_password", queries["drop_index"], false},
		{"no_permissions", queries["drop_index"], false},
		{"root", queries["drop_index"], true},

		{"", queries["insert"], false},
		{"user", queries["insert"], false},
		{"no_password", queries["insert"], false},
		{"no_permissions", queries["insert"], false},
		{"root", queries["insert"], true},

		{"", queries["lock"], false},
		{"user", queries["lock"], false},
		{"no_password", queries["lock"], false},
		{"no_permissions", queries["lock"], false},
		{"root", queries["lock"], true},

		{"", queries["unlock"], false},
		{"user", queries["unlock"], false},
		{"no_password", queries["unlock"], false},
		{"no_permissions", queries["unlock"], false},
		{"root", queries["unlock"], true},
	}

	testAuthorization(t, a, tests, nil)
}

func TestNativeErrors(t *testing.T) {
	tests := []struct {
		name   string
		config string
		err    *errors.Kind
	}{
		{"duplicate_user", duplicateUser, auth.ErrDuplicateUser},
		{"bad_permission", badPermission, auth.ErrUnknownPermission},
		{"malformed", badJSON, auth.ErrParseUserFile},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			conf, err := writeConfig(c.config)
			require.NoError(err)
			defer os.Remove(conf)

			_, err = auth.NewNativeFile(conf)
			require.Error(err)
			require.True(c.err.Is(err))
		})
	}
}
