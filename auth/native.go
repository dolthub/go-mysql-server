package auth

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"gopkg.in/src-d/go-vitess.v1/mysql"
)

var regNative = regexp.MustCompile(`^\*[0-9A-F]{40}$`)

// ErrUnknownPermission happens when a user permission is not defined.
const ErrUnknownPermission = "error parsing user file, unknown permission %s"

// nativeUser holds information about credentials and permissions for user.
type nativeUser struct {
	Name            string
	Password        string
	JSONPermissions []string `json:"Permissions"`
	Permissions     Permission
}

// Allowed checks if the user has certain permission.
func (u nativeUser) Allowed(p Permission) error {
	if u.Permissions&p == p {
		return nil
	}

	// permissions needed but not granted to the user
	p2 := (^u.Permissions) & p

	return fmt.Errorf(ErrNoPermission, p2)
}

// NativePassword generates a mysql_native_password string.
func NativePassword(password string) string {
	if len(password) == 0 {
		return ""
	}

	// native = sha1(sha1(password))

	hash := sha1.New()
	hash.Write([]byte(password))
	s1 := hash.Sum(nil)

	hash.Reset()
	hash.Write(s1)
	s2 := hash.Sum(nil)

	s := strings.ToUpper(hex.EncodeToString(s2))

	return fmt.Sprintf("*%s", s)
}

// Native holds mysql_native_password users.
type Native struct {
	users map[string]nativeUser
}

// NewNativeSingle creates a NativeAuth with a single user.
func NewNativeSingle(name, password string) *Native {
	users := make(map[string]nativeUser)
	users[name] = nativeUser{
		Name:        name,
		Password:    NativePassword(password),
		Permissions: AllPermissions,
	}

	return &Native{users}
}

// NewNativeFile creates a NativeAuth and loads users from a JSON file.
func NewNativeFile(file string) (*Native, error) {
	var data []nativeUser

	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	users := make(map[string]nativeUser)
	for _, u := range data {
		_, ok := users[u.Name]
		if ok {
			return nil, fmt.Errorf("duplicate user: %s", u.Name)
		}

		if !regNative.MatchString(u.Password) {
			u.Password = NativePassword(u.Password)
		}

		if len(u.JSONPermissions) == 0 {
			u.Permissions = DefaultPermissions
		}

		for _, p := range u.JSONPermissions {
			perm, ok := PermissionNames[strings.ToLower(p)]
			if !ok {
				return nil, fmt.Errorf(ErrUnknownPermission, p)
			}

			u.Permissions |= perm
		}

		users[u.Name] = u
	}

	return &Native{users}, nil
}

// Mysql implements Auth interface.
func (s *Native) Mysql() mysql.AuthServer {
	auth := mysql.NewAuthServerStatic()

	for k, v := range s.users {
		auth.Entries[k] = []*mysql.AuthServerStaticEntry{
			{MysqlNativePassword: v.Password},
		}
	}

	return auth
}

// Allowed implements Auth interface.
func (s *Native) Allowed(name string, permission Permission) error {
	u, ok := s.users[name]
	if !ok {
		return fmt.Errorf(ErrNoPermission, permission)
	}

	return u.Allowed(permission)
}
