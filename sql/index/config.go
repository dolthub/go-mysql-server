package index

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	yaml "gopkg.in/yaml.v2"
)

const (
	// ConfigFileName name of an index config file.
	ConfigFileName = "config.yml"
)

// Config represents index configuration
type Config struct {
	DB          string
	Table       string
	ID          string
	Expressions []string
	Drivers     map[string]map[string]string
}

// NewConfig creates a new Config instance for given driver's configuration
func NewConfig(db, table, id string,
	expressionHashes []sql.ExpressionHash,
	driverID string,
	driverConfig map[string]string) *Config {

	expressions := make([]string, len(expressionHashes))

	for i, h := range expressionHashes {
		expressions[i] = sql.EncodeExpressionHash(h)
	}

	cfg := &Config{
		DB:          db,
		Table:       table,
		ID:          id,
		Expressions: expressions,
		Drivers:     make(map[string]map[string]string),
	}
	cfg.Drivers[driverID] = driverConfig

	return cfg
}

// ExpressionHashes returns a slice of ExpressionHash for this configuration.
// Implementation decodes hex strings into byte slices.
func (cfg *Config) ExpressionHashes() []sql.ExpressionHash {
	h := make([]sql.ExpressionHash, len(cfg.Expressions))
	for i, hexstr := range cfg.Expressions {
		h[i], _ = sql.DecodeExpressionHash(hexstr)
	}
	return h
}

// Driver returns an configuration for the particular driverID.
func (cfg *Config) Driver(driverID string) map[string]string {
	return cfg.Drivers[driverID]
}

// WriteConfig writes the configuration to the passed writer (w).
func WriteConfig(w io.Writer, cfg *Config) error {
	data, err := yaml.Marshal(cfg)

	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// WriteConfigFile writes the configuration to dir/config.yml file.
func WriteConfigFile(dir string, cfg *Config) error {
	path := filepath.Join(dir, ConfigFileName)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return WriteConfig(f, cfg)
}

// ReadConfig reads an configuration from the passed reader (r).
func ReadConfig(r io.Reader) (*Config, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	return &cfg, err
}

// ReadConfigFile reads an configuration from dir/config.yml file.
func ReadConfigFile(dir string) (*Config, error) {
	path := filepath.Join(dir, ConfigFileName)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadConfig(f)
}
