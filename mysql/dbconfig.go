package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
)

var tlsIDCounter int32

// Section for various errors from an invalid configuration that is provided

var ErrInvalidDatabaseUserName = errors.New("invalid database username provided")
var ErrInvalidDatabasePassword = errors.New("invalid database password provided")
var ErrInvalidDatabaseHost = errors.New("invalid database host provided")
var ErrUnreachableDatabaseHost = errors.New("unreachable database host provided")
var ErrInvalidDatabasePort = errors.New("invalid database port provided")
var ErrInvalidDatabaseSchemaName = errors.New("invalid database schema name provided")
var ErrNoCABundleProvided = errors.New("TLS requires a CA bundle")
var ErrInvalidCABundleProvided = errors.New("TLS requires a valid CA bundle")

type NestedMySQLConfigWrapper struct {
	Cfg MySQLConfig `toml:"database" json:"database" yaml:"database,flow"`
}

// Configuration wrapper for the database
type MySQLConfig struct {
	// The database user name
	DatabaseUserName string `toml:"username" json:"username" yaml:"username"`
	// The database password
	DatabasePassword string `toml:"password" json:"password" yaml:"password"`
	// The database hostname
	DatabaseHost string `toml:"host" json:"host" yaml:"host"`
	// The database port (will default to 3306 if left blank)
	DatabasePort string `toml:"port" json:"port" yaml:"port"`
	// The database schema name (the actual database name)
	DatabaseSchemaName string `toml:"schema" json:"schema" yaml:"schema"`
	// The data character set that the database uses (defaults to UTF8MB4)
	DatabaseCharSet string `toml:"charset" json:"charset,omitempty" yaml:"charset,omitempty"`
	// The maximum datapacket size (in bytes) that cane be sent to the database (defaults to 16M)
	DatabaseMaxPacket string `toml:"maxpacket" json:"maxpacket,omitempty" yaml:"maxpacket,omitempty"`
	// CA Bundle required to validate against
	CABundle string `toml:"caBundle" json:"caBundle,omitempty" yaml:"caBundle,omitempty"`
	// Is TLS enabled?
	TLSEnabled bool `toml:"tlsEnabled" json:"tlsEnabled,omitempty" yaml:"tlsEnabled,omitempty"`
	// What is the SQL mode - defaults to ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
	SQLMode string `toml:"sqlMode" json:"sqlMode,omitempty" yaml:"sqlMode,omitempty"`
	// What is the transaction level - defaults to REPEATABLE-READ
	TXNIsolation string ` toml:"txnIsolation" json:"txnIsolation,omitempty" yaml:"txnIsolation,omitempty"`
	// The TLS extension id that was registered
	tlsID string
}

func DefaultMySQLCfg() MySQLConfig {
	return MySQLConfig{
		DatabaseUserName:   "root",
		DatabasePassword:   "test",
		DatabaseHost:       "localhost",
		DatabasePort:       "3306",
		DatabaseSchemaName: "",
		DatabaseCharSet:    "utf8mb4",
		DatabaseMaxPacket:  "16777216", // 16M
		CABundle:           "",
		TLSEnabled:         false,
		SQLMode:            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES",
		TXNIsolation:       "REPEATABLE-READ",
	}
}

func (dbCfg *MySQLConfig) Validate(getPem func() ([]byte, error)) error {
	if len(dbCfg.DatabaseHost) == 0 {
		return ErrInvalidDatabaseHost
	}
	_, err := net.LookupHost(dbCfg.DatabaseHost)
	if err != nil {
		return ErrUnreachableDatabaseHost
	}
	if len(dbCfg.DatabasePort) == 0 {
		dbCfg.DatabasePort = "3306"
	}
	if matched, err := regexp.MatchString(`^\d+$`, dbCfg.DatabasePort); err != nil || !matched {
		return ErrInvalidDatabasePort
	}
	if len(dbCfg.DatabasePassword) == 0 {
		return ErrInvalidDatabasePassword
	}
	if len(dbCfg.DatabaseSchemaName) == 0 {
		return ErrInvalidDatabaseSchemaName
	}
	if len(dbCfg.DatabaseUserName) == 0 {
		return ErrInvalidDatabaseUserName
	}
	if len(dbCfg.DatabaseCharSet) == 0 {
		dbCfg.DatabaseCharSet = "utf8mb4,utf8"
	}
	if dbCfg.TLSEnabled {
		if err = dbCfg.setupTLS(getPem); err != nil {
			return err
		}
	}
	return nil
}

// BuildDSN creates the DSN string for the driver
func (dbCfg MySQLConfig) BuildDSN() string {
	var arguments = []string{
		"parseTime=true",
		// These values need to be url-escape quoted to work correctly
		// Scroll to the bottom of the mysql driver github page
		/**
		System Variables

		Any other parameters are interpreted as system variables:

		    <boolean_var>=<value>: SET <boolean_var>=<value>
		    <enum_var>=<value>: SET <enum_var>=<value>
		    <string_var>=%27<value>%27: SET <string_var>='<value>'

		Rules:

		    The values for string variables must be quoted with '.
		    The values must also be url.QueryEscape'ed! (which implies values of string variables must be wrapped with %27).
		*/
		"sql_mode=%27" + dbCfg.SQLMode + "%27",
		"tx_isolation=%27" + dbCfg.TXNIsolation + "%27",
		"maxAllowedPacket=" + dbCfg.DatabaseMaxPacket,
		"interpolateParams=true",
		"loc=UTC",
		"charset=" + dbCfg.DatabaseCharSet,
	}
	if dbCfg.TLSEnabled {
		arguments = append(arguments, "tls="+dbCfg.tlsID)
		return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?%v", dbCfg.DatabaseUserName, dbCfg.DatabasePassword, dbCfg.DatabaseHost, dbCfg.DatabasePort, dbCfg.DatabaseSchemaName, strings.Join(arguments, "&"))
	}
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?%v", dbCfg.DatabaseUserName, dbCfg.DatabasePassword, dbCfg.DatabaseHost, dbCfg.DatabasePort, dbCfg.DatabaseSchemaName, strings.Join(arguments, "&"))
}

// setupTLS configures TLS for the driver.
// Optionally, a getPem function can be specified, if present, it will override it loading from the disk location in the config.
func (dbCfg *MySQLConfig) setupTLS(getPem func() ([]byte, error)) error {
	if len(dbCfg.tlsID) > 0 {
		return nil
	}
	if len(dbCfg.CABundle) == 0 && getPem == nil {
		return ErrNoCABundleProvided
	}
	certPool := x509.NewCertPool()
	var pem []byte
	var err error
	if getPem == nil {
		pem, err = os.ReadFile(dbCfg.CABundle)
	} else {
		pem, err = getPem()
	}
	if err != nil {
		return ErrInvalidCABundleProvided
	}
	if ok := certPool.AppendCertsFromPEM(pem); !ok {
		return ErrInvalidCABundleProvided
	}

	id := atomic.AddInt32(&tlsIDCounter, 1)
	dbCfg.tlsID = "mysql-tls-" + strconv.Itoa(int(id))
	if err = mysql.RegisterTLSConfig(dbCfg.tlsID, &tls.Config{
		ServerName: dbCfg.DatabaseHost,
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}); err != nil {
		return err
	}

	return nil
}
