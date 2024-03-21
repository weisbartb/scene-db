package mysql_test

import (
	_ "embed"
	"github.com/stretchr/testify/require"
	"github.com/weisbartb/scene-db/mysql"
	"os"
	"path"
	"testing"
)

//go:embed internal/test/mysql.cert
var mysqlCert string

func TestDefaultMySQLCfg(t *testing.T) {
	cfg := mysql.DefaultMySQLCfg()
	require.Equal(t, "root", cfg.DatabaseUserName)
	require.Equal(t, "test", cfg.DatabasePassword)
	require.Equal(t, "localhost", cfg.DatabaseHost)
	require.Equal(t, "3306", cfg.DatabasePort)
	require.Equal(t, "utf8mb4", cfg.DatabaseCharSet)
	require.Equal(t, "16777216", cfg.DatabaseMaxPacket)
	require.Equal(t, "", cfg.CABundle)
	require.Equal(t, false, cfg.TLSEnabled)
	require.Equal(t, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES", cfg.SQLMode)
	require.Equal(t, "REPEATABLE-READ", cfg.TXNIsolation)
}

func TestMySQLConfig_Validate(t *testing.T) {
	cfg := mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabaseHost = ""
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidDatabaseHost)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabaseHost = "donotresolve.localhost"
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrUnreachableDatabaseHost)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabasePort = ""
	require.NoError(t, cfg.Validate(nil))
	require.Equal(t, "3306", cfg.DatabasePort)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabasePort = "lol"
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidDatabasePort)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabasePassword = ""
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidDatabasePassword)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = ""
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidDatabaseSchemaName)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabaseUserName = ""
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidDatabaseUserName)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.DatabaseCharSet = ""
	require.NoError(t, cfg.Validate(nil))
	require.Equal(t, "utf8mb4,utf8", cfg.DatabaseCharSet)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.TLSEnabled = true
	cfg.CABundle = ""
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrNoCABundleProvided)
	cfg = mysql.DefaultMySQLCfg()
	cfg.DatabaseSchemaName = "test"
	cfg.TLSEnabled = true
	cfg.CABundle = path.Join(os.TempDir(), "this-file-should-not-exist")
	require.ErrorIs(t, cfg.Validate(nil), mysql.ErrInvalidCABundleProvided)
}

func TestMySQLConfig_SetupTLS(t *testing.T) {

	t.Run("good", func(t *testing.T) {
		cfg := mysql.DefaultMySQLCfg()
		cfg.TLSEnabled = true
		cfg.DatabaseSchemaName = "test"
		cfg.CABundle = "./internal/test/mysql.cert"
		err := cfg.Validate(func() ([]byte, error) {
			return []byte(mysqlCert), nil
		})
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		cfg := mysql.DefaultMySQLCfg()
		cfg.TLSEnabled = true
		cfg.DatabaseSchemaName = "test"
		cfg.CABundle = "./internal/test/mysql.cert"
		err := cfg.Validate(func() ([]byte, error) {
			return []byte("test"), nil
		})
		require.ErrorIs(t, err, mysql.ErrInvalidCABundleProvided)
	})

}
