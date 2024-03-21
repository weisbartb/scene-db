package internal

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/weisbartb/scene-db/mysql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const maxTestDepth = 10

func getSchema() ([]byte, error) {
	curr, err := filepath.Abs(".")
	if err != nil {
		return nil, errors.Wrap(err, "getting absolute path for working directory")
	}

	for i := 0; i < maxTestDepth; i++ {
		_, err := os.Stat(filepath.Join(curr, "go.mod"))
		if err != nil {
			if os.IsNotExist(err) {
				curr = filepath.Join(curr, "..")
				continue
			}
			return nil, errors.Wrap(err, "error finding go.mod")
		}
		buffer := bytes.Buffer{}
		schema, err := os.ReadFile(filepath.Join(curr, "internal/db", "schema.sql"))
		if err != nil {
			return nil, errors.Wrap(err, "get schema")
		}
		buffer.Write(bytes.TrimSpace(schema))

		return buffer.Bytes(), nil
	}

	return nil, errors.New("go.mod not found in relative directory")
}

func GetTestDatabaseConfiguration() mysql.MySQLConfig {
	host, ok := os.LookupEnv("MYSQL_HOST")
	if !ok {
		host = "127.0.0.1"
	}
	port, ok := os.LookupEnv("MYSQL_PORT")
	if !ok {
		port = "3306"
	}
	user, ok := os.LookupEnv("MYSQL_USER")
	if !ok {
		user = "root"
	}
	pw, ok := os.LookupEnv("MYSQL_PASSWORD")
	if !ok {
		// MySQL generally won't let you set up root w/o a password
		pw = "test"
	}
	cfg := mysql.DefaultMySQLCfg()
	cfg.DatabaseHost = host
	cfg.DatabasePort = port
	cfg.DatabaseUserName = user
	cfg.DatabasePassword = pw
	cfg.DatabaseSchemaName = "mysql"
	return cfg
}

// InitializeTestDB will create a database used for testing
func InitializeTestDB(tb testing.TB, empty bool) (*sqlx.DB, func()) {
	tb.Helper()
	cfg := GetTestDatabaseConfiguration()
	ctx := context.Background()
	dbInstance, err := sqlx.Connect(
		"mysql",
		cfg.BuildDSN(),
	)

	if err != nil {
		tb.Fatal("unable to connect to DB (did you start it before running tests?):", err)
	}
	dbConnection, err := dbInstance.Conn(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	// Generate random DB name
	dbName := fmt.Sprintf("test_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	cfg.DatabaseSchemaName = dbName
	//goland:noinspection SyntaxError
	_, err = dbConnection.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName))
	if err != nil {
		tb.Fatal(err)
	}
	_, err = dbConnection.ExecContext(ctx, fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		tb.Fatal(err)
	}
	if !empty {
		schema, err := getSchema()
		if err != nil {
			tb.Fatalf("unable to get schema file: %+v", err)
		}
		if err = runMySQLBulkCommands(dbConnection, schema); err != nil {
			tb.Fatal(err)
		}
	}
	dbInstance, err = sqlx.Connect(
		"mysql",
		cfg.BuildDSN(),
	)
	if err != nil {
		tb.Fatal(err)
	}
	if err = dbInstance.Ping(); err != nil {
		tb.Fatal(err)
	}
	dbInstance.SetMaxOpenConns(100)
	dbInstance.SetConnMaxLifetime(time.Second * 60 * 15)
	dbInstance.SetMaxIdleConns(50)
	return dbInstance, func() {
		_, err = dbConnection.ExecContext(ctx, "USE mysql;")
		if err != nil {
			tb.Fatal(err)
		}
		_, err = dbConnection.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", dbName))
		if err != nil {
			tb.Fatal(err)
		}
		_ = dbInstance.Close()
	}
}

// runSqlScripts executes the provided SQL source file
func runMySQLBulkCommands(w *sql.Conn, data []byte) error {
	// Split on all semicolons
	for _, splitData := range bytes.Split(data, []byte{';'}) {
		splitData = bytes.TrimSpace(splitData)
		if len(splitData) == 0 {
			continue
		}
		buf := bytes.Buffer{}
		// Split by line
		for _, line := range bytes.Split(splitData, []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			if line[0] == '#' {
				continue
			}
			switch {
			case bytes.HasPrefix(bytes.ToUpper(line), []byte("USE ")):
				continue
			case bytes.HasPrefix(bytes.ToUpper(line), []byte("CREATE DATABASE ")):
				continue
			}
			buf.Write(line)
			buf.WriteByte('\n')
		}
		if buf.Len() == 0 {
			continue
		}
		if _, err := w.ExecContext(context.Background(), buf.String()); err != nil {
			return err
		}
	}
	return nil
}

type LogWrapper struct {
	zerolog.Logger
}

func (l LogWrapper) Errorf(format string, v ...interface{}) {
	l.Error().Msgf(format, v...)
}
