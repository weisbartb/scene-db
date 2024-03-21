package mysql_test

import (
	"database/sql"
	"errors"
	"fmt"
	md "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/weisbartb/scene-db/mysql"
	"testing"
)

func TestGetErrorCode(t *testing.T) {
	err := &md.MySQLError{
		Number:   mysql.ErrCodeDuplicateKey,
		SQLState: [5]byte{},
		Message:  fmt.Sprintf(`Duplicate entry '%v' for key '(%v)`, "conflict", "test"),
	}
	require.Equal(t, mysql.ErrCodeDuplicateKey, mysql.GetErrorCode(err))
	require.Equal(t, uint16(0), mysql.GetErrorCode(errors.New("test")))
}

func TestIsDuplicateKeyError(t *testing.T) {
	err := &md.MySQLError{
		Number:   mysql.ErrCodeDuplicateKey,
		SQLState: [5]byte{},
		Message:  fmt.Sprintf(`Duplicate entry '%v' for key '(%v)`, "conflict", "test"),
	}
	require.Equal(t, true, mysql.IsDuplicateKeyError(err))
	require.Equal(t, false, mysql.IsDuplicateKeyError(errors.New("test")))
}

func TestIsNoRows(t *testing.T) {
	require.Equal(t, true, mysql.IsNoRows(sql.ErrNoRows))
	require.Equal(t, false, mysql.IsNoRows(errors.New("test")))
}

func TestIsDeadlocked(t *testing.T) {
	err := &md.MySQLError{
		Number:   mysql.ErrCodeDeadlockCode,
		SQLState: [5]byte{},
		Message:  "",
	}
	require.Equal(t, true, mysql.IsDeadlocked(err))
	require.Equal(t, false, mysql.IsDeadlocked(errors.New("test")))
}
