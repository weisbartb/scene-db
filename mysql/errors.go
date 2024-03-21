package mysql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

const (
	ErrCodeDuplicateKey uint16 = 1062
	ErrCodeDeadlockCode uint16 = 1213
)

func getMySQLError(err error) *mysql.MySQLError {
	var out *mysql.MySQLError
	if errors.As(err, &out) {
		return out
	}
	return nil
}

func GetErrorCode(err error) uint16 {
	if mysqlErr := getMySQLError(err); mysqlErr != nil {
		return mysqlErr.Number
	}
	return 0
}

func IsDuplicateKeyError(err error) bool {
	if mysqlErr := getMySQLError(err); mysqlErr != nil {
		return mysqlErr.Number == ErrCodeDuplicateKey
	}
	return false
}
func IsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
func IsDeadlocked(err error) bool {
	if mysqlErr := getMySQLError(err); mysqlErr != nil {
		return mysqlErr.Number == ErrCodeDeadlockCode
	}
	return false
}

func respErrorHandler(err error) error {
	if err == nil {
		return nil
	}
	if mErr := getMySQLError(err); mErr != nil {
		return mErr
	}
	return err
}
