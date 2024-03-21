package mysql_test

import (
	"context"
	"errors"
	"github.com/weisbartb/scene-db/mysql"
	"github.com/weisbartb/scene-db/mysql/internal"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupInstance(t *testing.T) *mysql.Instance {
	db, shutdown := internal.InitializeTestDB(t, false)
	t.Cleanup(func() {
		shutdown()
	})
	return mysql.NewInstance(context.Background(), db)
}

func TestNewInstance(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, true)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	require.NoError(t, instance.Ping())
}

func TestInstance_Query(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, false)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	var tmp int64
	_, err := instance.Exec(`INSERT INTO test_table VALUES ();`)
	require.NoError(t, err)
	rows, err := instance.Query("SELECT * FROM test_table")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	row := instance.QueryRow("SELECT * FROM test_table")
	require.NoError(t, row.Scan(&tmp))
	rowX := instance.QueryRowx("SELECT * FROM test_table")
	require.NoError(t, rowX.Scan(&tmp))
	rowsX, err := instance.Queryx("SELECT * FROM test_table")
	require.NoError(t, err)
	require.NoError(t, rowsX.Close())
}
func TestInstance_BeginTx(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, true)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	require.False(t, instance.InTx())
	require.NoError(t, instance.BeginTx(nil))
	require.True(t, instance.InTx())
	require.EqualError(t, instance.BeginTx(nil), mysql.ErrTransactionAlreadyStarted.Error())
	require.NoError(t, instance.Rollback())
	require.NoError(t, instance.BeginTx(nil))
	require.NoError(t, instance.Commit())
	require.NoError(t, instance.BeginTx(nil))
	require.Empty(t, instance.Close())
	require.Empty(t, instance.Close())
}

func TestIter_For(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, false)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	_, err := instance.Exec(`INSERT INTO test_table VALUES (),(),(),(),();`)
	require.NoError(t, err)
	iter := instance.QueryFor("SELECT id from test_table")
	require.NoError(t, iter.For(func(row mysql.Scannable) error {
		var id int
		require.NoError(t, row.Scan(&id))
		return nil
	}))
	iter = instance.QueryFor("SELECT id from test_table")
	require.EqualError(t, iter.For(func(row mysql.Scannable) error {
		var id int
		require.NoError(t, row.Scan(&id))
		return errors.New("hi")
	}), "hi")
	iter = instance.QueryFor("SELECT d from test_table")
	require.EqualError(t, iter.For(func(row mysql.Scannable) error {
		var id int
		require.NoError(t, row.Scan(&id))
		return errors.New("hi")
	}), "Error 1054 (42S22): Unknown column 'd' in 'field list'")
	iter = instance.QueryFor("SELECT id from test_table")
	require.EqualError(t, iter.For(func(row mysql.Scannable) error {
		var id []string
		return row.Scan(&id)
	}), `sql: Scan error on column index 0, name "id": unsupported Scan, storing driver.Value type int64 into type *[]string`)
}

func TestInstance_Raw(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, true)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	require.Equal(t, db, instance.Raw())
}

func TestInstance_DriverName(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, true)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	require.Equal(t, instance.DriverName(), "mysql")
}
func TestInstance_Rebind(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, true)
	defer shutdown()
	require.NotNil(t, db)
	instance := mysql.NewInstance(context.Background(), db)
	require.Equal(t, instance.Rebind("INSERT INTO test (id) (?)"), "INSERT INTO test (id) (?)")
}

func TestInstance_SpawnChild(t *testing.T) {
	instance := setupInstance(t)
	_, err := instance.Exec("INSERT INTO test_kvp (`key`,`val`) VALUES('test','test')")
	require.NoError(t, err)
	child := instance.SpawnChild()
	_, err = child.Query("SELECT `key`, val FROM test_kvp")
	require.NoError(t, err)
	require.Equal(t, 1, instance.Raw().Stats().InUse)
	errs := instance.Close()
	require.Nil(t, errs)
	require.Equal(t, 0, instance.Raw().Stats().InUse)
}

func TestInstance_QueryFor(t *testing.T) {
	instance := setupInstance(t)
	_, err := instance.Exec("INSERT INTO test_kvp (`key`,`val`) VALUES('test','test'),('test2','test2')")
	statement := "SELECT `key`,`val` FROM test_kvp"
	require.NoError(t, err)
	err = instance.QueryFor(statement).For(func(row mysql.Scannable) error {
		var v1, v2 string
		err = row.Scan(&v1, &v2)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
	rows, _ := instance.Query(statement)
	err = instance.QueryFor(statement).For(func(row mysql.Scannable) error {
		var v1, v2 string
		err = row.Scan(&v1, &v2)
		require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
		return err
	})
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	rows.Close()
	err = instance.QueryFor(statement).For(func(row mysql.Scannable) error {
		var v1, v2 string
		err = row.Scan(&v1, &v2)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

}

func TestInstance_RequireTx(t *testing.T) {
	instance := setupInstance(t)
	instance.RequireTx(func(db *mysql.Instance) error {
		_, err := instance.Exec("INSERT INTO test_kvp (`key`,`val`) VALUES('test','test'),('test2','test2')")
		require.NoError(t, err)
		statement := "SELECT count(*) FROM test_kvp"
		var ct int
		require.NoError(t, db.QueryRow(statement).Scan(&ct))
		require.Equal(t, 2, ct)
		instance2 := db.Isolate()
		require.NoError(t, instance2.QueryRow(statement).Scan(&ct))
		require.Equal(t, 0, ct)
		return nil
	})
}

func TestInstance_PartialCommit(t *testing.T) {
	instance := setupInstance(t)
	instance.RequireTx(func(db *mysql.Instance) error {
		_, err := instance.Exec("INSERT INTO test_kvp (`key`,`val`) VALUES('test','test'),('test2','test2')")
		require.NoError(t, err)
		statement := "SELECT count(*) FROM test_kvp"
		var ct int
		require.NoError(t, db.QueryRow(statement).Scan(&ct))
		require.Equal(t, 2, ct)
		require.NoError(t, db.PartialCommit())
		instance2 := db.Isolate()
		require.NoError(t, instance2.QueryRow(statement).Scan(&ct))
		require.Equal(t, 2, ct)
		return nil
	})
	require.ErrorIs(t, instance.PartialCommit(), mysql.ErrNoActiveTransaction)
}

func TestInstance_CloseDetection(t *testing.T) {
	instance := setupInstance(t)
	_, err := instance.Exec("INSERT INTO test_kvp (`key`,`val`) VALUES('test','test'),('test2','test2')")
	require.NoError(t, err)
	statement := "SELECT *FROM test_kvp"
	rows, err := instance.Queryx(statement)
	require.NoError(t, err)
	_, err = instance.Queryx(statement)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	_, err = instance.Query(statement)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	var ct int
	resp := instance.QueryRow("SELECT count(*) FROM test_kvp")
	err = resp.Scan(&ct)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	err = resp.Err()
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	xResp := instance.QueryRowx("SELECT count(*) FROM test_kvp")
	err = xResp.Scan(&ct)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	_, err = xResp.SliceScan()
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	_, err = xResp.Columns()
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	_, err = xResp.ColumnTypes()
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	err = xResp.MapScan(nil)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	err = xResp.StructScan(nil)
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	err = xResp.Err()
	require.ErrorIs(t, err, mysql.ErrRowsNotClosed)
	rows.Close()

}
