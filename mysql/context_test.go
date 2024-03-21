package mysql_test

import (
	"github.com/weisbartb/scene"
	"github.com/weisbartb/scene-db/mysql"
	"github.com/weisbartb/scene-db/mysql/internal"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/weisbartb/tsbuffer"
)

func must[t any](val t, err error) t {
	if err != nil {
		panic(err)
	}
	return val
}
func TestProvider(t *testing.T) {
	buf := tsbuffer.New()
	logger := internal.LogWrapper{Logger: zerolog.New(buf)}
	db, shutdown := internal.InitializeTestDB(t, true)
	t.Cleanup(func() {
		shutdown()
	})
	require.NotNil(t, db)

	factory, err := scene.NewSceneFactory(scene.Config{
		MaxTTL:    0,
		LogOutput: logger.Logger,
	}, must(mysql.NewSceneProvider(internal.GetTestDatabaseConfiguration(), logger)))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.True(t, factory.Shutdown(time.Millisecond*100))
	})
	ctx, _ := factory.NewCtx()
	defer ctx.Complete()
	require.NotNil(t, mysql.GetManagedDatabaseInstance(ctx))
}

func TestProviderAllocation(t *testing.T) {
	db, shutdown := internal.InitializeTestDB(t, false)
	defer shutdown()
	require.NotNil(t, db)
	provider := mysql.Provider{
		DB: db,
	}
	db.SetMaxOpenConns(50)
	factory, err := scene.NewSceneFactory(scene.Config{MaxTTL: time.Second * 4}, provider)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func() {
			ctx, _ := factory.NewCtx()
			defer func() {
				ctx.Complete()
				wg.Done()
			}()
			require.NoError(t, mysql.GetManagedDatabaseInstance(ctx).BeginTx(nil))
			var tmp int
			require.NoError(t, mysql.GetManagedDatabaseInstance(ctx).QueryRow("SELECT count(*) FROM `test_kvp`").Scan(&tmp))
			require.NoError(t, mysql.GetManagedDatabaseInstance(ctx).Rollback())
		}()
	}
	wg.Wait()
	require.Equal(t, 0, db.Stats().InUse)
}
