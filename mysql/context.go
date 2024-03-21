package mysql

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/weisbartb/deadline-wg"
	"github.com/weisbartb/scene"
	"github.com/weisbartb/stack"
)

type logger interface {
	// Errorf logs a warning message using Printf conventions.
	Errorf(format string, v ...interface{})
}

var ShutdownErr = errors.New("mysql did not shutdown cleanly")

type CtxContextKey struct{}

type Provider struct {
	scene.BaseProvider
	logger          logger
	DB              *sqlx.DB
	closeOnShutdown bool
}

func NewSceneProvider(cfg MySQLConfig, loggingInstance logger) (Provider, error) {
	db, err := sqlx.Connect("mysql", cfg.BuildDSN())
	if err != nil {
		return Provider{}, stack.Trace(err)
	}
	db.SetMaxOpenConns(50)

	db.SetConnMaxLifetime(time.Second * 60 * 15) // 15 minutes which should align to the AWS default
	db.SetMaxIdleConns(25)
	return Provider{
		DB:              db,
		closeOnShutdown: true,
		logger:          loggingInstance,
	}, stack.Trace(err)
}

// Provider uses a global database pool rather than a factory managed pool. This is intentional

func (i Provider) OnFactoryUnmount(valuer scene.FactoryDefaultValuer) error {
	if i.closeOnShutdown {
		wg := deadlinewg.NewWaitGroup(time.Second * 3)
		wg.Add(1)
		var closeErr error
		go func() {
			defer wg.Done()
			closeErr = i.DB.Close()
		}()
		wgErr := wg.Wait()
		if wgErr != nil {
			return closeErr
		}
		return ShutdownErr
	}
	return nil
}

func (i Provider) OnNewContext(ctx scene.Context) {
	// There should be a default logger that is
	val := ctx.Value(CtxContextKey{})
	if val == nil {
		instance := NewInstance(ctx, i.DB)
		ctx.Defer(func(ctx scene.Context, completeErr error) {
			if err := instance.Close(); err != nil {
				if i.logger != nil {
					i.logger.Errorf("Could not close connection on context completion. If you are seeing this, there is a bug in your code. %v", err)
				}
			}
			ctx.Store(CtxContextKey{}, nil)
		})
		// Add the database
		ctx.Store(CtxContextKey{}, instance)
	}
}

func GetManagedDatabaseInstance(ctx context.Context) *Instance {
	if val := ctx.Value(CtxContextKey{}); val != nil {
		return val.(*Instance)
	}
	return nil
}
