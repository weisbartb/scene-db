# Scene Database(s)

Adds providers for specific databases that are scene compatible.
These connectors offer additional functionality over just directly embedding your sql client.

## Supported Databases

### MySQL/MariaDB - [Click here](./mysql)
#### Quick start

```go
package app

import (
	"github.com/rs/zerolog"
	"github.com/weisbartb/scene"
	"github.com/weisbartb/scene-db/mysql"
)

type LogWrapper struct {
	zerolog.Logger
}

func (l LogWrapper) Errorf(format string, v ...interface{}) {
	l.Error().Msgf(format, v...)
}


func main() {
	var logger = zerolog.NewConsoleWriter()
	dbProvider,err := mysql.NewSceneProvider(internal.GetTestDatabaseConfiguration(), LogWrapper{Logger:logger})
	// setup scene
	factory, err := scene.NewSceneFactory(scene.Config{
		MaxTTL:    30,
		LogOutput: logger,
	}, dbProvider)
	
	ctx,_ := factory.NewCtx()
	defer ctx.Complete()
	dbInstance := dbProvider.GetManagedDatabaseInstance(ctx)
}
```
#### Primary changes over SQLx
- Provides connection lifecycle management tied to the context.
  - Cleans up leaky open rows
- Throws errors when concurrent row reads are attempted
- Has transaction management support (pinned to the context)
- Has Full Text Search cleaning