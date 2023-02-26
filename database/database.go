package database

import (
	"context"
	"fmt"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type Config struct {
	User         string
	Password     string
	Host         string
	Name         string
	MaxIdleConns int
	MaxOpenConns int
	DisableTLS   bool
}

func Open(cfg Config) (driver.Client, error) {
	// Create an HTTP connection to the database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{cfg.Host},
	})

	if err != nil {
		return nil, fmt.Errorf("error opening arangodb for unit tests:  http.NewConnection: %s", err)
	}
	// Create a client
	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(cfg.User, cfg.Password),
	})
	return c, nil
}

func CreateDatabase(ctx context.Context, c driver.Client, cfg Config) (driver.Database, error) {
	var db driver.Database
	var exist bool
	var err error

	exist, err = c.DatabaseExists(ctx, cfg.Name)
	options := driver.CreateDatabaseOptions{
		Users: []driver.CreateDatabaseUserOptions{
			{
				UserName: cfg.User,
				Password: cfg.Password,
			},
		},
	}

	if !exist {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		db, err = c.CreateDatabase(ctx, cfg.Name, &options)
		if err != nil {
			return nil, fmt.Errorf("error creating arangodb database for unit tests:  c.CreateDatabase: %s", err)
		}

	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		db, err = c.Database(ctx, cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("error fetching arangodb database for unit tests:  c.CreateDatabase")
		}
	}

	return db, nil
}

// StatusCheck returns nil if it can successfully talk to the database. It
// returns a non-nil error otherwise.
func StatusCheck(ctx context.Context, client driver.Client) error {

	// First check we can ping the database.
	for attempts := 1; ; attempts++ {
		// Run a simple query to determine connectivity.
		_, err := client.Databases(ctx)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(attempts) * 100 * time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	// Make sure we didn't timeout or be cancelled.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
}
