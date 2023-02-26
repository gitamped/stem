package dbschema

import (
	"context"
	_ "embed" // Calls init function.
	"fmt"
	"time"

	"github.com/arangodb/go-driver"
)

// Migrate attempts to bring the schema for db up to date with the migrations
// defined in this package.
func Migrate(ctx context.Context, db driver.Database, collections []string, edges []string) error {
	for _, c := range collections {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
		defer cancel()
		_, err := db.CreateCollection(ctx, c, &driver.CreateCollectionOptions{Type: driver.CollectionTypeDocument})
		if err != nil {
			return fmt.Errorf("error creating %s document collection: %s", c, err)
		}
	}

	for _, c := range edges {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
		defer cancel()
		_, err := db.CreateCollection(ctx, c, &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge})
		if err != nil {
			return fmt.Errorf("error creating %s edge collection: %s", c, err)
		}
	}
	return nil
}

// Seed runs the set of seed-data queries against db. The queries are ran in a
// transaction and rolled back if any fail.
func Seed(ctx context.Context, db driver.Database, seedAql string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5000)
	defer cancel()

	_, err := db.Query(ctx, seedAql, make(map[string]any))
	if err != nil {
		return fmt.Errorf("error running query: \n%s\n%s\n", seedAql, err)
	}

	return nil
}
