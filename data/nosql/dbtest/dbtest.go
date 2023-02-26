package dbtest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"testing"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/gitamped/seed/auth"
	"github.com/gitamped/seed/keystore"
	"github.com/gitamped/stem/data/nosql/dbschema"
	"github.com/gitamped/stem/database"
	"github.com/gitamped/stem/docker"

	"github.com/golang-jwt/jwt/v4"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	Success = "\u2713"
	Failed  = "\u2717"
)

type Data struct {
	CollectionData []string
	EdgeData       []string
	SeedAql        string
}

// StartDB starts a database instance.
func StartDB() (*docker.Container, error) {
	image := "arangodb:3.9.8"
	port := "8529"
	args := []string{"-e", "ARANGO_ROOT_PASSWORD=arangodb"}

	return docker.StartContainer(image, port, args...)
}

// StopDB stops a running database instance.
func StopDB(c *docker.Container) {
	docker.StopContainer(c.ID)
}

// NewUnit creates a test database inside a Docker container. It creates the
// required table structure but the database is otherwise empty. It returns
// the database to use as well as a function to call at the end of the test.
func NewUnit(t *testing.T, c *docker.Container, dbName string, data Data) (*zap.SugaredLogger, driver.Database, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbClient, err := database.Open(database.Config{
		User:       "root",
		Password:   "arangodb",
		Host:       fmt.Sprintf("http://%s", c.Host),
		Name:       "arangodb",
		DisableTLS: true,
	})
	if err != nil {
		t.Fatalf("Opening database connection: %v", err)
	}

	t.Log("Waiting for database to be ready ...")

	if err := database.StatusCheck(ctx, dbClient); err != nil {
		t.Fatalf("status check database: %v", err)
	}

	t.Log("Database ready")

	db, err := database.CreateDatabase(ctx, dbClient, database.Config{
		User:       "arangodb",
		Password:   "arangodb",
		Host:       fmt.Sprintf("http://%s", c.Host),
		Name:       dbName,
		DisableTLS: true,
	})

	// =========================================================================

	if err != nil {
		t.Fatalf("Opening database connection: %v", err)
	}

	t.Log("Migrate and seed database ...")

	if err := dbschema.Migrate(ctx, db, data.CollectionData, data.EdgeData); err != nil {
		docker.DumpContainerLogs(t, c.ID)
		t.Fatalf("Migrating error: %s", err)
	}

	if err := dbschema.Seed(ctx, db, data.SeedAql); err != nil {
		docker.DumpContainerLogs(t, c.ID)
		t.Fatalf("Seeding error: %s", err)
	}

	t.Log("Ready for testing ...")

	var buf bytes.Buffer
	encoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	writer := bufio.NewWriter(&buf)
	log := zap.New(
		zapcore.NewCore(encoder, zapcore.AddSync(writer), zapcore.DebugLevel)).
		Sugar()

	// teardown is the function that should be invoked when the caller is done
	// with the database.
	teardown := func() {
		t.Helper()
		log.Sync()
		writer.Flush()
		fmt.Println("******************** LOGS ********************")
		fmt.Print(buf.String())
		fmt.Println("******************** LOGS ********************")
	}

	return log, db, teardown
}

// Test owns state for running and shutting down tests.
type Test struct {
	DB       driver.Database
	Log      *zap.SugaredLogger
	Auth     *auth.Auth
	Teardown func()

	t *testing.T
}

// NewIntegration creates a database, seeds it, constructs an authenticator.
func NewIntegration(t *testing.T, c *docker.Container, dbName string, data Data) *Test {
	log, db, teardown := NewUnit(t, c, dbName, data)

	// Build an authenticator using this private key and id for the key store.
	auth := NewAuth(t)

	test := Test{
		DB:       db,
		Log:      log,
		Auth:     auth,
		t:        t,
		Teardown: teardown,
	}

	return &test
}

// Build an authenticator using this private key and id for the key store.
func NewAuth(t *testing.T) *auth.Auth {

	// Create RSA keys to enable authentication in our service.
	keyID := "4754d86b-7a6d-4df5-9c65-224741361492"
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// Build an authenticator using this private key and id for the key store.
	auth, err := auth.New(keyID, keystore.NewMap(map[string]*rsa.PrivateKey{keyID: privateKey}))
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

// Token generates an authenticated token for a user.
func (test *Test) Token(id string, roles []string) string {
	test.t.Log("Generating token for test ...")

	claims := auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   id,
			Issuer:    "service project",
			ExpiresAt: jwt.NewNumericDate(time.Now().UTC().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		},
		Roles: roles,
	}

	token, err := test.Auth.GenerateToken(claims)
	if err != nil {
		test.t.Fatal(err)
	}

	return token
}

// Invalid Token generates an invaid token signed by wrong key.
func InvalidToken(t *testing.T, a *auth.Auth) string {
	t.Log("Generating token for test ...")

	claims := auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "11",
			Issuer:    "service project",
			ExpiresAt: jwt.NewNumericDate(time.Now().UTC().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		},
		Roles: []string{"ADMIN", "USER"},
	}

	token, err := a.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}

	return token
}

// StringPointer is a helper to get a *string from a string. It is in the tests
// package because we normally don't want to deal with pointers to basic types
// but it's useful in some tests.
func StringPointer(s string) *string {
	return &s
}

// IntPointer is a helper to get a *int from a int. It is in the tests package
// because we normally don't want to deal with pointers to basic types but it's
// useful in some tests.
func IntPointer(i int) *int {
	return &i
}
