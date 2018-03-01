package geoserver

import (
	"database/sql"
	"fmt"
	"github.com/mattes/migrate"
	"github.com/stretchr/testify/suite"
	"gopkg.in/ory-am/dockertest.v3"
	"net/http"
	"strconv"
	"time"
	// Blank import required by the migration library
	_ "github.com/mattes/migrate/database/postgres"
	// Blank import required by the migration library
	_ "github.com/mattes/migrate/source/file"
	// Blank import required for DB driver
	_ "github.com/lib/pq"
)

const (
	// Geoserver
	geoserverDockerRepo  = "adbourne/geoserver"
	geoserverUsername    = "admin"
	geoserverPassword    = "geoserver"

	// Postgres
	postgresDockerRepo = "mdillon/postgis"
	postgresDockerTag  = "9.6-alpine"
	postgresUsername   = "postgres"
	postgresPassword   = "postgres"
)

// BaseIntegrationTestSuite provides a base to build integration tests for the Geoserver client
type BaseIntegrationTestSuite struct {
	suite.Suite

	// Logger is the test logger
	Logger LoggerFunc

	// DockerTestPool is an asbtraction over Docker resources
	DockerTestPool *dockertest.Pool

	// geoserverResource is the handle for the Geoserver Docker container
	geoserverResource *dockertest.Resource

	// postgresResource is the handle for the Postgres docker container
	postgresResource *dockertest.Resource
}

// InitialiseBase initialises the core components of the integration test
func (suite *BaseIntegrationTestSuite) InitialiseBase() {
	suite.Logger = NewStdOutLogger()
	suite.DockerTestPool = suite.createDockerConnectionPoolOrFail()
}

// geoserverConnectionDetails represents the connection details for the Geoserver instance
type geoserverConnectionDetails struct {
	BaseURL  string
	Port     int
	Username string
	Password string
}

// startGeoserver starts the Geoserver instance using Docker
func (suite *BaseIntegrationTestSuite) startGeoserver(geoserverDockerTag string) *geoserverConnectionDetails {
	geoserverOptions := &dockertest.RunOptions{
		Repository: geoserverDockerRepo,
		Tag:        geoserverDockerTag,
		Links: []string{
			suite.postgresResource.Container.Name,
		},
	}

	suite.Logger.Log("message", "Starting Geoserver...")
	resource := suite.startDockerTestContainerOrFail(suite.DockerTestPool, geoserverOptions)

	suite.geoserverResource = resource

	hostPort := fmt.Sprintf("localhost:%s", resource.GetPort("8080/tcp"))
	port, _ := strconv.Atoi(resource.GetPort("8080/tcp"))

	connectionDetails := &geoserverConnectionDetails{
		BaseURL:  hostPort,
		Port:     port,
		Username: geoserverUsername,
		Password: geoserverPassword,
	}

	suite.waitForGeoserverToStartUp(connectionDetails)

	suite.Logger.Log("message", "Geoserver has started", "hostPort", hostPort, "port", fmt.Sprintf("%d", port))

	return connectionDetails
}

// waitForGeoserverToStartUp waits for the Geoserver Docker instance to start up
func (suite *BaseIntegrationTestSuite) waitForGeoserverToStartUp(connectionDetails *geoserverConnectionDetails) {
	if err := suite.DockerTestPool.Retry(func() error {
		url := "http://" + connectionDetails.BaseURL + "/geoserver"
		suite.Logger.Log(
			"message", "Hitting geoserver to see if running",
			"url", url,
		)
		resp, err := NewTestHTTPClient().Get(url)
		if err != nil {
			return fmt.Errorf("error connecting to geoserver: %s", err)
		}
		if resp.StatusCode != 200 {
			suite.Logger.Log("message", "Geoserver did not return HTTP 200", "status", resp.StatusCode)
			return fmt.Errorf("geoserver did not return a HTTP 200: %s", err)
		}
		return nil
	}); err != nil {
		suite.Fail("Could not connect to docker: %s", err)
	}
	suite.Logger.Log("message", "Geoserver started successfully")
}

// stopGeoserver stops the Geoserver instance running in Docker
func (suite *BaseIntegrationTestSuite) stopGeoserver() {
	if suite.geoserverResource != nil {
		err := suite.DockerTestPool.Purge(suite.geoserverResource)
		if err != nil {
			suite.Fail(fmt.Sprintf("Could not stop Geoserver: %s", err))
		}
	}
}

// postgresConnectionDetails are the connection details for the Geoserver instance
type postgresConnectionDetails struct {
	Host       string
	Port       int
	MappedPort int
	Container  string
	Username   string
	Password   string
}

// URL generate a Postgres connection URL from the connection details
func (cd *postgresConnectionDetails) URL(schema string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cd.Username, cd.Password, cd.Host, cd.MappedPort, schema)
}

func (suite *BaseIntegrationTestSuite) startPostgres() *postgresConnectionDetails {
	postgresOptions := &dockertest.RunOptions{
		Repository: postgresDockerRepo,
		Tag:        postgresDockerTag,
	}

	suite.Logger.Log("message", "Starting Postgres...")
	resource := suite.startDockerTestContainerOrFail(suite.DockerTestPool, postgresOptions)

	suite.postgresResource = resource

	host := "localhost"
	port, _ := strconv.Atoi(resource.GetPort("5432/tcp"))

	containerName := resource.Container.Name[1:len(resource.Container.Name)]

	connectionDetails := &postgresConnectionDetails{
		Host:       host,
		Port:       5432,
		MappedPort: port,
		Container:  containerName,
		Username:   postgresUsername,
		Password:   postgresPassword,
	}

	// Create the Schema
	session := suite.waitForPostgresToStartUp(connectionDetails)
	err := session.Close()
	if err != nil {
		suite.Logger.Log("message", "Unable to close postgres connection")
	}

	// Migrate the schema
	err = suite.migratePostgresSchema(connectionDetails)
	if err != nil {
		suite.FailNow(err.Error())
	}

	return connectionDetails
}

func (suite *BaseIntegrationTestSuite) waitForPostgresToStartUp(connectionDetails *postgresConnectionDetails) (session *sql.DB) {
	// Connect to the default postgres schema
	postgresConnectionURL := connectionDetails.URL("postgres")

	outerErr := suite.DockerTestPool.Retry(func() (err error) {
		suite.Logger.Log(
			"message", "Attempting to connect to Postgres...",
			"URL", postgresConnectionURL,
		)
		db, err := sql.Open("postgres", postgresConnectionURL)
		if err != nil {
			return err
		}
		err = db.Ping()
		if err != nil {
			return err
		}

		session = db
		return
	})

	if outerErr != nil {
		suite.FailNow("Could not connect to docker: %s", outerErr)
	}

	suite.Logger.Log("message", "Postgres started successfully", "host", postgresConnectionURL)
	return
}

// migratePostgresSchema performs a schema migration using test data
func (suite *BaseIntegrationTestSuite) migratePostgresSchema(connectionDetails *postgresConnectionDetails) (err error) {
	defer func() {
		if r := recover(); r != nil {
			suite.Logger.Log("message", "Schema migration panicked", "panic", r)
			suite.FailNow("Unable to migrate Cassandra schema")
		}
	}()

	connectionURL := connectionDetails.URL("postgres")

	migrationsFilePath := fmt.Sprintf("file://testdata/migrations")
	suite.Logger.Log(
		"message", "Performing schema migration for test schema",
		"connection", connectionURL,
		"migrations", migrationsFilePath)

	var m *migrate.Migrate
	m, err = migrate.New(migrationsFilePath, connectionURL)
	if err != nil {
		suite.Logger.Log("message", "Schema migration is unable to connect to persistence", "error", err)
		return
	}

	err = m.Up()
	if err != nil {
		suite.Logger.Log("message", "Schema migration failed", "error", err)
		return
	}

	return
}

func (suite *BaseIntegrationTestSuite) stopPostgres() {
	if suite.postgresResource != nil {
		err := suite.DockerTestPool.Purge(suite.postgresResource)
		if err != nil {
			suite.Fail(fmt.Sprintf("Could not stop postgres: %s", err))
		}
	}
}

// createDockerConnectionPoolOrFail creates a Docker connection pool or fails the suite
func (suite *BaseIntegrationTestSuite) createDockerConnectionPoolOrFail() (pool *dockertest.Pool) {
	suite.Logger.Log("message", "Connecting to docker...")
	pool, err := dockertest.NewPool("")
	if err != nil {
		suite.Fail(fmt.Sprintf("Could not connect to docker: %s", err))
	}

	suite.Logger.Log("message", "Connected to docker!")
	return pool
}

// startDockerTestContainerOrFail starts a Docker container for a test or fails the suite.
func (suite *BaseIntegrationTestSuite) startDockerTestContainerOrFail(pool *dockertest.Pool, options *dockertest.RunOptions) (resource *dockertest.Resource) {
	resource, err := pool.RunWithOptions(options)
	if err != nil {
		suite.Fail(fmt.Sprintf("Could not start resource %s", err))
	}
	return
}

// NewTestHTTPClient creates a new HTTP client for use in tests
func NewTestHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
	}
}
