package geoserver

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

const (
	PostgresDatastoreType = "postgres"
)

type RestGeoserverClientTestSuite struct {
	BaseIntegrationTestSuite

	geoserverConnecitonDetails *GeoserverConnectionDetails
	postgresConnectionDetails  *PostgresConnectionDetails

	underTest *RestGeoserverClient
}

func (suite *RestGeoserverClientTestSuite) SetupSuite() {
	suite.InitialiseBase()
	// Postgres is required as Geoserver connects to it directly in order to use it as a datasource
	suite.postgresConnectionDetails = suite.StartPostgres()
	suite.geoserverConnecitonDetails = suite.StartGeoserver()

	geoserverBaseURL := "http://" + suite.geoserverConnecitonDetails.BaseURL + "/geoserver"
	suite.underTest = NewRestGeoserverClient(
		suite.Logger,
		NewTestHTTPClient(),
		geoserverBaseURL,
		suite.geoserverConnecitonDetails.Username,
		suite.geoserverConnecitonDetails.Password,
	)
}

func (suite *RestGeoserverClientTestSuite) TearDownSuite() {
	suite.StopGeoserver()
	suite.StopPostgres()
}

func (suite *RestGeoserverClientTestSuite) TearDownTest() {
	suite.deleteAllWorkspaces()
}

func (suite *RestGeoserverClientTestSuite) TestIsHealthOkReturnsTrueWhenGeoserverIsActive() {
	isHealthy, err := suite.underTest.IsHealthOk()
	assert.NoError(suite.T(), err)

	assert.True(suite.T(), isHealthy)
}

func (suite *RestGeoserverClientTestSuite) TestThatAWorkspaceCanBeCreated() {
	datasetName := "testdatastore"
	err := suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{datasetName})
	assert.NoError(suite.T(), err)

	var response *GetWorkspacesResponse
	response, err = suite.underTest.GetWorkspaces()
	assert.NoError(suite.T(), err)

	workspaces := response.Workspaces
	assert.Equal(suite.T(), 1, len(workspaces))

	assert.Equal(suite.T(), datasetName, workspaces[0].Name)
}

func (suite *RestGeoserverClientTestSuite) TestWorkspaceExistsReturnsFalseWhenAWorkspaceDoesNotExist() {
	workspace := "idontexist"
	isExisting, err := suite.underTest.WorkspaceExists(workspace)
	assert.NoError(suite.T(), err)

	assert.Equal(suite.T(), false, isExisting)
}

func (suite *RestGeoserverClientTestSuite) TestWorkspaceExistsReturnsTrueWhenAWorkspaceDoesExist() {
	workspace := "iexist"
	err := suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})
	assert.NoError(suite.T(), err)

	isExisting, err := suite.underTest.WorkspaceExists(workspace)
	assert.NoError(suite.T(), err)

	assert.Equal(suite.T(), true, isExisting)
}

func (suite *RestGeoserverClientTestSuite) TestThatNoWorkspacesAreReturnedWhenNoWorkspacesExist() {
	workspaces, err := suite.underTest.GetWorkspaces()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, len(workspaces.Workspaces))
}

func (suite *RestGeoserverClientTestSuite) TestWorkspaceCanBeDeleted() {
	workspaceID := "iexist"
	err := suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspaceID})

	assert.NoError(suite.T(), err)

	err = suite.underTest.DeleteWorkspace(workspaceID)
	assert.NoError(suite.T(), err)

	var workspaces *GetWorkspacesResponse
	workspaces, err = suite.underTest.GetWorkspaces()
	assert.NoError(suite.T(), err)

	var workspaceNames []string
	for _, workspace := range workspaces.Workspaces {
		workspaceNames = append(workspaceNames, workspace.Name)
	}

	assert.NotContains(suite.T(), workspaceNames, workspaceID)
}

func (suite *RestGeoserverClientTestSuite) TestDatasourceCanBeCreated() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"

	err := suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        PostgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: NewGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			TestTable,
			workspace,
		),
	})

	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDatasourceCanBeDeleted() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"
	//contactPoint := fmt.Sprintf("%s:%d", suite.postgresConnectionDetails.Container, suite.postgresConnectionDetails.Port)
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        PostgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: NewGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			TestTable,
			workspace,
		),
	})

	err := suite.underTest.DeleteDatastore(workspace, datastore)
	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDatasourceExists() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datasource := "f00b204e98"
	description := "00998ecf8427e"
	err := suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datasource,
		Description: description,
		Type:        PostgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: NewGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			TestTable,
			workspace,
		),
	})
	assert.NoError(suite.T(), err)

	var isExists bool
	isExists, err = suite.underTest.DatastoreExists(workspace, datasource)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), isExists)
}

func (suite *RestGeoserverClientTestSuite) TestDatastoreLayerCanBeCreated() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"

	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        PostgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: NewGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			TestSchema,
			TestDatabase,
		),
	})

	layerName := "g78ndqh356"
	layerDescription := "A data layer created by an integration test"

	err := suite.underTest.CreateFeatureType(&CreateFeatureTypeRequest{
		Name:       layerName,
		NativeName: layerName,
		Title:      layerName,
		Abstract:   layerDescription,
		SRS:        "EPSG:4326",
		NativeBoundingBox: &BoundingBox{
			MinX: -180,
			MaxX: 180,
			MinY: -90,
			MaxY: 90,
			CRS:  "EPSG:4326",
		},
		DataStore: datastore,
		Workspace: workspace,
	})

	assert.NoError(suite.T(), err)
}

func TestRunRestGeoserverClientTestSuite(t *testing.T) {
	suite.Run(t, new(RestGeoserverClientTestSuite))
}

// Deletes all restWorkspaces in Geoserver
func (suite *RestGeoserverClientTestSuite) deleteAllWorkspaces() {
	getWorkspacesResponse, err := suite.underTest.GetWorkspaces()
	assert.NoError(suite.T(), err, "Unable to query for existing restWorkspaces following the test")

	var workspaces []*Workspace
	if getWorkspacesResponse != nil {
		workspaces = getWorkspacesResponse.Workspaces
	}

	for _, workspace := range workspaces {
		workspaceName := workspace.Name
		suite.Logger.Log(
			"message", "Deleting restWorkspace following test",
			"workspace", workspaceName,
		)
		err = suite.underTest.DeleteWorkspace(workspaceName)
		errorMessage := fmt.Sprintf("Unable to delete restWorkspace '%s' following test", workspaceName)
		assert.NoError(suite.T(), err, errorMessage)
	}
}
