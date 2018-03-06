package geoserver

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

const (
	geoserverDockerTag10 = "v2.10.5"
	geoserverDockerTag11 = "v2.11.4"
	geoserverDockerTag12 = "v2.12.0"

	postgresDatastoreType = "postgres"
	testDatabase          = "postgres"
	testSchema            = "public"
)

type RestGeoserverClientTestSuite struct {
	BaseIntegrationTestSuite

	geoserverDockerTag string

	geoserverConnecitonDetails *geoserverConnectionDetails
	postgresConnectionDetails  *postgresConnectionDetails

	underTest *RestGeoserverClient
}

func newRestGeoserverClientTestSuite(geoserverContainer string) *RestGeoserverClientTestSuite {
	return &RestGeoserverClientTestSuite{
		geoserverDockerTag: geoserverContainer,
	}
}

func (suite *RestGeoserverClientTestSuite) SetupSuite() {
	suite.InitialiseBase()
	// Postgres is required as Geoserver connects to it directly in order to use it as a datasource
	suite.postgresConnectionDetails = suite.startPostgres()
	suite.geoserverConnecitonDetails = suite.startGeoserver(suite.geoserverDockerTag)

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
	suite.stopGeoserver()
	suite.stopPostgres()
}

func (suite *RestGeoserverClientTestSuite) TearDownTest() {
	suite.deleteAllWorkspaces()
}

func (suite *RestGeoserverClientTestSuite) TestIsHealthOkReturnsTrueWhenGeoserverIsActive() {
	isHealthy, err := suite.underTest.IsHealthOk()
	assert.NoError(suite.T(), err)

	assert.True(suite.T(), isHealthy)
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

func (suite *RestGeoserverClientTestSuite) TestGetWorkspacesReturnsNoWorkspacesWhenNoWorkspacesExist() {
	workspaces, err := suite.underTest.GetWorkspaces()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, len(workspaces.Workspaces))
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

func (suite *RestGeoserverClientTestSuite) TestDeleteWorkspaceReturnsAnErrorWhenTheWorkspaceDoesNotExist() {
	workspaceID := "idonotexist"
	err := suite.underTest.DeleteWorkspace(workspaceID)
	assert.Error(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDatastoreExistsReturnsFalseWhenDatastoreDoesNotExist() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	isExists, err := suite.underTest.DatastoreExists(workspace, "idonotexist")
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), isExists)
}

func (suite *RestGeoserverClientTestSuite) TestDatastoreExistsReturnsTrueWhenADatastoreExists() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"
	err := suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})
	assert.NoError(suite.T(), err)

	var isExists bool
	isExists, err = suite.underTest.DatastoreExists(workspace, datastore)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), isExists)
}

func (suite *RestGeoserverClientTestSuite) TestGetDatastoresReturnsNoDatastoreWhenNoneExist() {
	workspace := "e9800998ec"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	getDatastoresResponse, err := suite.underTest.GetDatastores(workspace)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), getDatastoresResponse)
	assert.Equal(suite.T(), 0, len(getDatastoresResponse.Datastores))
}

func (suite *RestGeoserverClientTestSuite) TestGetDatastoresReturnsADatastoreWhenADatastoreExists() {
	workspace := "e9800998ec"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "98ecf8427"
	description := "9800998ecf84"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	getDatastoresResponse, err := suite.underTest.GetDatastores(workspace)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), getDatastoresResponse)
	assert.Equal(suite.T(), 1, len(getDatastoresResponse.Datastores))
}

func (suite *RestGeoserverClientTestSuite) TestDatastoreCanBeCreated() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"

	err := suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDatastoreCanBeDeleted() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	err := suite.underTest.DeleteDatastore(workspace, datastore)
	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDeleteDatastoreReturnsDoesNotReturnAnErrorWhenTheDatastoreDoesNotExistButGeoserverRespondsCorrectly() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	err := suite.underTest.DeleteDatastore(workspace, "idonotexist")
	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestFeatureTypeExistsReturnsFalseWhenFeatureTypeDoesNotExist() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "98ecf8427"
	description := "9800998ecf84"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	isExists, err := suite.underTest.FeatureTypeExists(workspace, "98ecf8427", "idonotexist")
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), isExists)
}

func (suite *RestGeoserverClientTestSuite) TestFeatureTypeExistReturnsTrueWhenFeatureTypeExists() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "98ecf8427"
	description := "9800998ecf84"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	layerName := "g78ndqh356"
	layerDescription := "A data layer created by an integration test"

	suite.underTest.CreateFeatureType(&CreateFeatureTypeRequest{
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

	isExists, err := suite.underTest.FeatureTypeExists(workspace, "98ecf8427", layerName)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), isExists)
}

func (suite *RestGeoserverClientTestSuite) TestGetFeatureTypesReturnsNoFeatureTypesWhenNoneExist() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "98ecf8427"
	description := "9800998ecf84"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	featureTypes, err := suite.underTest.GetFeatureTypes(workspace, "98ecf8427")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), featureTypes)
	assert.Equal(suite.T(), 0, len(featureTypes.FeatureTypes))
}

func (suite *RestGeoserverClientTestSuite) TestGetFeatureTypesReturnsFeatureTypesWhenFeatureTypesExist() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "98ecf8427"
	description := "9800998ecf84"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
		),
	})

	layerName := "g78ndqh356"
	layerDescription := "A data layer created by an integration test"

	suite.underTest.CreateFeatureType(&CreateFeatureTypeRequest{
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

	featureTypes, err := suite.underTest.GetFeatureTypes(workspace, datastore)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), featureTypes)
	assert.Equal(suite.T(), 1, len(featureTypes.FeatureTypes))
}

func (suite *RestGeoserverClientTestSuite) TestFeatureTypeCanBeCreated() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"

	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			testDatabase,
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

func (suite *RestGeoserverClientTestSuite) TestFeatureTypeCanBeDeleted() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"
	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			workspace,
		),
	})

	featureType := "g78ndqh356"
	layerDescription := "A data layer created by an integration test"

	suite.underTest.CreateFeatureType(&CreateFeatureTypeRequest{
		Name:       featureType,
		NativeName: featureType,
		Title:      featureType,
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

	err := suite.underTest.DeleteFeatureType(workspace, datastore, featureType)
	assert.NoError(suite.T(), err)
}

func (suite *RestGeoserverClientTestSuite) TestDeleteFeatureTypeDoesNotReturnAnErrorWhenTheFeatureTypeDoesNotExistButGeoserverRespondsCorrectly() {
	workspace := "d41d8cd98"
	suite.underTest.CreateWorkspace(&CreateWorkspaceRequest{workspace})

	datastore := "f00b204e98"
	description := "00998ecf8427e"

	suite.underTest.CreateDatastore(&CreateDatastoreRequest{
		Name:        datastore,
		Description: description,
		Type:        postgresDatastoreType,
		Workspace:   workspace,
		ConnectionDetails: newGeoserverPostgisConnectionDetails(
			suite.postgresConnectionDetails.Container,
			suite.postgresConnectionDetails.Port,
			suite.postgresConnectionDetails.Username,
			suite.postgresConnectionDetails.Password,
			testSchema,
			workspace,
		),
	})

	err := suite.underTest.DeleteFeatureType(workspace, datastore, "g78ndqh356")
	assert.NoError(suite.T(), err)
}

func TestRunRestGeoserver10ClientTestSuite(t *testing.T) {
	suite.Run(t, newRestGeoserverClientTestSuite(geoserverDockerTag10))
}

func TestRunRestGeoserver11ClientTestSuite(t *testing.T) {
	suite.Run(t, newRestGeoserverClientTestSuite(geoserverDockerTag11))
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
			"message", "Deleting workspace following test",
			"workspace", workspaceName,
		)
		err = suite.underTest.DeleteWorkspace(workspaceName)
		errorMessage := fmt.Sprintf("Unable to delete restWorkspace '%s' following test", workspaceName)
		assert.NoError(suite.T(), err, errorMessage)
	}
}

// newGeoserverPostgisConnectionDetails creates a new postgresGeoserverConnectionDetails
func newGeoserverPostgisConnectionDetails(host string, port int, username string, password string, schema string, namespace string) *postgresGeoserverConnectionDetails {
	return &postgresGeoserverConnectionDetails{
		Host:      host,
		Port:      port,
		Username:  username,
		Password:  password,
		Schema:    schema,
		Namespace: namespace,
	}
}

// postgresGeoserverConnectionDetails represents the connection details for Postgres as used by Geoserver
type postgresGeoserverConnectionDetails struct {
	Host      string
	Port      int
	Username  string
	Password  string
	Schema    string
	Namespace string
}

// Entries returns the connection detail entries for connecting to a Postgis-enabled Postgres database
func (cd *postgresGeoserverConnectionDetails) Entries() map[string]string {
	return map[string]string{
		"host":      cd.Host,
		"port":      fmt.Sprintf("%d", cd.Port),
		"user":      cd.Username,
		"passwd":    cd.Password,
		"dbtype":    "postgis",
		"database":  "postgres",
		"schema":    cd.Schema,
		"namespace": cd.Namespace,
		// secondary configuration - set to defaults
		"Evictor run periodicity":                    "300",
		"Max open prepared statements":               "50",
		"encode functions":                           "false",
		"Batch insert size":                          "1",
		"preparedStatements":                         "false",
		"Loose bbox":                                 "true",
		"Estimated extends":                          "true",
		"fetch size":                                 "1000",
		"Expose primary keys":                        "false",
		"validate connections":                       "true",
		"Support on the fly geometry simplification": "true",
		"Connection timeout":                         "20",
		"create database":                            "false",
		"min connections":                            "1",
		"max connections":                            "10",
		"Evictor tests per run":                      "3",
		"Test while idle":                            "true",
		"Max connection idle time":                   "300",
	}
}
