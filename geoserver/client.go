package geoserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// Client is a Geoserver client
type Client interface {
	// IsHealthOk checks if the Geoserver instance is running it returns true if healthy, false otherwise.
	IsHealthOk() (bool, error)

	// WorkspaceExists returns true when the provided workspace exists, false otherwise.
	// An error is returned if it is not possible.
	WorkspaceExists(workspace string) (bool, error)

	// GetWorkspaces gets the available workspaces, returning an error if it is not possible.
	GetWorkspaces() (*GetWorkspacesResponse, error)

	// CreateWorkspace creates a workspace, returning an error if it is not possible.
	CreateWorkspace(*CreateWorkspaceRequest) error

	// DeleteWorkspace deletes a workspace, returning an error if it is not possible.
	DeleteWorkspace(workspace string) error

	// DatastoreExists checks if a datastore exists in a workspace, returns true if it does, false otherwise.
	DatastoreExists(workspace string, datastore string) (bool, error)

	// GetDatastores gets datastore for a workspace, returning an error if it is not possible.
	GetDatastores(workspace string) (*GetDatastoresResponse, error)

	// CreateDatastore creates a datastore in the provided workspace, returning an error if it is not possible.
	CreateDatastore(request *CreateDatastoreRequest) error

	// DeleteDatastore deletes the specified datastore, returning an error if it is not possible.
	DeleteDatastore(workspace string, datastore string) error

	// FeatureTypeExists checks if a feature type exists in a workspaces and datastore, returning true if it does and false otherwise.
	FeatureTypeExists(workspace string, datastore string, featureType string) (bool, error)

	// GetFeatureTypes gets the feature types for the provided workspace and datastore
	GetFeatureTypes(workspace string, datastore string) (*GetFeatureTypesResponse, error)

	// CreateFeatureType creates a "feature type", which is essentially a layer from a datastore.
	CreateFeatureType(request *CreateFeatureTypeRequest) error

	// DeleteFeatureType deletes a feature type if it exists.
	DeleteFeatureType(workspace string, datastore string, featureType string) error
}

// RestGeoserverClient is a implementation of GeoserverClient which uses Geoserver's REST API.
type RestGeoserverClient struct {
	logger            LoggerFunc
	httpClient        *http.Client
	geoserverBaseURL  string
	geoserverUsername string
	geoserverPassword string
}

// NewRestGeoserverClient creates a new RestGeoserverClient.
func NewRestGeoserverClient(logger LoggerFunc, httpClient *http.Client, geoserverBaseURL string, geoserverUsername string, geoserverPassword string) *RestGeoserverClient {
	return &RestGeoserverClient{
		logger:            logger,
		httpClient:        httpClient,
		geoserverBaseURL:  geoserverBaseURL,
		geoserverUsername: geoserverUsername,
		geoserverPassword: geoserverPassword,
	}
}

// IsHealthOk checks if the Geoserver instance is running it returns true if healthy, false otherwise.
// It interacts with Geoserver using its REST API.
func (client *RestGeoserverClient) IsHealthOk() (isHealthy bool, err error) {

	url := client.geoserverBaseURL + "/rest/about/status"
	client.logger.Log(
		levelKey, levelDebug,
		messageKey, "Querying Geoserver to see if healthy",
		urlKey, url,
	)

	request, err := client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if httpCodeOK == response.StatusCode {
		isHealthy = true

	} else {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Geoserver responded with a non-200 HTTP status code",
			urlKey, url,
			statusKey, response.StatusCode,
		)
	}

	return
}

// WorkspaceExists returns true when the provided workspace exists, false otherwise.
// An error is returned if it is not possible. It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) WorkspaceExists(workspace string) (isExisting bool, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace
	client.logger.Log(
		levelKey, levelDebug,
		messageKey, "Querying Geoserver for specific workspace",
		urlKey, url,
		"workspace", workspace,
	)

	var req *http.Request
	req, err = client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Cannot communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == resp.StatusCode {
		isExisting = true
	}
	return
}

// GetWorkspaces gets the available workspaces, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) GetWorkspaces() (response *GetWorkspacesResponse, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces"
	client.logger.Log(
		levelDebug, levelDebug,
		messageKey, "Querying Geoserver for workspaces",
		urlKey, url,
	)

	req, err := client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelDebug, levelDebug,
			messageKey, "Cannot communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	restResponse := newEmptyGetWorkspacesRestResponse()

	if 200 == resp.StatusCode {
		var responseBytes []byte
		responseBytes, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}

		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Geoserver returned workspaces",
			urlKey, url,
			"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
			"responseBody", string(responseBytes),
		)

		err = json.Unmarshal(responseBytes, restResponse)
		if err != nil {
			client.logger.Log(
				levelKey, levelWarn,
				messageKey, "Geoserver returned an invalid response",
				urlKey, url,
				"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
				"responseBody", string(responseBytes),
			)
			// Geoserver likes to return {"workspaces":""} when there are no spaces
			// this isn't necessarily standard, but we'll account for it here
			response = newEmptyGetWorkspacesResponse()
			err = nil
			return
		}

		response = getWorkspacesRestResponseToGetWorkspacesResponse(restResponse)

		return
	}

	responseBody, _ := ioutil.ReadAll(resp.Body)
	client.logger.Log(
		messageKey, "Unable to query Geoserver for workspaces",
		urlKey, url,
		"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
		"responseBody", string(responseBody),
	)

	err = errors.New("unable to query Geoserver for workspaces")
	return
}

// CreateWorkspace creates a workspace, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) CreateWorkspace(request *CreateWorkspaceRequest) (err error) {
	url := client.geoserverBaseURL + "/rest/workspaces.json"

	restRequest := newCreateWorkspaceRestRequest(request)

	var requestJSONBytes []byte
	requestJSONBytes, err = json.Marshal(restRequest)
	if err != nil {
		return
	}

	client.logger.Log(
		levelKey, levelDebug,
		messageKey, "Creating a Geoserver workspace",
		urlKey, url,
		requestKey, string(requestJSONBytes),
	)

	req, err := client.createAuthJSONRequest(http.MethodPost, url, bytes.NewReader(requestJSONBytes))
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	workSpaceName := request.Workspace
	if codeCreated == response.StatusCode {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Datastore created successfully",
			urlKey, url,
			"workspace", workSpaceName,
		)
		return
	}

	responseBody, _ := ioutil.ReadAll(response.Body)
	client.logger.Log(
		levelKey, levelDebug,
		messageKey, "Unable to create workspace",
		urlKey, url,
		"workspace", workSpaceName,
		"responseStatus", fmt.Sprintf("%d", response.StatusCode),
		"responseBody", string(responseBody),
	)

	err = fmt.Errorf("unable to create workspace '%s'", workSpaceName)
	return
}

// DeleteWorkspace deletes a workspace, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) DeleteWorkspace(workspace string) (err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "?recurse=true"

	client.logger.Log(
		levelKey, levelDebug,
		messageKey, "Deleting Geoserver workspace",
		urlKey, url,
		"workspace", workspace,
	)

	req, err := client.createAuthJSONRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == response.StatusCode {
		client.logger.Log(
			messageKey, "DataStore deleted successfully",
			urlKey, url,
			"workspace", workspace,
		)
		return
	}

	client.logger.Log(
		levelDebug, levelDebug,
		messageKey, "Unable to delete workspace",
		urlKey, url,
		"workspace", workspace,
		"responseStatus", fmt.Sprintf("%d", response.StatusCode),
	)

	err = fmt.Errorf("unable to delete workspace '%s'", workspace)
	return
}

// DatastoreExists checks if a datastore exists in a workspace, returns true if it does, false otherwise.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) DatastoreExists(workspace string, datastore string) (isExisting bool, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/datastores/" + datastore + ".json"

	client.logger.Log(
		messageKey, "Querying Geoserver for specific data datastore",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
	)

	var req *http.Request
	req, err = client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == resp.StatusCode {
		isExisting = true

	} else {
		client.logger.Log(
			messageKey, "Geoserver returned a non-200 HTTP when checking if data datastore exists",
			urlKey, url,
			"datastore", datastore,
			"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
		)
	}
	return
}

// GetDatastores gets the available datastores, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) GetDatastores(workspace string) (response *GetDatastoresResponse, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/datastores.json"
	client.logger.Log(
		levelDebug, levelDebug,
		messageKey, "Querying Geoserver for datastores",
		urlKey, url,
		"workspace", workspace,
	)

	req, err := client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelDebug, levelDebug,
			messageKey, "Cannot communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	restResponse := newEmptyGetDatastoresRestResponse()

	if 200 == resp.StatusCode {
		var responseBytes []byte
		responseBytes, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}

		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Geoserver returned datastores",
			urlKey, url,
			"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
			"responseBody", string(responseBytes),
		)

		err = json.Unmarshal(responseBytes, restResponse)
		if err != nil {
			client.logger.Log(
				levelKey, levelWarn,
				messageKey, "Geoserver returned an invalid response",
				urlKey, url,
				"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
				"responseBody", string(responseBytes),
			)
			// Geoserver likes to return {"workspaces":""} when there are no spaces
			// this isn't necessarily standard, but we'll account for it here
			response = newEmptyGetDatastoresResponse()
			err = nil
			return
		}

		response = getDatastoresRestResponseToGetDatatstoresResponse(restResponse)

		return
	}

	return
}

// CreateDatastore creates a datastore in the provided workspace, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) CreateDatastore(request *CreateDatastoreRequest) (err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + request.Workspace + "/datastores"

	restRequest := newCreateDatasouceRestRequest(request)

	var requestJSONBytes []byte
	requestJSONBytes, err = json.Marshal(restRequest)
	if err != nil {
		return
	}

	client.logger.Log(
		messageKey, "Creating a Geoserver datastore",
		urlKey, url,
		"request", string(requestJSONBytes),
	)

	req, err := client.createAuthJSONRequest(http.MethodPost, url, bytes.NewReader(requestJSONBytes))
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 201 == response.StatusCode {
		client.logger.Log(
			messageKey, "datastore created successfully",
			urlKey, url,
			"workspace", request.Workspace,
			"datastore", request.Name,
		)
		return

	}

	err = fmt.Errorf("unable to create datastore '%s'", request.Name)
	return
}

// DeleteDatastore deletes the specified datastore, returning an error if it is not possible.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) DeleteDatastore(workspace string, datastore string) (err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/" + datastore + "?recurse=true"

	client.logger.Log(
		messageKey, "Deleting Geoserver datastore",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
	)

	req, err := client.createAuthJSONRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == response.StatusCode {
		client.logger.Log(
			messageKey, "datastore deleted successfully",
			urlKey, url,
			"workspace", workspace,
			"datastore", datastore,
		)
		return
	}

	client.logger.Log(
		messageKey, "Unable to delete datastore",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
		"responseStatus", fmt.Sprintf("%d", response.StatusCode),
	)

	err = fmt.Errorf("unable to delete datastore '%s'", workspace)
	return
}

// FeatureTypeExists checks if a feature type exists, returning true if it does, false otherwise.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) FeatureTypeExists(workspace string, datastore string, featureType string) (isExisting bool, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/datastores/" + datastore + "/featuretypes/" + featureType + ".json"

	client.logger.Log(
		messageKey, "Querying Geoserver for specific data feature type",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
		"featureType", featureType,
	)

	var req *http.Request
	req, err = client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == resp.StatusCode {
		isExisting = true

	} else {
		client.logger.Log(
			messageKey, "Geoserver returned a non-200 HTTP when checking if feature type exists",
			urlKey, url,
			"workspace", workspace,
			"datastore", datastore,
			"featureType", featureType,
			"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
		)
	}
	return
}

// GetFeatureTypes gets available feature types.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) GetFeatureTypes(workspace string, datastore string) (result *GetFeatureTypesResponse, err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/datastores/" + datastore + "/featuretypes.json"
	client.logger.Log(
		levelDebug, levelDebug,
		messageKey, "Querying Geoserver for feature types",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
	)

	req, err := client.createAuthJSONRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			levelDebug, levelDebug,
			messageKey, "Cannot communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	restResponse := newBlankGetFeatureTypeRestResponse()

	if 200 == resp.StatusCode {
		var responseBytes []byte
		responseBytes, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}

		client.logger.Log(
			levelKey, levelDebug,
			messageKey, "Geoserver returned feature types",
			urlKey, url,
			"workspace", workspace,
			"datastore", datastore,
			"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
			"responseBody", string(responseBytes),
		)

		err = json.Unmarshal(responseBytes, restResponse)
		if err != nil {
			client.logger.Log(
				levelKey, levelWarn,
				messageKey, "Geoserver returned an invalid response",
				urlKey, url,
				"workspace", workspace,
				"datastore", datastore,
				"responseStatus", fmt.Sprintf("%d", resp.StatusCode),
				"responseBody", string(responseBytes),
			)
			result = newEmptyGetFeatureTypesResponse()
			err = nil
			return
		}

		result = getFeatureTypeRestResponseToGetFeatureTypeResponse(restResponse)

		return
	}

	return
}

// CreateFeatureType creates a "feature type", which is essentially a layer from a datastore.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) CreateFeatureType(request *CreateFeatureTypeRequest) (err error) {
	url := client.geoserverBaseURL + "/rest/workspaces/" + request.Workspace + "/datastores/" + request.DataStore + "/featuretypes.json"

	restRequest := newCreateFeatureTypeRestRequest(request)

	var requestJSONBytes []byte
	requestJSONBytes, err = json.Marshal(restRequest)
	if err != nil {
		return
	}

	client.logger.Log(
		messageKey, "Creating a Geoserver feature type",
		urlKey, url,
		"request", string(requestJSONBytes),
	)

	req, err := client.createAuthJSONRequest(http.MethodPost, url, bytes.NewReader(requestJSONBytes))
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Could not communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 201 == response.StatusCode {
		client.logger.Log(
			messageKey, "Feature type created successfully",
			urlKey, url,
			"workspace", request.DataStore,
			"datastore", request.Name,
		)
		return

	}

	err = fmt.Errorf("unable to create datalayer '%s'", request.Name)
	return
}

// DeleteFeatureType deletes a feature type, returning an error if it doesn't exist.
// It interacts with with Geoserver using its REST API.
func (client *RestGeoserverClient) DeleteFeatureType(workspace string, datastore string, featureType string) (err error) {

	url := client.geoserverBaseURL + "/rest/workspaces/" + workspace + "/datastores/" + datastore + "/featuretypes/" + featureType + ".json?recurse=true"

	client.logger.Log(
		messageKey, "Deleting a Geoserver feature type",
		urlKey, url,
		"workspace", workspace,
		"datstore", datastore,
		"featureType", featureType,
	)

	req, err := client.createAuthJSONRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}

	response, err := client.httpClient.Do(req)
	if err != nil {
		client.logger.Log(
			messageKey, "Cannot communicate with Geoserver",
			urlKey, url,
			errorKey, err.Error(),
		)
		return
	}

	if 200 == response.StatusCode || 404 == response.StatusCode {
		client.logger.Log(
			messageKey, "Feature type deleted successfully",
			urlKey, url,
			"workspace", workspace,
			"datastore", datastore,
			"featureType", featureType,
		)
		return
	}

	client.logger.Log(
		messageKey, "Feature type cannot be deleted",
		urlKey, url,
		"workspace", workspace,
		"datastore", datastore,
		"featureType", featureType,
		"statusCode", fmt.Sprintf("%d", response.StatusCode),
	)

	err = fmt.Errorf("unable to delete feature type '%s' in datastore '%s' and workspace '%s'", featureType, datastore, workspace)
	return
}

// createAuthJSONRequest creates a HTTP request, which uses JSON as a payload basic auth
func (client *RestGeoserverClient) createAuthJSONRequest(method string, url string, body io.Reader) (request *http.Request, err error) {
	request, err = http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	request.Header.Set(contentTypeHeader, applicationJSON)
	request.Header.Set(acceptHeader, applicationJSON)
	request.SetBasicAuth(client.geoserverUsername, client.geoserverPassword)
	return
}
