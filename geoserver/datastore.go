package geoserver

// CreateDatastoreRequest represents the properties that are required in order to create a datasource
type CreateDatastoreRequest struct {
	// Name is the name of the datastore to create
	Name string

	// Description is the description of the datastore to create
	Description string

	// Type is the type of datastore to create
	Type string

	// DataStore is the name of the workspace to create the datastore in
	Workspace string

	// ConnectionDetails is the connection details Geoserver should use when connecting to the datastore
	ConnectionDetails ConnectionDetails
}

// ConnectionDetails is a type-safe abstraction over the various connection details
// required to connection to different data sources in Geoserver
type ConnectionDetails interface {
	// Entries returns the various connection details as a list of entries
	// this is the format that will then be used in the request to Geoserver
	Entries() map[string]string
}

// GetDatastoresResponse is a response to getting datastores
type GetDatastoresResponse struct {
	Datastores []*Datastore
}

// Datastore is the client's representation of a Geoserver Datastore
type Datastore struct {
	Name                 string
	Description          string
	Type                 string
	Enabled              bool
	Workspace            *Workspace
	ConnectionParameters map[string]string
}

/**
 * REST API
 */

// createDatastoreRestRequest is a struct representation of the JSON required to create a datastore in Geoserver
type createDatastoreRestRequest struct {
	Datastore *restDatastore `json:"dataStore"`
}

// restDatastore is a Geoserver datastore used to interact with the REST API
type restDatastore struct {
	Name                 string                       `json:"name"`
	Description          string                       `json:"description"`
	Type                 string                       `json:"type"`
	Enabled              bool                         `json:"enabled"`
	Workspace            *restWorkspace               `json:"workspace"`
	ConnectionParameters *datasetConnectionParameters `json:"connectionParameters"`
}

// datasetConnectionParameters are the connection parameters for the Dataset
type datasetConnectionParameters struct {
	// entry are the key-value pairs of the connection parameters
	Entry []*entry `json:"entry"`
}

// entry is a key-value pair used when configuring Datasets
// it is used by the Geoserver REST API
type entry struct {
	// Key is the key of the key-value entry
	Key string `json:"@key"`
	// Value is the value of the key-value entry
	Value string `json:"$"`
}

// newCreateDatasouceRestRequest converts the generic CreateDatastoreRequest into the REST specific createDatastoreRestRequest
func newCreateDatasouceRestRequest(request *CreateDatastoreRequest) *createDatastoreRestRequest {
	return &createDatastoreRestRequest{
		Datastore: &restDatastore{
			Name:        request.Name,
			Description: request.Description,
			Type:        request.Type,
			Enabled:     true,
			Workspace: &restWorkspace{
				Name: request.Workspace,
			},
			ConnectionParameters: &datasetConnectionParameters{
				Entry: mapToEntries(request.ConnectionDetails.Entries()),
			},
		},
	}
}

// restDatastoreToDatastore converts a restDatastore to a Datastore
func restDatastoreToDatastore(restDatastore *restDatastore) *Datastore {
	return &Datastore{
		Name:                 restDatastore.Name,
		Description:          restDatastore.Description,
		Type:                 restDatastore.Type,
		Enabled:              restDatastore.Enabled,
		Workspace:            restWorkspaceToWorkspace(restDatastore.Workspace),
		ConnectionParameters: connectionDetailsToMap(restDatastore.ConnectionParameters),
	}
}

func mapToEntries(entries map[string]string) []*entry {
	result := make([]*entry, 0)

	for key, value := range entries {
		result = append(result, &entry{
			Key:   key,
			Value: value,
		})
	}

	return result
}

// connectionDetailsToMap converts datasetConnectionParameters to a map[string]string
func connectionDetailsToMap(connectionDetails *datasetConnectionParameters) (result map[string]string) {
	for _, entry := range connectionDetails.Entry {
		result[entry.Key] = entry.Value
	}
	return
}
