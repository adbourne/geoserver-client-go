package geoserver

import "fmt"

// CreateFeatureTypeRequest is the information required in order to create a feature type.
type CreateFeatureTypeRequest struct {
	// Name is the name of the feature type.
	Name string

	// NativeName is the native name of the feature type.
	NativeName string

	// Title is the title of the feature type.
	Title string

	// Abstract is the abstract of the feature type.
	Abstract string

	// NativeCRS is the native CRS of the feature type.
	NativeCRS string

	// SRS is the SRS of the feature type.
	SRS string

	// NativeBoundingBox is the native bounding box of the feature type.
	NativeBoundingBox *BoundingBox

	// LatLongBoundingBox is the latitude and latitude bounding box of the feature type.
	LatLongBoundingBox *BoundingBox

	// DataStore is the name of the datastore to which the feature type belongs.
	DataStore string

	// Workspace is the name of the workspace to which the feature type belongs.
	Workspace string
}

// GetFeatureTypesResponse represents a response for the get feature types request
type GetFeatureTypesResponse struct {
	FeatureTypes []*FeatureType
}

func newEmptyGetFeatureTypesResponse() *GetFeatureTypesResponse {
	return &GetFeatureTypesResponse{
		FeatureTypes: make([]*FeatureType, 0),
	}
}

// FeatureType represents a Geoserver feature type
type FeatureType struct {
	Name string
	Href string
}

// BoundingBox represents a geospatial bounding box
type BoundingBox struct {
	// MinX is the minimum X coordinate of the bounding box.
	MinX float64

	// MaxX is the maximum X coordinate of bounding box.
	MaxX float64

	// MinY is the minimum Y coordinate of the bounding box.
	MinY float64

	// MaxX is the maximum X coordinate of the bounding box.
	MaxY float64

	// CRS is the CRS of the bounding box's coordinates.
	CRS string
}

/**
 * REST API
 */

// restFeatureType is a Geoserver feature type used to interact with the REST API
type restFeatureType struct {
	// Name is the name of the layer, it can include any case and spaces.
	Name string `json:"name"`

	// Href is the link to the feature type
	Href string `json:"href"`

	// NativeName is the native name of the feature type.
	// It should be lowercase and contain no spaces.
	NativeName string `json:"nativeName"`

	// Namespace is the namespace to which the feature type belongs.
	Namespace *restNamespace `json:"namespace"`

	// Title is the title of the feature type.
	Title string `json:"title"`

	// Asbtract is the abstract of the feature type.
	Abstract string `json:"abstract"`

	// TODO: Keywords

	// TODO: NativeCRS

	// SRS is the srs the layer should use e.g "EPSG:4326".
	SRS string `json:"srs"`

	// NativeBoundingBox is the bounding box of the layer, using the native SRS/CRS.
	NativeBoundingBox *BoundingBox `json:"nativeBoundingBox"`

	// LatLongBoundingBox is the atitude and latitude bounding box.
	LatLongBoundingBox *BoundingBox `json:"latLongBoundingBox"`

	// ProjectionPolicy is the projection policy.
	ProjectionPolicy string `json:"projectionPolicy"`

	// restAttributes is the restAttributes belonging to the feature type.
	Attributes *restAttributes `json:"attributes"`

	// Enabled is whether of not the layer is enabled.
	Enabled bool `json:"enabled"`

	// Store is the datastore to use.
	Store *restStore `json:"store"`
}

// restAttributes only purpose is to create the JSON required by Geoserver.
type restAttributes struct {
	// restAttributes is a collection of attributes.
	Attribute []*restAttribute `json:"attribute"`
}

// restAttribute is a Geoserver attributes.
type restAttribute struct {
	// Name is the name of the attribute.
	Name string `json:"name"`
	// MinOccurs is the minimum number of times the attribute occurs.
	MinOccurs int `json:"minOccurs"`

	// MacOccurs is the maximum number of times the attribute occurs.
	MaxOccurs int `json:"maxOccurs"`

	// Nillable represents if the feature type is nillable, or not.
	Nillable bool `json:"nillable"`

	// Binding is the feature type's Java class binding.
	Binding string `json:"binding"`
}

// createFeatureTypeRestRequest exists in order to represent the JSON required by Geoserver when creating a feature type.
type createFeatureTypeRestRequest struct {
	FeatureType *restFeatureType `json:"featureType"`
}

// restNamespace exists in order to create the JSON required by Geoserver when dealing with feature types.
// is represents a Geoserver features types's namespace.
type restNamespace struct {
	// Name is the name of the Namepace (aka DataStore)
	Name string `json:"name"`
}

// restStore exists in order to create the JSON required by Geoserver when dealing with feature types
// restStore represents a Geoserver datastore
type restStore struct {
	// Class is the restStore's class
	Class string `json:"@class"`

	// Name is the restStore's name
	Name string `json:"name"`
}

// newCreateFeatureTypeRestRequest creates a new CreateFeatureTypeRequest.
func newCreateFeatureTypeRestRequest(request *CreateFeatureTypeRequest) *createFeatureTypeRestRequest {
	return &createFeatureTypeRestRequest{&restFeatureType{
		Name:       request.Name,
		NativeName: request.NativeName,
		Title:      request.Title,
		Namespace: &restNamespace{
			Name: request.Workspace,
		},
		SRS:                request.SRS,
		NativeBoundingBox:  request.NativeBoundingBox,
		LatLongBoundingBox: request.NativeBoundingBox,
		Enabled:            true,
		Store:              newStore(request.Workspace, request.DataStore),
		ProjectionPolicy:   "REPROJECT_TO_DECLARED", // TODO
		Attributes: &restAttributes{[]*restAttribute{// TODO: Make dynamic
			{
				Name:      "Geometry",
				MinOccurs: 0,
				MaxOccurs: 1,
				Nillable:  true,
				Binding:   "com.vividsolutions.jts.geom.Point",
			},
		},
		},
	}}
}

type getFeatureTypeRestResponse struct {
	FeatureTypes restFeatureTypes `json:"featureTypes"`
}

func newBlankGetFeatureTypeRestResponse() *getFeatureTypeRestResponse {
	return &getFeatureTypeRestResponse{
	}
}

type restFeatureTypes struct {
	FeatureTypes []*restFeatureType `json:"featureType"`
}

func getFeatureTypeRestResponseToGetFeatureTypeResponse(response *getFeatureTypeRestResponse) *GetFeatureTypesResponse {
	result := &GetFeatureTypesResponse{
		FeatureTypes: make([]*FeatureType, 0),
	}

	for _, featureType := range response.FeatureTypes.FeatureTypes {
		result.FeatureTypes = append(result.FeatureTypes, restFeatureTypeToFeatureType(featureType))
	}

	return result
}

func restFeatureTypeToFeatureType(featureType *restFeatureType) *FeatureType {
	return &FeatureType{
		Name: featureType.Name,
		Href: featureType.Href,
	}
}

// newStore creates a new REST restStore with default values populates
func newStore(workspace string, name string) *restStore {
	return &restStore{
		Class: "dataStore",
		Name:  fmt.Sprintf("%s:%s", workspace, name),
	}
}
