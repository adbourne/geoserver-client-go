package geoserver

// Workspace is the client's standard representation of a Geoserver workspace
type Workspace struct {
	// Name is the name of the workspace
	Name string

	// Href is the HREF of the workspace
	Href string
}

// CreateWorkspaceRequest is the information required in order to create a workspace
type CreateWorkspaceRequest struct {
	Workspace string
}

// GetWorkspacesResponse is the response from a create workspace request
type GetWorkspacesResponse struct {
	Workspaces []*Workspace
}

// newEmptyGetWorkspacesResponse is a utility function to create a new GetWorkspacesResponse
// with all field defaulted to empty
func newEmptyGetWorkspacesResponse() *GetWorkspacesResponse {
	return &GetWorkspacesResponse{
		Workspaces: make([]*Workspace, 0),
	}
}

/**
 * REST API
 */

// createWorkspaceRestRequest is a REST request to create a workspace
type createWorkspaceRestRequest struct {
	// restWorkspace is the workspace to create
	// only the 'Name' parameter is required
	Workspace *restWorkspace `json:"workspace"`
}

// getWorkspacesRestResponse exists in order to represent the JSON required by Geoserver when getting a workspace.
type getWorkspacesRestResponse struct {
	Workspaces *restWorkspaces `json:"workspaces,omitempty"`
}

// restWorkspaces exists in order to represent the JSON required by Geoserver when dealing with workspaces.
type restWorkspaces struct {
	Workspace []*restWorkspace `json:"workspace,omitempty"`
}

// restWorkspace represents a Geoserver workspace used to interact with the Geoserver REST API
type restWorkspace struct {
	Name string `json:"name,omitempty"`
	Href string `json:"href,omitempty"`
}

// newCreateWorkspaceRestRequest creates a new createWorkspaceRestRequest
func newCreateWorkspaceRestRequest(request *CreateWorkspaceRequest) *createWorkspaceRestRequest {
	return &createWorkspaceRestRequest{
		Workspace: &restWorkspace{
			Name: request.Workspace,
		},
	}
}

// getWorkspacesRestResponseToGetWorkspacesResponse converts a getWorkspacesRestResponse into a GetWorkspacesResponse
func getWorkspacesRestResponseToGetWorkspacesResponse(response *getWorkspacesRestResponse) *GetWorkspacesResponse {
	workspaces := make([]*Workspace, 0)
	if response.Workspaces.Workspace != nil {
		for _, workspace := range response.Workspaces.Workspace {
			workspaces = append(workspaces, restWorkspaceToWorkspace(workspace))
		}
	}

	return &GetWorkspacesResponse{
		Workspaces: workspaces,
	}
}

// restWorkspaceToWorkspace converts a restWorkspace into a Workspace
func restWorkspaceToWorkspace(restWorkspace *restWorkspace) *Workspace {
	if restWorkspace != nil {
		return &Workspace{
			Name: restWorkspace.Name,
			Href: restWorkspace.Href,
		}
	}

	return &Workspace{}
}

// newEmptyGetWorkspacesRestResponse creates a new getWorkspacesRestResponse with all pointers initialised
// this is useful for passing to unmarshallers
func newEmptyGetWorkspacesRestResponse() *getWorkspacesRestResponse {
	return &getWorkspacesRestResponse{
		Workspaces: &restWorkspaces{
			Workspace: make([]*restWorkspace, 0),
		},
	}
}
