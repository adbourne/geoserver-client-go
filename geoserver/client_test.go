package geoserver

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRestGeoserverClientImplementsClient(t *testing.T) {
	var client Client // nolint: megacheck
	client = NewRestGeoserverClient(NewStdOutLogger(), NewTestHTTPClient(), "", "", "") // nolint: megacheck
	assert.NotNil(t, client)
}
