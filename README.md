[![Build Status](https://travis-ci.org/adbourne/geoserver-client-go.svg?branch=master)](https://travis-ci.org/adbourne/geoserver-client-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/adbourne/geoserver-client-go)](https://goreportcard.com/report/github.com/adbourne/geoserver-client-go)
[![GoDoc](https://godoc.org/github.com/adbourne/geoserver-client-go?status.svg)](https://godoc.org/github.com/adbourne/geoserver-client-go)

# geoserver-client-go
A native Golang Geoserver client. It interacts with Geoserver via its REST API.

Supported versions of Geoserver:

| Version | Supported?   |
| ---     | ---          |
| v2.10.x | &#10003;     |
| v2.11.x | &#10003;     |
| v2.12.x | **UNTESTED** |


## Example
See [client_integration_test.go](geoserver/client_integration_test.go) for working examples of how to use the client.

### Logging
As there is currently no defacto-standard logging in Golang just yet, a function is provided to bridge the gap between
the client's logging and whatever logger your project is using. See [logger.go](geoserver/logger.go) for the details.
 
## Testing
Testing is performed using actual instances of Geoserver, packaged in a Docker container. The Geoserver image used 
[can be found here](https://github.com/adbourne/docker-geoserver). The Geoserver instance is run against a Postgis 
database, the docker container for that [can be found here](https://github.com/appropriate/docker-postgis).
