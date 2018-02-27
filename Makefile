# Determine this makefile's path.
# Be sure to place this BEFORE `include` directives, if any.
THIS_FILE := $(lastword $(MAKEFILE_LIST))

GOFMT_FILES?=$$(find . -name '*.go' | grep -v vendor)

#
default: dependencies build test

clean:
	rm -rf geoserver/bin/*; rm -rf pkg/*; rm -rf vendor/*

dependencies:
	dep ensure

fmt:
	gofmt -w $(GOFMT_FILES)

lint:
	gometalinter ./... | grep -v vendor/ | sed ''/warning/s//$$(printf "\033[33mwarning\033[0m")/'' | sed ''/error/s//$$(printf "\033[31merror\033[0m")/''

build:
	go install ./geoserver

mocks:
	mockery -dir services/ -all -case underscore

test:
	richgo test ./... -v --cover

.PHONY: test package