GO ?= go

go/test:
	CGO_ENABLED=1 $(GO) test -v ./...

go/build:
	CGO_ENABLED=1 $(GO) build ./...
