VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	@go build -ldflags "-X main.version=$(VERSION)" -o db-exec .

deploy: build
	@mv -v db-exec /usr/local/bin/
	@echo "Deployed Successfully"
