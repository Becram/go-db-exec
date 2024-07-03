build:
	@go build -o db-exec .

deploy: build
	@mv -v db-exec /usr/local/bin/
	@echo "Deployed Successfully"
