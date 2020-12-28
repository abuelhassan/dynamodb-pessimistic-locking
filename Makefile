.PHONY: build clean deploy

build:
	go mod vendor
	env GOOS=linux go build -ldflags="-s -w" -o .bin/reader reader/main.go
	env GOOS=linux go build -ldflags="-s -w" -o .bin/writer writer/main.go

clean:
	rm -rf ./bin

deploy: clean build
	sls deploy --verbose
