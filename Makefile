.PHONY: build clean deploy

clean:
	rm -rf ./bin

build:
	go mod vendor
	env GOOS=linux go build -ldflags="-s -w" -o .bin/reader reader/main.go
	env GOOS=linux go build -ldflags="-s -w" -o .bin/writer writer/main.go

deploy: clean build
	sls deploy --verbose

deploy-%: clean build
	sls deploy --verbose function -f $*

invoke-%:
	sls invoke --function $*

remove:
	sls remove