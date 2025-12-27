BINARY := shard
BUILD_DIR := build

.PHONY: clean format test

clean: 
	go clean 
	rm -rf build

format: 
	golines -m 100 -t 8 --shorten-comments -w .
	gofmt -w .

test: 
	go test ./...
