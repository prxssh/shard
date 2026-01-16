.PHONY: clean format test

clean: 
	go clean 
	rm -rf build

format: 
	golines -m 100 -t 8 --shorten-comments -w .
	gofmt -w .

test: 
	go test ./...

gen-proto:
	@if [ -z "$(FILE)" ]; then echo "Error: FILE is not set. Usage: make gen-proto FILE=path/to/file.proto"; exit 1; fi
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $(FILE)
