SRC_DIR := cmd/shard
BINARY := shard
BUILD_DIR := build

MASTER_PORT := 1234
WORKER_PORT := 8080
MASTER_ADDR := localhost:${MASTER_PORT}
INPUT_FILE := pg-book.txt

.PHONY: all clean format test build run-master run-worker docker-up docker-down

all: clean format build

build:
	mkdir -p ${BUILD_DIR}
	go build -o ${BUILD_DIR}/${BINARY} ${SRC_DIR}/main.go

run-master:
	go run ${SRC_DIR}/main.go -mode master -port ${MASTER_PORT} -nreduce 10 ${INPUT_FILE}

run-worker:
	go run ${SRC_DIR}/main.go -mode worker -port ${WORKER_PORT} -master ${MASTER_ADDR}

docker-build:
	docker compose build

docker-up:
	docker compose up --build

docker-scale:
	docker compose up -d --scale worker=5

docker-down:
	docker compose down

clean:
	go clean
	rm -rf ${BUILD_DIR}

format:
	golines -m 100 -t 8 --shorten-comments -w .
	gofmt -w .

tidy:
	go mod tidy

test:
	go test -v -race ./...
