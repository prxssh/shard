FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o shard cmd/shard/main.go 

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/shard .
RUN mkdir data && echo "Hello distributed world. This is a text file to be processed by the shard system." > data/input.txt

CMD ["./shard"]
