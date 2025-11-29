FROM golang:1.25-alpine AS builder

ARG EXAMPLE=wordcount

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN cd examples/${EXAMPLE} && go build -o /app/app .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/app .
COPY examples/data ./data

CMD ["./app"]
