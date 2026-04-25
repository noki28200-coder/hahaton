FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /app/service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /app/service /service
CMD ["/service"]
