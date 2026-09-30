FROM golang:1.27-alpine
WORKDIR /src
COPY go.mod go.sum main.go ./
RUN go build -o /usr/local/bin/client .
