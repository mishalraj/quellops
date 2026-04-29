FROM golang:1.24 AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/aidbg-agent .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/aidbg-agent /aidbg-agent
ENTRYPOINT ["/aidbg-agent"]
