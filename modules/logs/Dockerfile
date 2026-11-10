FROM golang:1.24 AS builder
WORKDIR /src
COPY modules/logs/go.mod modules/logs/go.sum ./
RUN go mod download
COPY modules/logs/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aidbg-logs ./cmd/aidbg-logs

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/aidbg-logs /aidbg-logs
EXPOSE 8091 8092
ENTRYPOINT ["/aidbg-logs"]
