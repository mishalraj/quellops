FROM golang:1.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go mod tidy
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/quellops-agent .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/quellops-agent /quellops-agent
ENTRYPOINT ["/quellops-agent"]
