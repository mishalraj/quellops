package logs

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGRPCAuthentication(t *testing.T) {
	interceptor := AuthInterceptor("secret")
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/aidbg.logs.v1.LogsService/ListSources"}
	if _, err := interceptor(context.Background(), nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if value, err := interceptor(ctx, nil, info, handler); err != nil || value != "ok" {
		t.Fatalf("expected authorized call: %v %v", value, err)
	}
	if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/aidbg.logs.v1.LogsService/Health"}, handler); err != nil {
		t.Fatalf("health should not require a token: %v", err)
	}
}

func TestGRPCDiscoveryStructEncoding(t *testing.T) {
	transport := &fakeTransport{responses: [][]byte{
		[]byte(`{"status":"success","data":["service"]}`),
		[]byte(`{"status":"success","data":["checkout"]}`),
	}}
	service := NewService(&Config{Sources: map[string]*SourceConfig{}}, transport)
	if err := service.ApplySource("prod", "loki", map[string]any{"url": "https://loki.example.com"}, nil); err != nil {
		t.Fatal(err)
	}
	request, _ := structpb.NewStruct(map[string]any{"source": "prod"})
	response, err := NewGRPCServer(service).Discover(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	values := response.AsMap()
	if values["provider"] != "loki" || len(values["scopes"].([]any)) != 1 {
		t.Fatalf("unexpected discovery response: %#v", values)
	}
}
