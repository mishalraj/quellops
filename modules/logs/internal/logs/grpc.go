package logs

import (
	"context"
	"crypto/subtle"
	"fmt"
	"strings"
	"time"

	logsv1 "github.com/mishalraj/infra-project-poc-mishalraj/modules/logs/gen/logsv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type GRPCServer struct {
	logsv1.UnimplementedLogsServiceServer
	service  *Service
	adaptive *AdaptiveFetcher
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service, adaptive: NewAdaptiveFetcher(service)}
}

func AuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/Health") {
			return handler(ctx, request)
		}
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		provided := ""
		if len(values) == 1 {
			provided = values[0]
		}
		expected := "Bearer " + token
		if token == "" || len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "log service authentication required")
		}
		return handler(ctx, request)
	}
}

func (s *GRPCServer) Health(context.Context, *logsv1.HealthRequest) (*logsv1.HealthResponse, error) {
	return &logsv1.HealthResponse{Ok: true, Version: "0.2.0"}, nil
}

func (s *GRPCServer) ListSources(context.Context, *logsv1.ListSourcesRequest) (*logsv1.ListSourcesResponse, error) {
	response := &logsv1.ListSourcesResponse{}
	for _, source := range s.service.ListSources() {
		response.Sources = append(response.Sources, &logsv1.Source{Name: source.Name, Provider: source.Provider})
	}
	return response, nil
}

func (s *GRPCServer) Fetch(ctx context.Context, request *logsv1.FetchRequest) (*logsv1.FetchResponse, error) {
	result, err := s.service.Fetch(ctx, request.Source, request.StartUnixS, request.EndUnixS, int(request.Limit))
	if err != nil {
		return nil, rpcError(err)
	}
	return fetchProto(result)
}

func (s *GRPCServer) Investigate(ctx context.Context, request *logsv1.InvestigateRequest) (*logsv1.InvestigationResponse, error) {
	result, err := s.adaptive.Investigate(ctx, request.Source, request.StartUnixS, request.EndUnixS, int(request.TargetRecords), request.Focus)
	if err != nil {
		return nil, rpcError(err)
	}
	return investigationProto(result)
}

func (s *GRPCServer) FetchComplete(ctx context.Context, request *logsv1.FetchCompleteRequest) (*logsv1.InvestigationResponse, error) {
	result, err := s.adaptive.FetchComplete(ctx, request.Source, request.StartUnixS, request.EndUnixS)
	if err != nil {
		return nil, rpcError(err)
	}
	return investigationProto(result)
}

func (s *GRPCServer) ApplySource(_ context.Context, request *logsv1.ApplySourceRequest) (*logsv1.ApplySourceResponse, error) {
	values := map[string]any{}
	if request.Config != nil {
		values = request.Config.AsMap()
	}
	if err := s.service.ApplySource(request.Name, request.Provider, values, request.Credentials); err != nil {
		return nil, rpcError(err)
	}
	return &logsv1.ApplySourceResponse{Source: &logsv1.Source{Name: request.Name, Provider: request.Provider}}, nil
}

func (s *GRPCServer) DeleteSource(_ context.Context, request *logsv1.DeleteSourceRequest) (*logsv1.DeleteSourceResponse, error) {
	return &logsv1.DeleteSourceResponse{Deleted: s.service.DeleteSource(request.Name)}, nil
}

func (s *GRPCServer) TestSource(ctx context.Context, request *logsv1.TestSourceRequest) (*logsv1.TestSourceResponse, error) {
	cfg, ok := s.service.source(request.Name)
	if !ok {
		return nil, rpcError(fmt.Errorf("unknown log source"))
	}
	end := float64(time.Now().Unix())
	start := end - float64(cfg.InitialWindow)
	result, err := s.service.Fetch(ctx, request.Name, start, end, min(1, cfg.MaxRecords))
	if err != nil {
		return nil, rpcError(err)
	}
	return &logsv1.TestSourceResponse{Ok: true, Provider: cfg.Provider, Records: uint32(len(result.Records)), Requests: uint32(result.Requests), BytesReceived: uint64(result.BytesReceived)}, nil
}

func (s *GRPCServer) Discover(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	name := stringify(request.AsMap()["source"])
	result, err := s.service.Discover(ctx, name)
	if err != nil {
		return nil, rpcError(err)
	}
	values := make([]any, 0, len(result.Scopes))
	for _, scope := range result.Scopes {
		values = append(values, map[string]any{"kind": scope.Kind, "key": scope.Key, "value": scope.Value, "label": scope.Label})
	}
	warnings := make([]any, 0, len(result.Warnings))
	for _, warning := range result.Warnings {
		warnings = append(warnings, warning)
	}
	response, err := structpb.NewStruct(map[string]any{"source": result.Source, "provider": result.Provider, "scopes": values, "warnings": warnings, "requests": result.Requests})
	if err != nil {
		return nil, status.Error(codes.Internal, "discovery result could not be encoded")
	}
	return response, nil
}

func rpcError(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }

func recordProto(record Record) (*logsv1.LogRecord, error) {
	attributes, err := structpb.NewStruct(record.Attributes)
	if err != nil {
		return nil, fmt.Errorf("normalized attributes could not be encoded")
	}
	return &logsv1.LogRecord{Timestamp: record.Timestamp, Message: record.Message, Source: record.Source, Id: record.ID, Attributes: attributes}, nil
}

func fetchProto(result FetchResult) (*logsv1.FetchResponse, error) {
	response := &logsv1.FetchResponse{Source: result.Source, Provider: result.Provider, Start: result.Start, End: result.End, Truncated: result.Truncated, Warnings: result.Warnings, Requests: uint32(result.Requests), BytesReceived: uint64(result.BytesReceived)}
	for _, record := range result.Records {
		converted, err := recordProto(record)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Records = append(response.Records, converted)
	}
	return response, nil
}

func investigationProto(result Investigation) (*logsv1.InvestigationResponse, error) {
	response := &logsv1.InvestigationResponse{Source: result.Source, Provider: result.Provider, ScopeStart: result.ScopeStart, ScopeEnd: result.ScopeEnd, Strategy: result.Strategy, RequestedStart: result.RequestedStart, TargetRecords: uint32(result.TargetRecords), TargetReached: result.TargetReached, CoverageComplete: result.CoverageComplete, BackendPartial: result.BackendPartial, StopReason: result.StopReason, Warnings: result.Warnings, Usage: &logsv1.Usage{Fetches: uint32(result.Usage.Fetches), BackendRequests: uint32(result.Usage.BackendRequests), BytesReceived: uint64(result.Usage.BytesReceived), Records: uint32(result.Usage.Records)}}
	for _, record := range result.Records {
		converted, err := recordProto(record)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Records = append(response.Records, converted)
	}
	for _, attempt := range result.Attempts {
		response.Attempts = append(response.Attempts, &logsv1.Attempt{Start: attempt.Start, End: attempt.End, Limit: uint32(attempt.Limit), Records: uint32(attempt.Records), Truncated: attempt.Truncated, Warnings: attempt.Warnings})
	}
	return response, nil
}
