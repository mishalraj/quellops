package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	logsv1 "github.com/mishalraj/infra-project-poc-mishalraj/modules/logs/gen/logsv1"
	"github.com/mishalraj/infra-project-poc-mishalraj/modules/logs/internal/logs"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := env("AIDBG_LOG_CONFIG", "/etc/aidbg/logs.yaml")
	grpcAddress := env("AIDBG_LOG_GRPC_BIND", "0.0.0.0:8091")
	healthAddress := env("AIDBG_LOG_HEALTH_BIND", "0.0.0.0:8092")
	token := os.Getenv("AIDBG_LOG_SERVICE_TOKEN")
	if token == "" {
		return fmt.Errorf("AIDBG_LOG_SERVICE_TOKEN is required")
	}
	config, err := logs.LoadConfig(configPath)
	if err != nil {
		return err
	}
	service := logs.NewService(config, nil)
	listener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return fmt.Errorf("cannot listen for gRPC: %w", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(logs.AuthInterceptor(token)))
	logsv1.RegisterLogsServiceServer(grpcServer, logs.NewGRPCServer(service))

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ok": true})
	})
	healthServer := &http.Server{Addr: healthAddress, Handler: healthMux, ReadHeaderTimeout: 5 * time.Second}
	errors := make(chan error, 2)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errors <- err
		}
	}()
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errors <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errors:
		return err
	case <-signals:
	}
	grpcServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return healthServer.Shutdown(ctx)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
