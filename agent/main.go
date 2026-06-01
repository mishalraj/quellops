package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type Event struct {
	Type      string            `json:"type"`
	AtUnixMs  int64             `json:"at_unix_ms"`
	Cluster   string            `json:"cluster,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Name      string            `json:"name,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Message   string            `json:"message,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

type ChangeEvent struct {
	Kind      string            `json:"kind"`
	AtUnixMs  int64             `json:"at_unix_ms"`
	Entity    string            `json:"entity"`
	Namespace string            `json:"namespace,omitempty"`
	Metadata  map[string]string `json:"metadata"`
}

func postJSON(ctx context.Context, endpoint string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("non-2xx from backend: %s", resp.Status)
	}
	return nil
}

func postEvent(ctx context.Context, endpoint string, ev Event) error {
	return postJSON(ctx, endpoint, ev)
}

func postChange(ctx context.Context, endpoint string, ev ChangeEvent) error {
	return postJSON(ctx, endpoint, ev)
}

func workloadEntity(labels map[string]string, name string) string {
	if labels == nil {
		return name
	}
	if app := labels["app"]; app != "" {
		return app
	}
	if app := labels["app.kubernetes.io/name"]; app != "" {
		return app
	}
	return name
}

func emitConfigChange(ctx context.Context, changeURL, clusterName, kind, namespace, name string, labels map[string]string, fields map[string]string) {
	entity := workloadEntity(labels, name)
	meta := map[string]string{
		"resource":  kind,
		"name":      name,
		"cluster":   clusterName,
		"operation": "updated",
	}
	for k, v := range fields {
		meta[k] = v
	}
	_ = postChange(ctx, changeURL, ChangeEvent{
		Kind:      "config.changed",
		AtUnixMs:  time.Now().UnixMilli(),
		Entity:    entity,
		Namespace: namespace,
		Metadata:  meta,
	})
}

func main() {
	var backendURL string
	var changeURL string
	var clusterName string
	flag.StringVar(&backendURL, "backend-url", getenv("AIDBG_BACKEND_EVENT_URL", "http://aidbg-backend.aidbg:8088/ingest/k8s"), "backend k8s ingest URL")
	flag.StringVar(&changeURL, "change-url", getenv("AIDBG_BACKEND_CHANGE_URL", "http://aidbg-backend.aidbg:8088/ingest/change"), "backend change ingest URL")
	flag.StringVar(&clusterName, "cluster", getenv("AIDBG_CLUSTER", ""), "cluster name (optional)")
	flag.Parse()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load in-cluster kube config: %v\n", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create k8s client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopCh := make(chan struct{})

	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	// Deployments
	deployInf := factory.Apps().V1().Deployments().Informer()
	_, _ = deployInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			meta := mustMeta(obj)
			ev := Event{
				Type:      "k8s.deployment.added",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "Deployment",
				Labels:    meta.Labels,
			}
			_ = postEvent(ctx, backendURL, ev)
		},
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			status := deploymentStatus(newObj)
			evType := "deploy.started"
			if status == "completed" {
				evType = "deploy.completed"
			} else if status == "failed" {
				evType = "deploy.failed"
			}
			fields := map[string]string{
				"resource_version": meta.ResourceVersion,
				"rollout_status":   status,
			}
			if desired, updated, ready, ok := deploymentReplicaSummary(newObj); ok {
				fields["desired_replicas"] = strconv.Itoa(int(desired))
				fields["updated_replicas"] = strconv.Itoa(int(updated))
				fields["ready_replicas"] = strconv.Itoa(int(ready))
			}
			ev := Event{
				Type:      evType,
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "Deployment",
				Labels:    meta.Labels,
				Fields:    fields,
			}
			_ = postEvent(ctx, backendURL, ev)
		},
	})

	// ReplicaSets
	rsInf := factory.Apps().V1().ReplicaSets().Informer()
	_, _ = rsInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			meta := mustMeta(obj)
			ev := Event{
				Type:      "k8s.replicaset.added",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "ReplicaSet",
				Labels:    meta.Labels,
			}
			_ = postEvent(ctx, backendURL, ev)
		},
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			ev := Event{
				Type:      "k8s.replicaset.updated",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "ReplicaSet",
				Labels:    meta.Labels,
				Fields: map[string]string{
					"resource_version": meta.ResourceVersion,
				},
			}
			_ = postEvent(ctx, backendURL, ev)
		},
	})

	// Pods
	podInf := factory.Core().V1().Pods().Informer()
	_, _ = podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			meta := mustMeta(obj)
			ev := Event{
				Type:      "k8s.pod.added",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "Pod",
				Labels:    meta.Labels,
			}
			_ = postEvent(ctx, backendURL, ev)
		},
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			ev := Event{
				Type:      "k8s.pod.updated",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      "Pod",
				Labels:    meta.Labels,
				Fields: map[string]string{
					"resource_version": meta.ResourceVersion,
				},
			}
			_ = postEvent(ctx, backendURL, ev)
		},
	})

	// Events
	evInf := factory.Core().V1().Events().Informer()
	_, _ = evInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			meta, reason, message, involvedKind, involvedName := mustEventSummary(obj)
			ev := Event{
				Type:      "k8s.event",
				AtUnixMs:  time.Now().UnixMilli(),
				Cluster:   clusterName,
				Namespace: meta.Namespace,
				Name:      meta.Name,
				Kind:      involvedKind,
				Reason:    reason,
				Message:   message,
				Fields: map[string]string{
					"involved_name": involvedName,
				},
			}
			_ = postEvent(ctx, backendURL, ev)
		},
	})

	// ConfigMaps
	cmInf := factory.Core().V1().ConfigMaps().Informer()
	_, _ = cmInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			emitConfigChange(ctx, changeURL, clusterName, "ConfigMap", meta.Namespace, meta.Name, meta.Labels, map[string]string{
				"resource_version": meta.ResourceVersion,
			})
		},
	})

	// Secrets (metadata only; never post secret data)
	secretInf := factory.Core().V1().Secrets().Informer()
	_, _ = secretInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			emitConfigChange(ctx, changeURL, clusterName, "Secret", meta.Namespace, meta.Name, meta.Labels, map[string]string{
				"resource_version": meta.ResourceVersion,
			})
		},
	})

	// HPAs
	hpaInf := factory.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
	_, _ = hpaInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			meta := mustMeta(newObj)
			fields := map[string]string{
				"resource_version": meta.ResourceVersion,
			}
			if hpa, ok := newObj.(*autoscalingv2.HorizontalPodAutoscaler); ok && hpa.Spec.ScaleTargetRef.Name != "" {
				fields["scale_target"] = hpa.Spec.ScaleTargetRef.Name
			}
			emitConfigChange(ctx, changeURL, clusterName, "HorizontalPodAutoscaler", meta.Namespace, meta.Name, meta.Labels, fields)
		},
	})

	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)

	// shutdown on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	close(stopCh)
	cancel()
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func deploymentStatus(obj any) string {
	m := mustMeta(obj)
	genS := m.Annotations["deployment.kubernetes.io/revision"]
	if genS == "" {
		return "in_progress"
	}
	if _, err := strconv.Atoi(genS); err != nil {
		return "in_progress"
	}
	return "completed"
}
