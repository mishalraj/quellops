//go:build !legacy
// +build !legacy

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const agentVersion = "0.2.0"

type Event struct {
	EventID   string            `json:"event_id"`
	Type      string            `json:"type"`
	AtUnixMs  int64             `json:"at_unix_ms"`
	Namespace string            `json:"namespace,omitempty"`
	Name      string            `json:"name,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Message   string            `json:"message,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

type scopeState struct{ value atomic.Value }

func newScopeState() *scopeState {
	s := &scopeState{}
	s.value.Store(map[string]struct{}{})
	return s
}

func (s *scopeState) Set(namespaces []string) {
	next := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		next[namespace] = struct{}{}
	}
	s.value.Store(next)
}

func (s *scopeState) Allows(namespace string) bool {
	_, ok := s.value.Load().(map[string]struct{})[namespace]
	return ok
}

type errorState struct {
	mu      sync.Mutex
	message string
}

func (s *errorState) Set(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.message = ""
		return
	}
	s.message = truncate(err.Error(), 1000)
}

func (s *errorState) Get() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.message
}

func main() {
	backendURL := flag.String("backend-url", getenv("AIDBG_BACKEND_URL", "http://aidbg-backend.aidbg:8088"), "aidbg backend base URL")
	clusterName := flag.String("cluster", getenv("AIDBG_CLUSTER_NAME", ""), "friendly cluster name")
	clusterIDFlag := flag.String("cluster-id", getenv("AIDBG_CLUSTER_ID", ""), "stable cluster identifier")
	token := flag.String("token", getenv("AIDBG_K8S_AGENT_TOKEN", ""), "agent bootstrap token")
	kubeconfig := flag.String("kubeconfig", getenv("AIDBG_KUBECONFIG", ""), "optional kubeconfig path outside a cluster")
	queueLimit := flag.Int("queue-limit", getenvInt("AIDBG_K8S_QUEUE_LIMIT", 2000), "maximum events held in memory")
	flag.Parse()

	if *token == "" {
		fmt.Fprintln(os.Stderr, "AIDBG_K8S_AGENT_TOKEN is required")
		os.Exit(2)
	}
	cfg, err := kubernetesConfig(*kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load Kubernetes configuration: %v\n", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	clusterID, err := clusterIdentity(ctx, clientset, *clusterIDFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover cluster identity: %v\n", err)
		os.Exit(1)
	}
	if *clusterName == "" {
		*clusterName = "cluster-" + shortID(clusterID)
	}

	queue := newEventQueue(*queueLimit)
	scope := newScopeState()
	errors := &errorState{}
	control := newControlClient(*backendURL, *token, 15*time.Second)
	var sequence uint64
	enqueue := func(event Event) {
		if !scope.Allows(event.Namespace) {
			return
		}
		event.AtUnixMs = time.Now().UnixMilli()
		event.EventID = fmt.Sprintf("%s-%d-%d", clusterID, event.AtUnixMs, atomic.AddUint64(&sequence, 1))
		event.Labels = safeLabels(event.Labels)
		event.Message = truncate(event.Message, 2000)
		queue.Push(event)
	}

	// Registration starts with an empty scope. Until the backend returns an
	// administrator-approved namespace list, informer callbacks forward nothing.
	if config, registerErr := registerCluster(ctx, control, clientset, clusterID, *clusterName, queue, errors); registerErr != nil {
		errors.Set(registerErr)
		fmt.Fprintf(os.Stderr, "initial registration failed: %v\n", registerErr)
	} else {
		scope.Set(config.Namespaces)
	}

	stopCh := make(chan struct{})
	factory := informers.NewSharedInformerFactory(clientset, 60*time.Second)
	installHandlers(factory, enqueue)
	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		controlLoop(ctx, control, clientset, clusterID, *clusterName, queue, scope, errors)
	}()
	go func() {
		defer workers.Done()
		deliveryLoop(ctx, control, clusterID, queue, errors)
	}()
	<-ctx.Done()
	close(stopCh)
	workers.Wait()
}

func kubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func clusterIdentity(ctx context.Context, client kubernetes.Interface, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	namespace, err := client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return "k8s:" + string(namespace.UID), nil
}

func shortID(value string) string {
	value = strings.TrimPrefix(value, "k8s:")
	if len(value) > 8 {
		return value[:8]
	}
	return value
}

func discover(ctx context.Context, client kubernetes.Interface) ([]string, []Workload, string, error) {
	namespaceList, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, "", err
	}
	namespaces := make([]string, 0, len(namespaceList.Items))
	for _, item := range namespaceList.Items {
		namespaces = append(namespaces, item.Name)
	}
	sort.Strings(namespaces)
	workloads := make([]Workload, 0)
	deployments, err := client.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, "", err
	}
	for _, item := range deployments.Items {
		workloads = append(workloads, Workload{Namespace: item.Namespace, Name: item.Name, Kind: "Deployment"})
	}
	statefulSets, err := client.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, "", err
	}
	for _, item := range statefulSets.Items {
		workloads = append(workloads, Workload{Namespace: item.Namespace, Name: item.Name, Kind: "StatefulSet"})
	}
	daemonSets, err := client.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, "", err
	}
	for _, item := range daemonSets.Items {
		workloads = append(workloads, Workload{Namespace: item.Namespace, Name: item.Name, Kind: "DaemonSet"})
	}
	sort.Slice(workloads, func(i, j int) bool {
		if workloads[i].Namespace != workloads[j].Namespace {
			return workloads[i].Namespace < workloads[j].Namespace
		}
		if workloads[i].Kind != workloads[j].Kind {
			return workloads[i].Kind < workloads[j].Kind
		}
		return workloads[i].Name < workloads[j].Name
	})
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return nil, nil, "", err
	}
	return namespaces, workloads, version.GitVersion, nil
}

func registerCluster(ctx context.Context, control *controlClient, client kubernetes.Interface, clusterID, name string, queue *eventQueue, errors *errorState) (AgentConfig, error) {
	namespaces, workloads, version, err := discover(ctx, client)
	if err != nil {
		return AgentConfig{}, err
	}
	depth, dropped := queue.Stats()
	return control.Register(ctx, Registration{
		ClusterID: clusterID, Name: name, AgentVersion: agentVersion,
		KubernetesVersion: version, Namespaces: namespaces, Workloads: workloads,
		QueueDepth: depth, DroppedEvents: dropped, LastError: errors.Get(),
	})
}

func controlLoop(ctx context.Context, control *controlClient, client kubernetes.Interface, clusterID, name string, queue *eventQueue, scope *scopeState, errors *errorState) {
	heartbeat := time.NewTicker(30 * time.Second)
	discovery := time.NewTicker(5 * time.Minute)
	defer heartbeat.Stop()
	defer discovery.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			depth, dropped := queue.Stats()
			config, err := control.Heartbeat(ctx, clusterID, depth, dropped, errors.Get())
			if err != nil {
				errors.Set(err)
				continue
			}
			scope.Set(config.Namespaces)
			errors.Set(nil)
		case <-discovery.C:
			config, err := registerCluster(ctx, control, client, clusterID, name, queue, errors)
			if err != nil {
				errors.Set(err)
				continue
			}
			scope.Set(config.Namespaces)
			errors.Set(nil)
		}
	}
}

func deliveryLoop(ctx context.Context, control *controlClient, clusterID string, queue *eventQueue, errors *errorState) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		batch := queue.Peek(100)
		if len(batch) == 0 {
			backoff = time.Second
			continue
		}
		if err := control.SendEvents(ctx, clusterID, batch); err != nil {
			errors.Set(err)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		queue.Ack(len(batch))
		errors.Set(nil)
		backoff = 200 * time.Millisecond
	}
}

func installHandlers(factory informers.SharedInformerFactory, enqueue func(Event)) {
	deployments := factory.Apps().V1().Deployments().Informer()
	_, _ = deployments.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			meta := mustMeta(obj)
			desired, updated, ready, _ := deploymentReplicaSummary(obj)
			enqueue(Event{Type: "k8s.deployment.observed", Namespace: meta.Namespace, Name: meta.Name, Kind: "Deployment", Labels: meta.Labels,
				Fields: map[string]string{"desired_replicas": strconv.Itoa(int(desired)), "updated_replicas": strconv.Itoa(int(updated)), "ready_replicas": strconv.Itoa(int(ready))}})
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldDesired, oldUpdated, oldReady, _ := deploymentReplicaSummary(oldObj)
			desired, updated, ready, _ := deploymentReplicaSummary(newObj)
			if oldDesired == desired && oldUpdated == updated && oldReady == ready {
				return
			}
			meta := mustMeta(newObj)
			eventType := "deploy.started"
			if desired == ready && desired == updated {
				eventType = "deploy.completed"
			} else if desired > 0 && ready == 0 {
				eventType = "deploy.failed"
			}
			enqueue(Event{Type: eventType, Namespace: meta.Namespace, Name: meta.Name, Kind: "Deployment", Labels: meta.Labels,
				Fields: map[string]string{"desired_replicas": strconv.Itoa(int(desired)), "updated_replicas": strconv.Itoa(int(updated)), "ready_replicas": strconv.Itoa(int(ready)), "resource_version": meta.ResourceVersion}})
		},
	})

	pods := factory.Core().V1().Pods().Informer()
	_, _ = pods.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: func(oldObj, newObj any) {
		oldState := summarizePod(oldObj)
		state := summarizePod(newObj)
		if oldState == state {
			return
		}
		meta := mustMeta(newObj)
		fields := map[string]string{"phase": state.phase, "ready": strconv.FormatBool(state.ready), "restarts": strconv.Itoa(int(state.restarts))}
		if state.waiting != "" {
			fields["waiting_reason"] = state.waiting
		}
		eventType := "k8s.pod.updated"
		if state.restarts > oldState.restarts {
			eventType = "k8s.pod.restarted"
		}
		if state.waiting == "CrashLoopBackOff" || state.waiting == "ImagePullBackOff" || state.phase == "Failed" {
			eventType = "k8s.pod.failed"
		}
		enqueue(Event{Type: eventType, Namespace: meta.Namespace, Name: meta.Name, Kind: "Pod", Reason: state.waiting, Labels: meta.Labels, Fields: fields})
	}})

	events := factory.Core().V1().Events().Informer()
	_, _ = events.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(obj any) {
		meta, reason, message, involvedKind, involvedName, eventType := eventSummary(obj)
		if eventType != corev1.EventTypeWarning && !usefulEventReason(reason) {
			return
		}
		enqueue(Event{Type: "k8s.event", Namespace: meta.Namespace, Name: involvedName, Kind: involvedKind, Reason: reason, Message: message,
			Fields: map[string]string{"event_type": eventType}})
	}})

	hpas := factory.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
	_, _ = hpas.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: func(oldObj, newObj any) {
		oldHPA, oldOK := oldObj.(*autoscalingv2.HorizontalPodAutoscaler)
		hpa, ok := newObj.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok || !oldOK || (oldHPA.Status.CurrentReplicas == hpa.Status.CurrentReplicas && oldHPA.Status.DesiredReplicas == hpa.Status.DesiredReplicas) {
			return
		}
		meta := mustMeta(newObj)
		enqueue(Event{Type: "k8s.hpa.changed", Namespace: meta.Namespace, Name: meta.Name, Kind: "HorizontalPodAutoscaler", Labels: meta.Labels,
			Fields: map[string]string{"current_replicas": strconv.Itoa(int(hpa.Status.CurrentReplicas)), "desired_replicas": strconv.Itoa(int(hpa.Status.DesiredReplicas)), "scale_target": hpa.Spec.ScaleTargetRef.Name}})
	}})
}

func usefulEventReason(reason string) bool {
	switch reason {
	case "BackOff", "Failed", "FailedMount", "FailedScheduling", "Unhealthy", "OOMKilling", "Evicted", "Killing", "ScalingReplicaSet":
		return true
	default:
		return false
	}
}

func safeLabels(labels map[string]string) map[string]string {
	allowed := map[string]bool{"app": true, "app.kubernetes.io/name": true, "app.kubernetes.io/component": true, "app.kubernetes.io/instance": true}
	result := map[string]string{}
	for key, value := range labels {
		if allowed[key] {
			result[key] = truncate(value, 253)
		}
	}
	return result
}

func truncate(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err == nil && value > 0 {
		return value
	}
	return fallback
}
