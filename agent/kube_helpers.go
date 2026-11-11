package main

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mustMeta(obj any) *metav1.ObjectMeta {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return &metav1.ObjectMeta{}
	}
	return &metav1.ObjectMeta{
		Name:            accessor.GetName(),
		Namespace:       accessor.GetNamespace(),
		Labels:          accessor.GetLabels(),
		ResourceVersion: accessor.GetResourceVersion(),
		Annotations:     accessor.GetAnnotations(),
		Generation:      accessor.GetGeneration(),
	}
}

func mustEventSummary(obj any) (*metav1.ObjectMeta, string, string, string, string) {
	ev, ok := obj.(*v1.Event)
	if !ok {
		return &metav1.ObjectMeta{}, "", "", "", ""
	}
	meta := &ev.ObjectMeta
	involvedKind := ev.InvolvedObject.Kind
	involvedName := ev.InvolvedObject.Name
	return meta, ev.Reason, ev.Message, involvedKind, involvedName
}

func eventSummary(obj any) (*metav1.ObjectMeta, string, string, string, string, string) {
	ev, ok := obj.(*v1.Event)
	if !ok {
		return &metav1.ObjectMeta{}, "", "", "", "", ""
	}
	return &ev.ObjectMeta, ev.Reason, ev.Message, ev.InvolvedObject.Kind, ev.InvolvedObject.Name, ev.Type
}

type podSummary struct {
	phase    string
	ready    bool
	restarts int32
	waiting  string
}

func summarizePod(obj any) podSummary {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return podSummary{}
	}
	result := podSummary{phase: string(pod.Status.Phase)}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady {
			result.ready = condition.Status == v1.ConditionTrue
			break
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		result.restarts += status.RestartCount
		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			result.waiting = status.State.Waiting.Reason
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		result.restarts += status.RestartCount
		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			result.waiting = status.State.Waiting.Reason
		}
	}
	return result
}

func deploymentReplicaSummary(obj any) (desired int32, updated int32, ready int32, ok bool) {
	deploy, ok := obj.(*appsv1.Deployment)
	if !ok {
		return 0, 0, 0, false
	}
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	}
	return desired, deploy.Status.UpdatedReplicas, deploy.Status.ReadyReplicas, true
}
