package collector

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func podObj(name, namespace, nodeName string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func TestCollectExcludesUnchargeablePods(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		podObj("running", "team-a", "node-1", corev1.PodRunning),
		podObj("pending-scheduled", "team-a", "node-1", corev1.PodPending),
		podObj("succeeded", "team-a", "node-1", corev1.PodSucceeded),
		podObj("failed", "team-b", "node-1", corev1.PodFailed),
		podObj("unscheduled", "team-b", "", corev1.PodPending),
	)

	c := &Collector{client: client}
	info, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(info.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(info.Nodes))
	}
	// Legacy collection behavior is unchanged: every scheduled pod, including
	// terminated ones, remains attached to the node.
	gotPods := info.Nodes[0].Pods
	if len(gotPods) != 4 {
		t.Fatalf("expected 4 scheduled pods (legacy behavior), got %d: %+v", len(gotPods), gotPods)
	}

	// The opt-in exclusion diagnostic classifies every unchargeable pod.
	reasons := map[string]string{}
	for _, ex := range info.ExcludedPods {
		reasons[ex.Name] = ex.Reason
	}
	want := map[string]string{
		"succeeded":   ExcludeReasonTerminated,
		"failed":      ExcludeReasonTerminated,
		"unscheduled": ExcludeReasonNoNode,
	}
	if len(reasons) != len(want) {
		t.Fatalf("expected %d exclusions, got %d: %+v", len(want), len(reasons), info.ExcludedPods)
	}
	for name, reason := range want {
		if reasons[name] != reason {
			t.Errorf("pod %q reason = %q, want %q", name, reasons[name], reason)
		}
	}
}

func TestDeriveExcludedPods(t *testing.T) {
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "ok", Namespace: "a"}, Spec: corev1.PodSpec{NodeName: "n1"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		{ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "a"}, Spec: corev1.PodSpec{NodeName: "n1"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		{ObjectMeta: metav1.ObjectMeta{Name: "dead", Namespace: "a"}, Spec: corev1.PodSpec{NodeName: "n1"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}},
		{ObjectMeta: metav1.ObjectMeta{Name: "waiting", Namespace: "a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		{ObjectMeta: metav1.ObjectMeta{Name: "orphan"}, Spec: corev1.PodSpec{NodeName: "n1"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	}
	excluded := DeriveExcludedPods(pods)
	byName := map[string]ExcludedPod{}
	for _, e := range excluded {
		byName[e.Name] = e
	}
	if _, ok := byName["ok"]; ok {
		t.Error("running scheduled pod must not be excluded")
	}
	if byName["done"].Reason != ExcludeReasonTerminated || byName["dead"].Reason != ExcludeReasonTerminated {
		t.Errorf("terminated exclusion wrong: %+v %+v", byName["done"], byName["dead"])
	}
	if byName["waiting"].Reason != ExcludeReasonNoNode {
		t.Errorf("unscheduled pod reason = %q, want %q", byName["waiting"].Reason, ExcludeReasonNoNode)
	}
	if byName["orphan"].Reason != ExcludeReasonNoNamespace {
		t.Errorf("no-namespace pod reason = %q, want %q", byName["orphan"].Reason, ExcludeReasonNoNamespace)
	}
}

func TestCollectExcludesPodWithoutNamespace(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		podObj("orphan", "", "node-1", corev1.PodRunning),
	)

	c := &Collector{client: client}
	info, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	// Legacy collection still attaches the scheduled pod to the node.
	if len(info.Nodes[0].Pods) != 1 {
		t.Fatalf("expected the scheduled pod to remain attached, got %+v", info.Nodes[0].Pods)
	}
	// The diagnostic flags it so the pool engine can refuse to charge it.
	if len(info.ExcludedPods) != 1 || info.ExcludedPods[0].Reason != ExcludeReasonNoNamespace {
		t.Fatalf("expected no-namespace exclusion, got %+v", info.ExcludedPods)
	}
}

func TestCollectNamespaceLabels(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: "team-a", Labels: map[string]string{"team": "platform", "env": "prod"},
		}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: "team-b", Labels: map[string]string{"team": "data"},
		}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-c"}},
	)

	c := &Collector{client: client}
	labels, err := c.collectNamespaceLabels(ctx)
	if err != nil {
		t.Fatalf("collectNamespaceLabels: %v", err)
	}
	if labels["team-a"]["team"] != "platform" {
		t.Errorf("team-a team label = %q, want platform", labels["team-a"]["team"])
	}
	if _, ok := labels["team-c"]["team"]; ok {
		t.Error("team-c should have no team label")
	}
	if len(labels) != 3 {
		t.Errorf("expected 3 namespaces, got %d", len(labels))
	}
}

// TestCollectNamespaceLabelPermissionFailure simulates a ServiceAccount
// without namespace get/list permission. The label read reports an error but
// the analysis itself must not fail.
func TestCollectNamespaceLabelPermissionFailure(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		podObj("app", "restricted-ns", "node-1", corev1.PodRunning),
	)
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "namespaces"}, "restricted-ns", nil)
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, forbidden
	})
	client.PrependReactor("list", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, forbidden
	})

	c := &Collector{client: client, namespace: "restricted-ns"}

	// The label read fails ...
	_, err := c.collectNamespaceLabels(ctx)
	if err == nil {
		t.Fatal("expected permission error, got nil")
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected apierrors.StatusError, got %T: %v", err, err)
	}

	// ... but the full analysis still succeeds and records the diagnostic.
	info, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect must not fail when namespace labels are unreadable: %v", err)
	}
	if info.NamespaceLabelError == "" {
		t.Error("expected NamespaceLabelError to be recorded")
	}
	if len(info.NamespaceLabels) != 0 {
		t.Errorf("expected no labels, got %+v", info.NamespaceLabels)
	}
	if len(info.Nodes[0].Pods) != 1 {
		t.Errorf("pod collection must be unaffected by the label permission error")
	}
}
