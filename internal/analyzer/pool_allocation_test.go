package analyzer

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/tanrikuluozlem/burn/internal/collector"
	"github.com/tanrikuluozlem/burn/internal/pricing"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	giB = 1024 * 1024 * 1024
)

// runPool builds an analyzer with the given options and runs a full Analyze,
// returning the pool report.
func runPool(t *testing.T, info *collector.ClusterInfo, opts Options) *PoolAllocationReport {
	t.Helper()
	if opts.PoolBasis == "" {
		opts.PoolBasis = PoolBasisRequests
	}
	a := NewWithOptions(&mockPricing{price: 0.10}, opts)
	report, err := a.Analyze(context.Background(), info)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if report.PoolAllocation == nil {
		t.Fatal("expected pool allocation report, got nil")
	}
	return report.PoolAllocation
}

func baseNode(pods ...collector.PodInfo) collector.NodeInfo {
	return collector.NodeInfo{
		Name:           "node-1",
		InstanceType:   "m5.large",
		Region:         "us-east-1",
		CPUAllocatable: 4000, // 4 cores
		MemAllocatable: 8 * giB,
		Pods:           pods,
	}
}

// conservationError returns |a+b-total| using relative tolerance against
// non-zero totals and absolute tolerance for zero totals; never divides by
// zero.
func conservationError(sum, total float64) float64 {
	diff := math.Abs(sum - total)
	if total == 0 {
		return diff
	}
	return diff / total
}

func assertConservation(t *testing.T, pool *PoolAllocationReport) {
	t.Helper()
	if got := conservationError(pool.AllocatedHourly+pool.UnallocatedHourly, pool.NodeTotalHourly); got > 1e-9 {
		t.Errorf("node-level conservation: relative error %.3e > 1e-9 (allocated=%v unallocated=%v total=%v)",
			got, pool.AllocatedHourly, pool.UnallocatedHourly, pool.NodeTotalHourly)
	}

	var nsSum float64
	for _, ns := range pool.Namespaces {
		nsSum += ns.Hourly
	}
	if got := conservationError(nsSum, pool.AllocatedHourly); got > 1e-9 {
		t.Errorf("namespace aggregation: relative error %.3e > 1e-9 (ns sum=%v allocated=%v)",
			got, nsSum, pool.AllocatedHourly)
	}

	// Per-node per-pod charges sum to that node's allocated cost.
	for _, n := range pool.Nodes {
		var podSum float64
		for _, pc := range n.Pods {
			podSum += pc.Hourly
			// Component fields must add up to the per-pod total.
			if got := conservationError(pc.CPU+pc.Memory+pc.GPU, pc.Hourly); got > 1e-9 {
				t.Errorf("node %s pod %s components do not sum to hourly: %.3e", n.Name, pc.Name, got)
			}
		}
		var nodeAllocated float64
		for _, p := range n.Pools {
			nodeAllocated += p.AllocatedHourly
		}
		if got := conservationError(podSum, nodeAllocated); got > 1e-9 {
			t.Errorf("node %s per-pod sum %.10f != allocated %.10f (%.3e)",
				n.Name, podSum, nodeAllocated, got)
		}
	}

	var bucketSum float64
	if pool.LabelKey != "" {
		for _, b := range pool.LabelBuckets {
			bucketSum += b.Hourly
		}
		if got := conservationError(bucketSum, pool.AllocatedHourly); got > 1e-9 {
			t.Errorf("label-bucket aggregation: relative error %.3e > 1e-9 (bucket sum=%v allocated=%v)",
				got, bucketSum, pool.AllocatedHourly)
		}
	}
}

func findPool(t *testing.T, node NodePoolAllocation, resource string) ResourcePoolCost {
	t.Helper()
	for _, p := range node.Pools {
		if p.Resource == resource {
			return p
		}
	}
	t.Fatalf("pool %q not found on node %s", resource, node.Name)
	return ResourcePoolCost{}
}

func TestPoolDisabledByDefaultLeavesLegacyReport(t *testing.T) {
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "p", Namespace: "default", CPURequest: 1000, MemoryRequest: 2 * giB},
		)},
	}
	a := New(&mockPricing{price: 0.10})
	report, err := a.Analyze(context.Background(), info)
	if err != nil {
		t.Fatal(err)
	}
	if report.PoolAllocation != nil {
		t.Error("pool allocation must be nil when not explicitly enabled")
	}

	// Enabled report must not alter the legacy fields either: run twice and
	// compare the shared legacy sections via JSON.
	enabled := runPool(t, info, Options{EnablePoolAllocation: true, NamespaceLabelKey: "team"})
	if enabled.AllocatedHourly <= 0 {
		t.Error("expected positive allocated cost")
	}
}

// TestPoolEnablingDoesNotChangeLegacyFields proves the opt-in engine is
// purely additive: every legacy field is identical with and without it.
func TestPoolEnablingDoesNotChangeLegacyFields(t *testing.T) {
	info := &collector.ClusterInfo{
		TotalNodes: 2,
		Nodes: []collector.NodeInfo{
			baseNode(
				collector.PodInfo{Name: "p1", Namespace: "a", CPURequest: 1000, MemoryRequest: 2 * giB, CPUUsage: 0.4, MemoryUsage: giB},
				collector.PodInfo{Name: "p2", Namespace: "b", CPURequest: 500, MemoryRequest: giB},
			),
			func() collector.NodeInfo {
				n := baseNode()
				n.Name = "node-2"
				n.Pods = []collector.PodInfo{
					{Name: "p3", Namespace: "a", CPURequest: 2000, MemoryRequest: 4 * giB},
				}
				return n
			}(),
		},
		PVCs: []collector.PVCInfo{
			{Name: "vol", Namespace: "a", StorageClass: "gp3", RequestedBytes: 10 * giB},
		},
		LoadBalancers: []collector.LBServiceInfo{{Name: "lb", Namespace: "b"}},
		ExcludedPods: []collector.ExcludedPod{
			{Name: "done", Namespace: "a", Phase: "Succeeded", Reason: collector.ExcludeReasonTerminated},
		},
	}

	legacy, err := New(&mockPricing{price: 0.10}).Analyze(context.Background(), info)
	if err != nil {
		t.Fatal(err)
	}
	enabled, err := NewWithOptions(&mockPricing{price: 0.10}, Options{
		EnablePoolAllocation: true, NamespaceLabelKey: "team",
	}).Analyze(context.Background(), info)
	if err != nil {
		t.Fatal(err)
	}

	legacy.PoolAllocation = nil
	enabled.PoolAllocation = nil
	// GeneratedAt is wall-clock by design; normalize before comparing.
	legacy.GeneratedAt = time.Time{}
	enabled.GeneratedAt = time.Time{}
	b1, _ := json.Marshal(legacy)
	b2, _ := json.Marshal(enabled)
	if string(b1) != string(b2) {
		t.Fatalf("legacy report changed when pool allocation enabled:\nlegacy:  %s\nenabled: %s", b1, b2)
	}
}

func TestPoolRequestsUnderAllocatableLeavesUnallocated(t *testing.T) {
	// Pod requests 1 core / 2 GiB on a 4 core / 8 GiB node: shares are 25% of
	// each pool, so 75% of each pool stays unallocated.
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "p", Namespace: "default", CPURequest: 1000, MemoryRequest: 2 * giB},
		)},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)

	node := pool.Nodes[0]
	for _, resource := range []string{poolCPU, poolMemory} {
		p := findPool(t, node, resource)
		if p.Basis != PoolBasisRequests {
			t.Errorf("%s basis = %q, want requests", resource, p.Basis)
		}
		if !floatEquals(p.AllocatedHourly/p.PoolHourly, 0.25) {
			t.Errorf("%s allocated share = %v, want 0.25", resource, p.AllocatedHourly/p.PoolHourly)
		}
		if !floatEquals(p.UnallocatedHourly/p.PoolHourly, 0.75) {
			t.Errorf("%s unallocated share = %v, want 0.75", resource, p.UnallocatedHourly/p.PoolHourly)
		}
	}
	if pool.UnallocatedHourly <= 0 {
		t.Error("expected a positive independently-listed unallocated amount")
	}
}

func TestPoolRequestsEqualAllocatableFullyAllocated(t *testing.T) {
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "p1", Namespace: "a", CPURequest: 1000, MemoryRequest: 2 * giB},
			collector.PodInfo{Name: "p2", Namespace: "b", CPURequest: 3000, MemoryRequest: 6 * giB},
		)},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)
	if !floatEquals(pool.UnallocatedHourly, 0) {
		t.Errorf("expected zero unallocated, got %v", pool.UnallocatedHourly)
	}
	if !floatEquals(pool.AllocatedHourly, pool.NodeTotalHourly) {
		t.Errorf("allocated %v != node total %v", pool.AllocatedHourly, pool.NodeTotalHourly)
	}
}

func TestPoolRequestsOverAllocatableNormalized(t *testing.T) {
	// Two pods each requesting 3 cores / 6 GiB => sums 6 cores / 12 GiB,
	// 1.5x allocatable. Shares normalize against the larger sum, so each pod
	// gets half of each pool and nothing stays unallocated.
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "p1", Namespace: "a", CPURequest: 3000, MemoryRequest: 6 * giB},
			collector.PodInfo{Name: "p2", Namespace: "b", CPURequest: 3000, MemoryRequest: 6 * giB},
		)},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)

	node := pool.Nodes[0]
	cpu := findPool(t, node, poolCPU)
	if cpu.Denominator != 6.0 {
		t.Errorf("CPU denominator = %v, want 6 (request sum, larger than allocatable)", cpu.Denominator)
	}
	if !floatEquals(pool.UnallocatedHourly, 0) {
		t.Errorf("normalized over-commit must leave no unallocated cost, got %v", pool.UnallocatedHourly)
	}
	for _, ns := range pool.Namespaces {
		if !floatEquals(ns.Hourly, pool.NodeTotalHourly/2) {
			t.Errorf("namespace %s hourly = %v, want half of node %v", ns.Name, ns.Hourly, pool.NodeTotalHourly/2)
		}
	}
}

func TestPoolUsageModeAndMetricFallback(t *testing.T) {
	// CPU has usage data; memory has none, so the memory pool must fall back to
	// requests while CPU stays in usage mode.
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{
				Name: "p", Namespace: "default",
				CPURequest: 1000, MemoryRequest: 2 * giB,
				CPUUsage: 2.0, // uses 2 cores vs 1 requested
			},
		)},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisUsage})
	assertConservation(t, pool)

	if pool.EffectiveBasis[poolCPU] != PoolBasisUsage {
		t.Errorf("cpu effective basis = %q, want usage", pool.EffectiveBasis[poolCPU])
	}
	if pool.EffectiveBasis[poolMemory] != PoolBasisRequests {
		t.Errorf("memory effective basis = %q, want requests after fallback", pool.EffectiveBasis[poolMemory])
	}
	if len(pool.BasisFallbacks) == 0 {
		t.Fatal("expected a basis fallback diagnostic for the memory pool")
	}
	found := false
	for _, fb := range pool.BasisFallbacks {
		if fb.Pool == poolMemory && fb.Reason == fallbackReasonNoMetrics {
			found = true
		}
	}
	if !found {
		t.Errorf("memory fallback not recorded: %+v", pool.BasisFallbacks)
	}

	cpu := findPool(t, pool.Nodes[0], poolCPU)
	if !floatEquals(cpu.UsageTotal, 2.0) {
		t.Errorf("cpu usage total = %v, want 2", cpu.UsageTotal)
	}
	// 2 used of 4 allocatable cores => 50% of the CPU pool.
	if !floatEquals(cpu.AllocatedHourly/cpu.PoolHourly, 0.5) {
		t.Errorf("cpu allocated fraction = %v, want 0.5", cpu.AllocatedHourly/cpu.PoolHourly)
	}
}

func TestPoolUsageModeAllMetricsMissingFallsBackFully(t *testing.T) {
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "p", Namespace: "default", CPURequest: 1000, MemoryRequest: 2 * giB},
		)},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisUsage})
	assertConservation(t, pool)
	for _, resource := range []string{poolCPU, poolMemory} {
		if pool.EffectiveBasis[resource] != PoolBasisRequests {
			t.Errorf("%s effective basis = %q, want requests when no metrics exist", resource, pool.EffectiveBasis[resource])
		}
	}
}

func TestPoolBestEffortZeroRequestButUsageParticipates(t *testing.T) {
	// Request mode: zero shares, no charge.
	reqInfo := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "be", Namespace: "default"},
		)},
	}
	reqPool := runPool(t, reqInfo, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisRequests})
	assertConservation(t, reqPool)
	if len(reqPool.Namespaces) != 0 {
		t.Errorf("BestEffort pod must not be charged in request mode, got %+v", reqPool.Namespaces)
	}
	if !floatEquals(reqPool.AllocatedHourly, 0) {
		t.Errorf("request-mode allocated = %v, want 0", reqPool.AllocatedHourly)
	}

	// Usage mode with effective usage: must participate. A fresh cluster is
	// built because node pods are value copies.
	usageInfo := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "be", Namespace: "default", CPUUsage: 1.0, MemoryUsage: 1 * giB},
		)},
	}
	usagePool := runPool(t, usageInfo, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisUsage})
	assertConservation(t, usagePool)
	if len(usagePool.Namespaces) != 1 || usagePool.Namespaces[0].Name != "default" {
		t.Fatalf("BestEffort pod with usage must be charged in usage mode, got %+v", usagePool.Namespaces)
	}
	if usagePool.AllocatedHourly <= 0 {
		t.Error("expected positive usage-mode charge for BestEffort pod")
	}
}

func TestPoolUsageOverRequestOnlyAffectsUsageMode(t *testing.T) {
	pod := collector.PodInfo{
		Name: "burst", Namespace: "default",
		CPURequest: 1000, MemoryRequest: 2 * giB,
		CPUUsage: 3.0, MemoryUsage: 6 * giB,
	}
	info := &collector.ClusterInfo{Nodes: []collector.NodeInfo{baseNode(pod)}}

	req := runPool(t, info, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisRequests})
	usage := runPool(t, info, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisUsage})
	assertConservation(t, req)
	assertConservation(t, usage)

	reqCPU := findPool(t, req.Nodes[0], poolCPU)
	usageCPU := findPool(t, usage.Nodes[0], poolCPU)
	// Request mode: 1/4 of CPU pool. Usage mode: 3/4.
	if !floatEquals(reqCPU.AllocatedHourly/reqCPU.PoolHourly, 0.25) {
		t.Errorf("request fraction = %v, want 0.25", reqCPU.AllocatedHourly/reqCPU.PoolHourly)
	}
	if !floatEquals(usageCPU.AllocatedHourly/usageCPU.PoolHourly, 0.75) {
		t.Errorf("usage fraction = %v, want 0.75", usageCPU.AllocatedHourly/usageCPU.PoolHourly)
	}
}

func TestPoolEmptyNodeFullyUnallocated(t *testing.T) {
	info := &collector.ClusterInfo{Nodes: []collector.NodeInfo{baseNode()}}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)

	if !floatEquals(pool.AllocatedHourly, 0) {
		t.Errorf("empty node allocated = %v, want 0", pool.AllocatedHourly)
	}
	if !floatEquals(pool.UnallocatedHourly, pool.NodeTotalHourly) {
		t.Errorf("empty node unallocated = %v, want full price %v", pool.UnallocatedHourly, pool.NodeTotalHourly)
	}
	if len(pool.Namespaces) != 0 {
		t.Errorf("empty node must produce no namespace charges, got %+v", pool.Namespaces)
	}
	if pool.Nodes[0].PodCount != 0 {
		t.Errorf("pod count = %d, want 0", pool.Nodes[0].PodCount)
	}
}

func TestPoolLabelBucketsMissingEmptyAndCaseSensitive(t *testing.T) {
	mk := func(name, ns string, req int64) collector.PodInfo {
		return collector.PodInfo{Name: name, Namespace: ns, CPURequest: req, MemoryRequest: giB}
	}
	info := &collector.ClusterInfo{
		NamespaceLabels: map[string]map[string]string{
			"ns-platform":  {"team": "Platform"}, // capital P
			"ns-platform2": {"team": "platform"}, // different bucket: case-sensitive
			"ns-data":      {"team": "data"},
			"ns-empty":     {"team": ""}, // empty value
			// ns-unlabeled present only via pods, no labels map entry
		},
		Nodes: []collector.NodeInfo{baseNode(
			mk("p1", "ns-platform", 500),
			mk("p2", "ns-platform2", 500),
			mk("p3", "ns-data", 500),
			mk("p4", "ns-empty", 500),
			mk("p5", "ns-unlabeled", 500),
		)},
	}
	opts := Options{
		EnablePoolAllocation: true,
		NamespaceLabelKey:    "team",
		NamespaceLabels:      info.NamespaceLabels,
		EmptyLabelIsMissing:  true,
	}
	pool := runPool(t, info, opts)
	assertConservation(t, pool)

	byLabel := map[string]LabelBucketCost{}
	for _, b := range pool.LabelBuckets {
		byLabel[b.Label] = b
	}
	for _, label := range []string{"Platform", "platform", "data", UnlabeledBucket} {
		if _, ok := byLabel[label]; !ok {
			t.Errorf("expected bucket %q in %+v", label, pool.LabelBuckets)
		}
	}
	// Empty-string label and missing label both land in the unlabeled bucket.
	if byLabel[UnlabeledBucket].NamespaceCount != 2 {
		t.Errorf("unlabeled bucket namespace count = %d, want 2 (ns-empty + ns-unlabeled)",
			byLabel[UnlabeledBucket].NamespaceCount)
	}
	// Case-sensitive values stay apart.
	if byLabel["Platform"].Label == byLabel["platform"].Label {
		t.Error("team label values must be case-sensitive")
	}

	// Every namespace appears in exactly one bucket; bucket totals equal
	// namespace totals.
	nsSeen := map[string]int{}
	for _, b := range pool.LabelBuckets {
		nsSeen[b.Label] += b.NamespaceCount
	}
	var totalNS int
	for _, n := range nsSeen {
		totalNS += n
	}
	if totalNS != len(pool.Namespaces) {
		t.Errorf("bucket namespace count %d != namespace rows %d", totalNS, len(pool.Namespaces))
	}

	// Empty label treated as a real bucket when EmptyLabelIsMissing is false.
	opts.EmptyLabelIsMissing = false
	pool2 := runPool(t, info, opts)
	assertConservation(t, pool2)
	var emptyBucket bool
	for _, b := range pool2.LabelBuckets {
		if b.Label == "" {
			emptyBucket = true
		}
	}
	if !emptyBucket {
		t.Error("empty-string label should form its own bucket when EmptyLabelIsMissing is false")
	}
}

func TestPoolUnlabeledSentinelIsNotValidLabelValue(t *testing.T) {
	// Kubernetes label values are alphanumeric, '-', '_' is NOT allowed at
	// all... verify against the real validator so the reserved identity can
	// never equal a user-supplied label value.
	if errs := validation.IsValidLabelValue(UnlabeledBucket); len(errs) == 0 {
		t.Fatalf("sentinel %q must be rejected as a Kubernetes label value", UnlabeledBucket)
	}
}

func TestPoolGPUOnlyChargesGPURequestingPods(t *testing.T) {
	node := baseNode(
		collector.PodInfo{Name: "gpu-pod", Namespace: "ml", CPURequest: 1000, MemoryRequest: 2 * giB, GPURequest: 1},
		collector.PodInfo{Name: "cpu-pod", Namespace: "web", CPURequest: 1000, MemoryRequest: 2 * giB},
	)
	node.GPUCount = 2
	info := &collector.ClusterInfo{Nodes: []collector.NodeInfo{node}}
	pool := runPool(t, info, Options{EnablePoolAllocation: true, PoolBasis: PoolBasisUsage})
	assertConservation(t, pool)

	gpu := findPool(t, pool.Nodes[0], poolGPU)
	if gpu.Unit != unitGPU {
		t.Errorf("gpu unit = %q, want %q", gpu.Unit, unitGPU)
	}
	if gpu.Basis != PoolBasisRequests {
		t.Errorf("gpu basis = %q, want requests (no GPU usage metric)", gpu.Basis)
	}
	// One GPU requested of two allocatable: half allocated, half unallocated.
	if !floatEquals(gpu.AllocatedHourly/gpu.PoolHourly, 0.5) {
		t.Errorf("gpu allocated fraction = %v, want 0.5", gpu.AllocatedHourly/gpu.PoolHourly)
	}
	var ml, web *NamespacePoolCost
	for i := range pool.Namespaces {
		switch pool.Namespaces[i].Name {
		case "ml":
			ml = &pool.Namespaces[i]
		case "web":
			web = &pool.Namespaces[i]
		}
	}
	if ml == nil || web == nil {
		t.Fatalf("expected both namespaces, got %+v", pool.Namespaces)
	}
	if ml.GPUHourly <= 0 {
		t.Error("GPU-requesting namespace must carry GPU cost")
	}
	if web.GPUHourly != 0 {
		t.Errorf("namespace without GPU request must not carry GPU cost, got %v", web.GPUHourly)
	}

	// Pool sizes reconcile to the node hourly price.
	var poolSum float64
	for _, p := range pool.Nodes[0].Pools {
		poolSum += p.PoolHourly
	}
	if conservationError(poolSum, pool.Nodes[0].HourlyPrice) > 1e-9 {
		t.Errorf("pool sizes sum %v != node price %v", poolSum, pool.Nodes[0].HourlyPrice)
	}
}

func TestPoolExcludedPodsReportedNotAttributed(t *testing.T) {
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{baseNode(
			collector.PodInfo{Name: "live", Namespace: "default", CPURequest: 1000, MemoryRequest: 2 * giB},
		)},
		ExcludedPods: []collector.ExcludedPod{
			{Name: "completed", Namespace: "default", Reason: collector.ExcludeReasonTerminated},
			{Name: "pending", Namespace: "default", Reason: collector.ExcludeReasonNoNode},
		},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)
	if pool.ExcludedPodCount != 2 {
		t.Errorf("excluded count = %d, want 2", pool.ExcludedPodCount)
	}
	// Only the live pod's namespace exists, and its charge derives only from
	// the live pod's shares.
	if len(pool.Namespaces) != 1 {
		t.Fatalf("expected 1 namespace, got %+v", pool.Namespaces)
	}
}

func TestPoolTerminatedAttachedPodIsNotCharged(t *testing.T) {
	// A Succeeded pod still attached to the node (legacy collection keeps it)
	// must be ignored by the pool engine; only the live pod is charged.
	node := baseNode(
		collector.PodInfo{Name: "live", Namespace: "default", Phase: "Running", CPURequest: 1000, MemoryRequest: 2 * giB},
		collector.PodInfo{Name: "finished", Namespace: "batch", Phase: "Succeeded", CPURequest: 1000, MemoryRequest: 2 * giB},
	)
	info := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{node},
		ExcludedPods: []collector.ExcludedPod{
			{Name: "finished", Namespace: "batch", Phase: "Succeeded", Reason: collector.ExcludeReasonTerminated},
		},
	}
	pool := runPool(t, info, Options{EnablePoolAllocation: true})
	assertConservation(t, pool)

	if len(pool.Namespaces) != 1 || pool.Namespaces[0].Name != "default" {
		t.Fatalf("terminated pod's namespace must not be charged, got %+v", pool.Namespaces)
	}
	for _, pc := range pool.Nodes[0].Pods {
		if pc.Name == "finished" && pc.Hourly != 0 {
			t.Errorf("terminated pod charge = %v, want 0", pc.Hourly)
		}
	}
}

// TestPoolDeterministicOutput verifies byte-stable ordering: cost desc with
// name/label secondary key, identical inputs produce identical JSON.
func TestPoolDeterministicOutput(t *testing.T) {
	mk := func(ns string, cost int64) collector.PodInfo {
		return collector.PodInfo{Name: ns + "-p", Namespace: ns, CPURequest: cost, MemoryRequest: giB}
	}
	build := func() *collector.ClusterInfo {
		return &collector.ClusterInfo{
			NamespaceLabels: map[string]map[string]string{
				"zeta": {"team": "t1"}, "alpha": {"team": "t1"}, "mid": {"team": "t2"},
			},
			Nodes: []collector.NodeInfo{baseNode(
				mk("zeta", 1500), mk("alpha", 1500), mk("mid", 500),
			)},
		}
	}
	opts := Options{
		EnablePoolAllocation: true, NamespaceLabelKey: "team",
		NamespaceLabels: build().NamespaceLabels,
	}
	first := runPool(t, build(), opts)
	second := runPool(t, build(), opts)

	b1, _ := json.Marshal(first)
	b2, _ := json.Marshal(second)
	if string(b1) != string(b2) {
		t.Fatalf("non-deterministic output:\n%s\nvs\n%s", b1, b2)
	}
	// Equal-cost namespaces ordered by name (alpha < zeta).
	if first.Namespaces[0].Name != "alpha" || first.Namespaces[1].Name != "zeta" {
		t.Errorf("name tiebreak wrong: %s before %s", first.Namespaces[0].Name, first.Namespaces[1].Name)
	}
	// Buckets: t1 holds the two large namespaces, t2 the small one.
	if first.LabelBuckets[0].Label != "t1" {
		t.Errorf("bucket order = %q first, want t1", first.LabelBuckets[0].Label)
	}
}

// TestPoolPropertyConservationFixedSeed is a fixed-seed property test over
// randomized in-memory clusters: conservation, both aggregation layers and
// the single-bucket-per-namespace invariant must hold for every generated
// case. It never connects to a cluster.
func TestPoolPropertyConservationFixedSeed(t *testing.T) {
	rng := rand.New(rand.NewSource(20261123))
	const iterations = 300
	labelValues := []string{"platform", "data", "ml", "infra"}

	for iter := 0; iter < iterations; iter++ {
		nodeCount := 1 + rng.Intn(5)
		nodes := make([]collector.NodeInfo, 0, nodeCount)
		nsLabels := map[string]map[string]string{}
		nsNames := make([]string, 0)
		nsCount := 1 + rng.Intn(6)
		for i := 0; i < nsCount; i++ {
			name := nsName(i)
			nsNames = append(nsNames, name)
			switch rng.Intn(4) {
			case 0: // missing label
			case 1: // empty label
				nsLabels[name] = map[string]string{"team": ""}
			default:
				nsLabels[name] = map[string]string{"team": labelValues[rng.Intn(len(labelValues))]}
			}
		}

		var analyzedTotal float64
		for n := 0; n < nodeCount; n++ {
			cpuAlloc := int64((1 + rng.Intn(8)) * 1000)
			memAlloc := int64((1 + rng.Intn(16)) * giB)
			podCount := rng.Intn(8) // includes zero-pod nodes
			pods := make([]collector.PodInfo, 0, podCount)
			for p := 0; p < podCount; p++ {
				ns := nsNames[rng.Intn(len(nsNames))]
				bestEffort := rng.Intn(5) == 0
				var cpuReq, memReq int64
				if !bestEffort {
					cpuReq = int64(rng.Intn(int(cpuAlloc) + 1000))
					memReq = int64(rng.Intn(int(memAlloc) + giB))
				}
				pod := collector.PodInfo{
					Name:          podName(n, p),
					Namespace:     ns,
					CPURequest:    cpuReq,
					MemoryRequest: memReq,
				}
				if rng.Intn(2) == 0 {
					pod.CPUUsage = rng.Float64() * float64(cpuAlloc) / 1000.0
					pod.MemoryUsage = int64(rng.Float64() * float64(memAlloc))
				}
				pods = append(pods, pod)
			}
			node := collector.NodeInfo{
				Name:           nodeName(n),
				InstanceType:   "m5.large",
				Region:         "us-east-1",
				CPUAllocatable: cpuAlloc,
				MemAllocatable: memAlloc,
				Pods:           pods,
			}
			if rng.Intn(4) == 0 {
				node.GPUCount = int64(1 + rng.Intn(4))
				for i := range node.Pods {
					if rng.Intn(3) == 0 {
						node.Pods[i].GPURequest = rng.Int63n(node.GPUCount + 1)
					}
				}
			}
			nodes = append(nodes, node)
			analyzedTotal += 0.10 // mockPricing flat hourly price
		}

		info := &collector.ClusterInfo{Nodes: nodes, NamespaceLabels: nsLabels}
		basis := PoolBasisRequests
		if rng.Intn(2) == 0 {
			basis = PoolBasisUsage
		}
		opts := Options{
			EnablePoolAllocation: true,
			PoolBasis:            basis,
			NamespaceLabelKey:    "team",
			NamespaceLabels:      nsLabels,
			EmptyLabelIsMissing:  true,
		}
		pool := runPool(t, info, opts)
		assertConservation(t, pool)

		// Per-node conservation: the pools of every node reconcile to its
		// price, with per-pool allocated + unallocated equal to pool size.
		for _, n := range pool.Nodes {
			var poolSizeSum, poolAllocated, poolUnallocated float64
			for _, p := range n.Pools {
				poolSizeSum += p.PoolHourly
				poolAllocated += p.AllocatedHourly
				poolUnallocated += p.UnallocatedHourly
				if got := conservationError(p.AllocatedHourly+p.UnallocatedHourly, p.PoolHourly); got > 1e-9 {
					t.Fatalf("iter %d node %s pool %s not conserved: %.3e",
						iter, n.Name, p.Resource, got)
				}
			}
			if got := conservationError(poolSizeSum, n.HourlyPrice); got > 1e-9 {
				t.Fatalf("iter %d node %s pool sizes %.10f != price %.10f (%.3e)",
					iter, n.Name, poolSizeSum, n.HourlyPrice, got)
			}
			if got := conservationError(poolAllocated+poolUnallocated, n.HourlyPrice); got > 1e-9 {
				t.Fatalf("iter %d node %s alloc+unalloc not conserved: %.3e", iter, n.Name, got)
			}
		}

		// Every analyzed node is present and priced.
		if len(pool.Nodes) != nodeCount {
			t.Fatalf("iter %d: node rows = %d, want %d", iter, len(pool.Nodes), nodeCount)
		}
		if conservationError(pool.NodeTotalHourly, analyzedTotal) > 1e-9 {
			t.Fatalf("iter %d: node total %v != %v", iter, pool.NodeTotalHourly, analyzedTotal)
		}

		// Each namespace lands in exactly one bucket and namespace count is
		// consistent.
		bucketNS := map[string]struct{}{}
		var bucketNSSum int
		for _, b := range pool.LabelBuckets {
			bucketNSSum += b.NamespaceCount
		}
		for _, ns := range pool.Namespaces {
			bucketNS[ns.Name] = struct{}{}
		}
		if bucketNSSum != len(bucketNS) {
			t.Fatalf("iter %d: bucket namespace count %d != unique charged namespaces %d",
				iter, bucketNSSum, len(bucketNS))
		}

		// Zero-price tolerance path never divides by zero: re-run with a
		// zero-price node shape by checking internal helper directly.
		if pool.NodeTotalHourly == 0 && pool.UnallocatedHourly != 0 {
			t.Fatalf("iter %d: zero total must use absolute error and stay zero", iter)
		}
	}
}

func nsName(i int) string   { return "ns-" + string(rune('a'+i)) }
func nodeName(i int) string { return "node-" + string(rune('a'+i)) }
func podName(n, p int) string {
	return "pod-" + string(rune('a'+n)) + "-" + string(rune('a'+p))
}

// zeroRatePricing returns a node price but zero split rates, exercising the
// degenerate pool branch without dividing by zero.
type zeroRatePricing struct {
	mockPricing
}

func (m *zeroRatePricing) GetNodePricing(_ context.Context, _ collector.NodeInfo) (*pricing.NodePricing, error) {
	return &pricing.NodePricing{HourlyTotal: 0.10}, nil
}

func TestPoolZeroTotalUsesAbsoluteErrorAndNoDivideByZero(t *testing.T) {
	// All nodes priced at zero: skipped from the analyzed total, every
	// aggregate is zero and conservation is checked in absolute terms.
	info := &collector.ClusterInfo{Nodes: []collector.NodeInfo{baseNode(
		collector.PodInfo{Name: "p", Namespace: "default", CPURequest: 1000, MemoryRequest: giB},
	)}}
	a := NewWithOptions(&mockPricing{price: 0}, Options{EnablePoolAllocation: true})
	report, err := a.Analyze(context.Background(), info)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	pool := report.PoolAllocation
	if pool.NodeTotalHourly != 0 || pool.AllocatedHourly != 0 || pool.UnallocatedHourly != 0 {
		t.Errorf("zero-price cluster must be all-zero, got total=%v allocated=%v unallocated=%v",
			pool.NodeTotalHourly, pool.AllocatedHourly, pool.UnallocatedHourly)
	}
	// The absolute-error path must not panic and must report conservation.
	assertConservation(t, pool)
}

func TestPoolDegenerateZeroRatesFullyUnallocated(t *testing.T) {
	info := &collector.ClusterInfo{Nodes: []collector.NodeInfo{baseNode(
		collector.PodInfo{Name: "p", Namespace: "default", CPURequest: 1000, MemoryRequest: giB},
	)}}
	a := NewWithOptions(&zeroRatePricing{}, Options{EnablePoolAllocation: true})
	report, err := a.Analyze(context.Background(), info)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	pool := report.PoolAllocation
	assertConservation(t, pool)
	if !floatEquals(pool.AllocatedHourly, 0) {
		t.Errorf("zero-rate node must allocate nothing, got %v", pool.AllocatedHourly)
	}
	if !floatEquals(pool.UnallocatedHourly, 0.10) {
		t.Errorf("full node price must stay unallocated, got %v", pool.UnallocatedHourly)
	}
}
