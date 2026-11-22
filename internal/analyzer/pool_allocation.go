package analyzer

import (
	"context"
	"log/slog"
	"math"
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/tanrikuluozlem/burn/internal/collector"
	"github.com/tanrikuluozlem/burn/internal/pricing"
)

const (
	poolCPU    = "cpu"
	poolMemory = "memory"
	poolGPU    = "gpu"

	unitCore = "core"
	unitGiB  = "GiB"
	unitGPU  = "gpu"

	bytesPerGiB  = 1024 * 1024 * 1024
	milliPerCore = 1000.0

	// fallbackReasonNoMetrics is recorded when a usage-mode pool has no
	// positive usage data and therefore falls back to requests.
	fallbackReasonNoMetrics = "no_positive_usage_metrics"
)

// isExcludedFromPool reports whether a pod attached to a node must not
// receive pool-allocation charges. Unscheduled pods never reach node.Pods;
// this handles the remaining cases (terminated pods still present, and the
// defensive empty-namespace case).
func isExcludedFromPool(p collector.PodInfo) bool {
	if p.Namespace == "" {
		return true
	}
	return p.Phase == string(corev1.PodSucceeded) || p.Phase == string(corev1.PodFailed)
}

// podShare holds one included pod's normalized shares in a node's pools.
type podShare struct {
	pod                    collector.PodInfo
	cpuReq, memReq, gpuReq float64 // in core / GiB / gpu units
	cpuUsage, memUsage     float64
}

// nsAccum accumulates pool charges for one namespace.
type nsAccum struct {
	podCount                        int
	cpuHourly, memHourly, gpuHourly float64
}

// bucketAccum accumulates pool charges for one label-bucket value.
type bucketAccum struct {
	namespaces                      map[string]struct{}
	cpuHourly, memHourly, gpuHourly float64
}

// calculatePoolAllocation runs the conservation-guaranteed pool allocation.
// Node hourly prices are first split into CPU, memory and (when present) GPU
// pools using the existing SplitNodeCost / SplitNodeCostWithGPU rates; each
// pool is then allocated to pods by request share or usage share.
func (a *Analyzer) calculatePoolAllocation(ctx context.Context, info *collector.ClusterInfo) *PoolAllocationReport {
	basis := a.options.PoolBasis
	if basis != PoolBasisUsage {
		basis = PoolBasisRequests
	}

	report := &PoolAllocationReport{
		RequestedBasis:   basis,
		EffectiveBasis:   make(map[string]string),
		LabelKey:         a.options.NamespaceLabelKey,
		UnlabeledBucket:  UnlabeledBucket,
		ExcludedPodCount: len(info.ExcludedPods),
	}

	// Aggregations keyed by namespace and label bucket.
	nsCosts := make(map[string]*nsAccum)
	buckets := make(map[string]*bucketAccum)
	bucketForNS := a.buildNamespaceBuckets(info)

	var allocatedTotal, nodeTotal float64

	for _, node := range info.Nodes {
		// Only chargeable pods participate. Terminated pods may still be
		// attached to the node (the collector keeps them for the legacy
		// report) but must not receive pool charges; exclusions are reported
		// on info.ExcludedPods.
		shares := make([]podShare, 0, len(node.Pods))
		for _, p := range node.Pods {
			if isExcludedFromPool(p) {
				continue
			}
			shares = append(shares, podShare{
				pod:      p,
				cpuReq:   float64(p.CPURequest) / milliPerCore,
				memReq:   float64(p.MemoryRequest) / bytesPerGiB,
				gpuReq:   float64(p.GPURequest),
				cpuUsage: p.CPUUsage,
				memUsage: float64(p.MemoryUsage) / bytesPerGiB,
			})
		}

		np, skip := a.nodePricingForPool(ctx, node, report)
		if skip {
			continue
		}

		nodeAlloc := NodePoolAllocation{
			Name:        node.Name,
			HourlyPrice: np.HourlyTotal,
			PodCount:    len(shares),
		}
		nodeTotal += np.HourlyTotal

		pools := a.buildPools(node, np, shares, basis, report)
		nodeAlloc.Pools = pools

		var nodeAllocated float64
		// Per-pod charges accumulated across this node's pools.
		podCharges := make([]PodPoolCost, len(shares))
		for i, s := range shares {
			podCharges[i] = PodPoolCost{
				Name: s.pod.Name, Namespace: s.pod.Namespace, NodeName: node.Name,
			}
		}

		for _, pool := range pools {
			nodeAllocated += pool.AllocatedHourly
			chargePods(pool, shares, podCharges)
		}

		// Reconcile node-level roundoff: pool sizes are scaled versions of the
		// split rates, so a tiny negative residual (roundoff only) is treated
		// as fully allocated. The global residual below is derived exactly.
		allocatedTotal += nodeAllocated
		nodeAlloc.Pods = podCharges
		report.Nodes = append(report.Nodes, nodeAlloc)

		for i := range podCharges {
			charge := &podCharges[i]
			if charge.Hourly == 0 {
				// A pod can have a zero charge (e.g. BestEffort in request
				// mode); it must not appear as a charged tenant.
				continue
			}
			ns := charge.Namespace
			acc, ok := nsCosts[ns]
			if !ok {
				acc = &nsAccum{}
				nsCosts[ns] = acc
			}
			acc.podCount++
			acc.cpuHourly += charge.CPU
			acc.memHourly += charge.Memory
			acc.gpuHourly += charge.GPU

			bucket := bucketForNS[ns]
			bacc, ok := buckets[bucket]
			if !ok {
				bacc = &bucketAccum{namespaces: make(map[string]struct{})}
				buckets[bucket] = bacc
			}
			bacc.namespaces[ns] = struct{}{}
			bacc.cpuHourly += charge.CPU
			bacc.memHourly += charge.Memory
			bacc.gpuHourly += charge.GPU
		}
	}

	report.NodeTotalHourly = nodeTotal
	report.AllocatedHourly = allocatedTotal
	// Global conservation: the independently-listed unallocated line is the
	// exact residual of the analyzed node total. Per-pool unallocated values
	// on ResourcePoolCost remain independently derived.
	unallocatedTotal := nodeTotal - allocatedTotal
	report.UnallocatedHourly = unallocatedTotal
	report.AllocatedMonthly = allocatedTotal * hoursPerMonth
	report.UnallocatedMonthly = unallocatedTotal * hoursPerMonth

	report.Namespaces = buildNamespacePoolCosts(nsCosts)
	if a.options.NamespaceLabelKey != "" {
		report.LabelBuckets = buildLabelBucketCosts(buckets)
	}
	return report
}

// nodePricingForPool fetches pricing for a node, recording skip diagnostics.
// It returns skip=true when the node cannot participate in the allocation.
func (a *Analyzer) nodePricingForPool(ctx context.Context, node collector.NodeInfo, report *PoolAllocationReport) (*pricing.NodePricing, bool) {
	if node.CPUAllocatable <= 0 || node.MemAllocatable <= 0 {
		slog.Warn("pool allocation: node has non-positive allocatable resources, skipping",
			"node", node.Name)
		return nil, true
	}
	np, err := a.pricing.GetNodePricing(ctx, node)
	if err != nil {
		slog.Warn("pool allocation: pricing lookup failed, skipping node",
			"node", node.Name, "error", err)
		return nil, true
	}
	if np == nil || np.HourlyTotal <= 0 {
		slog.Warn("pool allocation: no price for node, skipping", "node", node.Name)
		return nil, true
	}
	return np, false
}

// buildPools derives the CPU, memory and GPU pools for one node and allocates
// each of them. Pool hourly sizes are scaled so they sum exactly to the node
// hourly price, preserving node-level conservation by construction.
func (a *Analyzer) buildPools(node collector.NodeInfo, np *pricing.NodePricing, shares []podShare, requestedBasis string, report *PoolAllocationReport) []ResourcePoolCost {
	cpuCores := float64(node.CPUAllocatable) / milliPerCore
	memGiB := float64(node.MemAllocatable) / bytesPerGiB
	gpuUnits := float64(node.GPUCount)

	rawCPU := np.CPUCostPerCore * cpuCores
	rawMem := np.RAMCostPerGiB * memGiB
	rawGPU := np.GPUCostPerUnit * gpuUnits
	rawTotal := rawCPU + rawMem + rawGPU

	pools := make([]ResourcePoolCost, 0, 3)
	if rawTotal <= 0 {
		// Degenerate rates: keep the whole node price unallocated rather than
		// dividing by zero.
		pools = append(pools, ResourcePoolCost{
			Resource: poolCPU, Unit: unitCore, Allocatable: cpuCores,
			Basis: PoolBasisRequests, PoolHourly: np.HourlyTotal,
			UnallocatedHourly: np.HourlyTotal,
		})
		markEffective(report, poolCPU, PoolBasisRequests)
		return pools
	}
	scale := np.HourlyTotal / rawTotal
	cpuPoolHourly := rawCPU * scale
	memPoolHourly := rawMem * scale
	gpuPoolHourly := rawGPU * scale

	// CPU pool
	var cpuReqTotal, cpuUsageTotal float64
	for _, s := range shares {
		cpuReqTotal += s.cpuReq
		cpuUsageTotal += s.cpuUsage
	}
	cpuBasis := requestedBasis
	if requestedBasis == PoolBasisUsage && cpuUsageTotal <= 0 {
		cpuBasis = PoolBasisRequests
		report.BasisFallbacks = append(report.BasisFallbacks, BasisFallback{
			NodeName: node.Name, Pool: poolCPU, Reason: fallbackReasonNoMetrics,
		})
	}
	markEffective(report, poolCPU, cpuBasis)
	pools = append(pools, allocatePool(ResourcePoolCost{
		Resource: poolCPU, Unit: unitCore, Allocatable: cpuCores,
		RequestTotal: cpuReqTotal, UsageTotal: cpuUsageTotal,
		Basis: cpuBasis, PoolHourly: cpuPoolHourly,
	}, shares, cpuBasis))

	// Memory pool
	var memReqTotal, memUsageTotal float64
	for _, s := range shares {
		memReqTotal += s.memReq
		memUsageTotal += s.memUsage
	}
	memBasis := requestedBasis
	if requestedBasis == PoolBasisUsage && memUsageTotal <= 0 {
		memBasis = PoolBasisRequests
		report.BasisFallbacks = append(report.BasisFallbacks, BasisFallback{
			NodeName: node.Name, Pool: poolMemory, Reason: fallbackReasonNoMetrics,
		})
	}
	markEffective(report, poolMemory, memBasis)
	pools = append(pools, allocatePool(ResourcePoolCost{
		Resource: poolMemory, Unit: unitGiB, Allocatable: memGiB,
		RequestTotal: memReqTotal, UsageTotal: memUsageTotal,
		Basis: memBasis, PoolHourly: memPoolHourly,
	}, shares, memBasis))

	// GPU pool exists only when the node exposes GPUs. Pods without GPU
	// requests never carry a share of this pool.
	if node.GPUCount > 0 {
		var gpuReqTotal float64
		for _, s := range shares {
			gpuReqTotal += s.gpuReq
		}
		// There is no GPU usage metric in the collected data; GPU is always
		// request-based even in usage mode.
		if requestedBasis == PoolBasisUsage {
			report.BasisFallbacks = append(report.BasisFallbacks, BasisFallback{
				NodeName: node.Name, Pool: poolGPU, Reason: "gpu_usage_metrics_unavailable",
			})
		}
		markEffective(report, poolGPU, PoolBasisRequests)
		pools = append(pools, allocatePool(ResourcePoolCost{
			Resource: poolGPU, Unit: unitGPU, Allocatable: gpuUnits,
			RequestTotal: gpuReqTotal,
			Basis:        PoolBasisRequests, PoolHourly: gpuPoolHourly,
		}, shares, PoolBasisRequests))
	}

	return pools
}

// markEffective records the basis that actually applied to a pool. A pool
// only ever stays in usage mode when every node has usable metrics, so
// request wins globally as soon as any node falls back.
func markEffective(report *PoolAllocationReport, pool, basis string) {
	if existing, ok := report.EffectiveBasis[pool]; !ok || (existing == PoolBasisUsage && basis == PoolBasisRequests) {
		report.EffectiveBasis[pool] = basis
	}
}

// allocatePool distributes one resource pool's hourly cost. The denominator
// is max(pod share sum, allocatable): below allocatable, the residual stays
// unallocated; above allocatable, shares are normalized by the larger sum.
func allocatePool(pool ResourcePoolCost, shares []podShare, basis string) ResourcePoolCost {
	shareOf := func(s podShare) float64 {
		if basis == PoolBasisUsage {
			switch pool.Resource {
			case poolCPU:
				return s.cpuUsage
			case poolMemory:
				return s.memUsage
			}
		}
		switch pool.Resource {
		case poolCPU:
			return s.cpuReq
		case poolMemory:
			return s.memReq
		case poolGPU:
			return s.gpuReq
		}
		return 0
	}

	var shareSum float64
	for _, s := range shares {
		shareSum += shareOf(s)
	}

	denominator := math.Max(shareSum, pool.Allocatable)
	pool.Denominator = denominator
	if denominator <= 0 {
		// No shares and no capacity priced here: leave fully unallocated.
		pool.UnallocatedHourly = pool.PoolHourly
		return pool
	}

	var allocated float64
	for _, s := range shares {
		share := shareOf(s)
		if share <= 0 {
			continue
		}
		allocated += pool.PoolHourly * share / denominator
	}
	pool.AllocatedHourly = allocated
	pool.UnallocatedHourly = pool.PoolHourly - allocated
	return pool
}

// chargePods adds each pool's per-pod charges to the shared pod charge slice.
// Charges use the same share/denominator pair as allocatePool, and the last
// charged pod absorbs the floating-point residual so pod charges sum exactly
// to the pool's allocated cost.
func chargePods(pool ResourcePoolCost, shares []podShare, charges []PodPoolCost) {
	if pool.Denominator <= 0 {
		return
	}
	shareFor := func(s podShare) float64 {
		switch pool.Resource {
		case poolCPU:
			if pool.Basis == PoolBasisUsage {
				return s.cpuUsage
			}
			return s.cpuReq
		case poolMemory:
			if pool.Basis == PoolBasisUsage {
				return s.memUsage
			}
			return s.memReq
		case poolGPU:
			return s.gpuReq
		}
		return 0
	}

	lastIdx := -1
	var sum float64
	for i, s := range shares {
		share := shareFor(s)
		if share <= 0 {
			continue
		}
		cost := pool.PoolHourly * share / pool.Denominator
		switch pool.Resource {
		case poolCPU:
			charges[i].CPU += cost
		case poolMemory:
			charges[i].Memory += cost
		case poolGPU:
			charges[i].GPU += cost
		}
		charges[i].Hourly += cost
		sum += cost
		lastIdx = i
	}
	if lastIdx >= 0 && sum != pool.AllocatedHourly {
		delta := pool.AllocatedHourly - sum
		switch pool.Resource {
		case poolCPU:
			charges[lastIdx].CPU += delta
		case poolMemory:
			charges[lastIdx].Memory += delta
		case poolGPU:
			charges[lastIdx].GPU += delta
		}
		charges[lastIdx].Hourly += delta
	}
}

// buildNamespaceBuckets maps every namespace name to exactly one label
// bucket. Missing/empty labels use the reserved unlabeled identity that
// cannot collide with a real label value.
func (a *Analyzer) buildNamespaceBuckets(info *collector.ClusterInfo) map[string]string {
	out := make(map[string]string)
	seen := make(map[string]struct{})
	for _, node := range info.Nodes {
		for _, pod := range node.Pods {
			if pod.Namespace == "" {
				continue
			}
			if _, ok := seen[pod.Namespace]; ok {
				continue
			}
			seen[pod.Namespace] = struct{}{}
			out[pod.Namespace] = a.bucketForNamespace(pod.Namespace, info.NamespaceLabels)
		}
	}
	// Namespaces that only appear via storage/LBs still get a deterministic
	// bucket even though they carry no compute charge.
	for ns := range info.NamespaceLabels {
		if _, ok := out[ns]; !ok {
			out[ns] = a.bucketForNamespace(ns, info.NamespaceLabels)
		}
	}
	return out
}

func (a *Analyzer) bucketForNamespace(namespace string, labels map[string]map[string]string) string {
	key := a.options.NamespaceLabelKey
	if key == "" {
		return UnlabeledBucket
	}
	nsLabels, ok := labels[namespace]
	if !ok {
		return UnlabeledBucket
	}
	value, present := nsLabels[key]
	if !present {
		return UnlabeledBucket
	}
	if value == "" && a.options.EmptyLabelIsMissing {
		return UnlabeledBucket
	}
	return value
}

func buildNamespacePoolCosts(acc map[string]*nsAccum) []NamespacePoolCost {
	out := make([]NamespacePoolCost, 0, len(acc))
	for name, a := range acc {
		hourly := a.cpuHourly + a.memHourly + a.gpuHourly
		out = append(out, NamespacePoolCost{
			Name:         name,
			PodCount:     a.podCount,
			CPUHourly:    a.cpuHourly,
			MemoryHourly: a.memHourly,
			GPUHourly:    a.gpuHourly,
			Hourly:       hourly,
			Monthly:      hourly * hoursPerMonth,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hourly != out[j].Hourly {
			return out[i].Hourly > out[j].Hourly
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func buildLabelBucketCosts(acc map[string]*bucketAccum) []LabelBucketCost {
	out := make([]LabelBucketCost, 0, len(acc))
	for label, a := range acc {
		hourly := a.cpuHourly + a.memHourly + a.gpuHourly
		out = append(out, LabelBucketCost{
			Label:          label,
			NamespaceCount: len(a.namespaces),
			CPUHourly:      a.cpuHourly,
			MemoryHourly:   a.memHourly,
			GPUHourly:      a.gpuHourly,
			Hourly:         hourly,
			Monthly:        hourly * hoursPerMonth,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hourly != out[j].Hourly {
			return out[i].Hourly > out[j].Hourly
		}
		return out[i].Label < out[j].Label
	})
	return out
}
