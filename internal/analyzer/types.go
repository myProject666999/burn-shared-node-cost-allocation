package analyzer

import (
	"time"

	"github.com/tanrikuluozlem/burn/internal/collector"
)

// Allocation basis modes for the opt-in resource-pool allocation.
const (
	// PoolBasisRequests allocates each resource pool by pod requests.
	PoolBasisRequests = "requests"
	// PoolBasisUsage allocates by Prometheus usage, falling back per pool to
	// requests when usage data is unavailable.
	PoolBasisUsage = "usage"
)

// UnlabeledBucket is the fixed internal identity for namespaces without the
// requested label. It is not a valid Kubernetes label value path because it
// contains underscores, so it cannot collide with a real tenant label value.
const UnlabeledBucket = "__burn_unlabeled__"

// Options controls opt-in analyses. The zero value preserves the legacy
// report exactly: resource-pool allocation is disabled and no new fields are
// populated.
type Options struct {
	// EnablePoolAllocation turns on the conservation-guaranteed node
	// resource-pool (CPU / memory / GPU) cost allocation.
	EnablePoolAllocation bool

	// PoolBasis is PoolBasisRequests or PoolBasisUsage.
	PoolBasis string

	// NamespaceLabelKey buckets namespaces by this label (e.g. "team").
	// Required when EnablePoolAllocation is true.
	NamespaceLabelKey string

	// NamespaceLabels is the read-only namespace → labels input.
	NamespaceLabels map[string]map[string]string

	// EmptyLabelIsMissing treats an empty-string label value the same as a
	// missing label (unlabeled bucket). The recommended default is true;
	// callers may set false to treat "" as an explicit bucket.
	EmptyLabelIsMissing bool
}

type CostReport struct {
	GeneratedAt     time.Time
	TotalNodes      int
	TotalPods       int
	SkippedNodes    int
	HourlyCost      float64
	MonthlyCost     float64 // compute only
	TotalIdleCost   float64 // monthly idle cost
	Nodes           []NodeCost
	InefficientPods []PodEfficiency
	Namespaces      []NamespaceCost `json:"namespaces,omitempty"`
	AllPods         []PodEfficiency `json:"-"`
	WasteAnalysis   WasteAnalysis
	MetricsSource   string // "prometheus" or "requests"
	Period          string `json:"period,omitempty"`

	// Non-compute costs
	PVCosts          []PVCost    `json:"pv_costs,omitempty"`
	LBCosts          []LBCost    `json:"lb_costs,omitempty"`
	NetworkCost      NetworkCost `json:"network_cost,omitempty"`
	TotalPVCost      float64     `json:"total_pv_cost,omitempty"`
	TotalLBCost      float64     `json:"total_lb_cost,omitempty"`
	TotalNetworkCost float64     `json:"total_network_cost,omitempty"`
	TotalMonthlyCost float64     `json:"total_monthly_cost"` // compute + storage + LB + network

	// Spot readiness
	SpotReadiness []SpotReadiness `json:"spot_readiness,omitempty"`
	SpotSavings   float64         `json:"spot_savings,omitempty"`

	// PoolAllocation is populated only when resource-pool allocation was
	// explicitly enabled. Legacy fields above keep their original semantics.
	PoolAllocation *PoolAllocationReport `json:"pool_allocation,omitempty"`

	// ExcludedPods are pods deliberately excluded from allocation with a
	// reason; they are never attributed to a real tenant.
	ExcludedPods []collector.ExcludedPod `json:"excluded_pods,omitempty"`
}

// PoolAllocationReport is the conservation-guaranteed allocation of node
// prices across CPU, memory and GPU resource pools.
type PoolAllocationReport struct {
	// RequestedBasis is the mode requested by the caller ("requests" or
	// "usage").
	RequestedBasis string `json:"requested_basis"`
	// EffectiveBasis records the mode actually applied per resource pool;
	// "usage" pools fall back to "requests" when no metrics exist.
	EffectiveBasis map[string]string `json:"effective_basis"`
	// BasisFallbacks lists per-pool fallback events for diagnostics.
	BasisFallbacks []BasisFallback `json:"basis_fallbacks,omitempty"`

	// AllocatedHourly is the total charged to pods; UnallocatedHourly is the
	// residual listed independently in this report.
	AllocatedHourly    float64 `json:"allocated_hourly"`
	UnallocatedHourly  float64 `json:"unallocated_hourly"`
	AllocatedMonthly   float64 `json:"allocated_monthly"`
	UnallocatedMonthly float64 `json:"unallocated_monthly"`

	// NodeTotalHourly is the sum of every analyzed node's hourly price; the
	// conservation identity is allocated + unallocated == node total.
	NodeTotalHourly  float64              `json:"node_total_hourly"`
	Nodes            []NodePoolAllocation `json:"nodes"`
	Namespaces       []NamespacePoolCost  `json:"namespaces"`
	LabelBuckets     []LabelBucketCost    `json:"label_buckets,omitempty"`
	LabelKey         string               `json:"label_key,omitempty"`
	UnlabeledBucket  string               `json:"unlabeled_bucket"`
	ExcludedPodCount int                  `json:"excluded_pod_count"`
}

// BasisFallback records that a resource pool could not be allocated in usage
// mode and was allocated in requests mode instead.
type BasisFallback struct {
	NodeName string `json:"node_name"`
	Pool     string `json:"pool"`
	Reason   string `json:"reason"`
}

// NodePoolAllocation is one node's per-pool split. Pool hourly sizes sum to
// the node hourly price (within floating-point tolerance).
type NodePoolAllocation struct {
	Name        string             `json:"name"`
	HourlyPrice float64            `json:"hourly_price"`
	Pools       []ResourcePoolCost `json:"pools"`
	// Pods are the per-pod charges derived from the same shares; they sum to
	// the node's allocated cost.
	Pods     []PodPoolCost `json:"pods"`
	PodCount int           `json:"pod_count"`
}

// ResourcePoolCost is one CPU/memory/GPU pool on one node.
type ResourcePoolCost struct {
	// Resource is "cpu", "memory" or "gpu".
	Resource string `json:"resource"`
	// Unit is the denominator unit: "core", "GiB" or "gpu".
	Unit string `json:"unit"`
	// Allocatable is the pool denominator in Unit.
	Allocatable float64 `json:"allocatable"`
	// RequestTotal / UsageTotal are the sums across the node's included pods,
	// in Unit.
	RequestTotal float64 `json:"request_total"`
	UsageTotal   float64 `json:"usage_total,omitempty"`
	// Denominator is max(pod share sum, allocatable); zero shares leave the
	// whole pool unallocated.
	Denominator float64 `json:"denominator"`
	// Basis actually applied to this pool: "requests" or "usage".
	Basis string `json:"basis"`

	PoolHourly        float64 `json:"pool_hourly"`
	AllocatedHourly   float64 `json:"allocated_hourly"`
	UnallocatedHourly float64 `json:"unallocated_hourly"`
}

// PodPoolCost is one pod's charge from the pool allocation.
type PodPoolCost struct {
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	NodeName  string  `json:"node_name"`
	CPU       float64 `json:"cpu_hourly"`
	Memory    float64 `json:"memory_hourly"`
	GPU       float64 `json:"gpu_hourly,omitempty"`
	Hourly    float64 `json:"hourly"`
}

// NamespacePoolCost aggregates pool charges by namespace.
type NamespacePoolCost struct {
	Name         string  `json:"name"`
	PodCount     int     `json:"pod_count"`
	CPUHourly    float64 `json:"cpu_hourly"`
	MemoryHourly float64 `json:"memory_hourly"`
	GPUHourly    float64 `json:"gpu_hourly"`
	Hourly       float64 `json:"hourly"`
	Monthly      float64 `json:"monthly"`
}

// LabelBucketCost aggregates pool charges by the value of the configured
// namespace label. Every namespace belongs to exactly one bucket.
type LabelBucketCost struct {
	// Label is the real label value, or UnlabeledBucket for missing labels.
	Label          string  `json:"label"`
	NamespaceCount int     `json:"namespace_count"`
	CPUHourly      float64 `json:"cpu_hourly"`
	MemoryHourly   float64 `json:"memory_hourly"`
	GPUHourly      float64 `json:"gpu_hourly"`
	Hourly         float64 `json:"hourly"`
	Monthly        float64 `json:"monthly"`
}

type NodeCost struct {
	Name         string
	InstanceType string
	Region       string
	IsSpot       bool
	HourlyPrice  float64
	MonthlyPrice float64
	PodCount     int

	// Per-resource cost rates
	CPUCostPerCore float64 `json:"cpu_cost_per_core,omitempty"`
	RAMCostPerGiB  float64 `json:"ram_cost_per_gib,omitempty"`
	GPUCostPerUnit float64 `json:"gpu_cost_per_unit,omitempty"`
	GPUCount       int64   `json:"gpu_count,omitempty"`

	// Resource allocation (what pods requested as % of node capacity)
	CPURequested float64
	MemRequested float64

	// Idle capacity cost (per-resource: CPU idle + RAM idle)
	IdleCostHourly  float64
	IdleCostMonthly float64
	IdlePercent     float64 // percentage of node capacity that is idle
	CPUIdleCost     float64 `json:"cpu_idle_cost,omitempty"` // monthly
	RAMIdleCost     float64 `json:"ram_idle_cost,omitempty"` // monthly
}

type PodEfficiency struct {
	Name            string
	Namespace       string
	OwnerKind       string  `json:"-"`
	OwnerName       string  `json:"-"`
	CPURequest      int64   // millicores
	CPUUsage        float64 // cores (from Prometheus)
	CPUEfficiency   float64 // usage/request ratio (0-1+)
	MemRequest      int64   // bytes
	MemUsage        int64   // bytes (from Prometheus)
	MemEfficiency   float64 // usage/request ratio (0-1+)
	MonthlyCost     float64 // estimated cost based on resource allocation
	CPUCost         float64 `json:"cpu_cost"`
	RAMCost         float64 `json:"ram_cost"`
	GPUCost         float64 `json:"gpu_cost,omitempty"`
	GPURequest      int64   `json:"gpu_request,omitempty"`
	CPUP95Usage     float64 `json:"cpu_p95_usage,omitempty"` // p95 CPU usage in cores
	MemoryP95Usage  int64   `json:"mem_p95_usage,omitempty"` // p95 memory usage in bytes
	CPUP95Available bool    `json:"-"`
	MemP95Available bool    `json:"-"`
}

type NamespaceCost struct {
	Name        string  `json:"name"`
	PodCount    int     `json:"pod_count"`
	CPURequest  int64   `json:"cpu_request"` // total millicores
	CPUUsage    float64 `json:"cpu_usage"`   // total cores
	MemRequest  int64   `json:"mem_request"` // total bytes
	MemUsage    int64   `json:"mem_usage"`   // total bytes
	MonthlyCost float64 `json:"monthly_cost"`
	CPUCost     float64 `json:"cpu_cost"`
	RAMCost     float64 `json:"ram_cost"`
	StorageCost float64 `json:"storage_cost,omitempty"`
}

type PVCost struct {
	Name         string  `json:"name"`
	Namespace    string  `json:"namespace"`
	StorageClass string  `json:"storage_class"`
	CapacityGiB  float64 `json:"capacity_gib"`
	MonthlyCost  float64 `json:"monthly_cost"`
}

type LBCost struct {
	Name        string  `json:"name"`
	Namespace   string  `json:"namespace"`
	MonthlyCost float64 `json:"monthly_cost"`
}

type NetworkCost struct {
	EgressGiBPerMonth float64 `json:"egress_gib_per_month"`
	MonthlyCost       float64 `json:"monthly_cost"`
}

type WasteAnalysis struct {
	UnderutilizedNodes []UnderutilizedNode
	PotentialSavings   float64
}

type UnderutilizedNode struct {
	Name           string
	IdlePercent    float64
	IdleCost       float64
	Recommendation string
}
