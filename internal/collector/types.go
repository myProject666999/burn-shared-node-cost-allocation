package collector

import "time"

type CloudProvider string

const (
	CloudAWS     CloudProvider = "aws"
	CloudGCP     CloudProvider = "gcp"
	CloudAzure   CloudProvider = "azure"
	CloudUnknown CloudProvider = "unknown"
)

type NodeInfo struct {
	Name           string
	InternalIP     string
	InstanceType   string
	Region         string
	Zone           string
	CloudProvider  CloudProvider
	CPUCores       int64
	MemoryBytes    int64
	CPUAllocatable int64
	MemAllocatable int64
	CPUUsage       float64
	MemoryUsage    int64
	GPUCount       int64
	GPUType        string // e.g. "Tesla-T4", "A100"
	IsSpot         bool
	CreatedAt      time.Time
	ProviderID     string
	Labels         map[string]string
	Pods           []PodInfo
}

type PodInfo struct {
	Name            string
	Namespace       string
	Phase           string // corev1.PodPhase: "Running", "Pending", "Succeeded", "Failed"
	OwnerKind       string // resolved controller: "Deployment", "StatefulSet", "DaemonSet"
	OwnerName       string
	CPURequest      int64
	CPULimit        int64
	MemoryRequest   int64
	MemoryLimit     int64
	GPURequest      int64 // nvidia.com/gpu
	CPUUsage        float64
	MemoryUsage     int64
	CPUP95Usage     float64 // p95 CPU usage in cores (over analysis period)
	MemoryP95Usage  int64   // p95 memory usage in bytes (over analysis period)
	CPUP95Available bool    // true if P95 query returned data for this pod
	MemP95Available bool    // true if P95 query returned data for this pod
}

type PVCInfo struct {
	Name           string
	Namespace      string
	StorageClass   string
	RequestedBytes int64
	VolumeName     string
	CloudDiskID    string // vol-xxx (AWS) or Azure disk resource path
}

type LBServiceInfo struct {
	Name      string
	Namespace string
	Hostname  string // ELB hostname or Azure LB IP
}

type WorkloadInfo struct {
	Name                  string
	Namespace             string
	Kind                  string
	Replicas              int32
	HasLocalStorage       bool
	HasGPU                bool
	MaxUnavailable        int32
	PDBMinAvailable       int32
	PDBMaxUnavailable     int32
	PDBFound              bool
	PDBUsesMaxUnavailable bool
	PriorityClass         string
	MonthlyCost           float64
}

// ExcludedPod records a pod that was deliberately excluded from cost allocation
// so it can never be silently attributed to a real tenant.
type ExcludedPod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	NodeName  string `json:"node_name,omitempty"`
	Phase     string `json:"phase,omitempty"`
	Reason    string `json:"reason"`
}

// Reasons for excluding a pod from cost allocation.
const (
	ExcludeReasonNoNode      = "no_node_assigned"
	ExcludeReasonNoNamespace = "no_namespace"
	ExcludeReasonTerminated  = "terminated_pod"
)

type ClusterInfo struct {
	Nodes         []NodeInfo
	TotalNodes    int
	TotalPods     int
	PVCs          []PVCInfo
	LoadBalancers []LBServiceInfo
	Workloads     []WorkloadInfo

	// ExcludedPods lists pods removed from cost allocation with the reason.
	ExcludedPods []ExcludedPod `json:"excluded_pods,omitempty"`

	// NamespaceLabels maps namespace name to its labels. Read-only input for
	// label-bucket (e.g. team) allocation. May be nil when labels are unavailable.
	NamespaceLabels map[string]map[string]string `json:"-"`

	// NamespaceLabelError holds a non-fatal error encountered while reading
	// namespace labels (e.g. missing list permission in single-namespace mode).
	NamespaceLabelError string `json:"namespace_label_error,omitempty"`
}
