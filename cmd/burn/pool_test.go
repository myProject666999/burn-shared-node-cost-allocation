package main

import (
	"strings"
	"testing"

	"github.com/tanrikuluozlem/burn/internal/analyzer"
	"github.com/tanrikuluozlem/burn/internal/collector"
)

func TestPoolOptionsDisabledByDefault(t *testing.T) {
	poolAllocate = false
	opts := poolOptions(&collector.ClusterInfo{})
	if opts.EnablePoolAllocation {
		t.Error("pool allocation must be opt-in; zero-value options expected")
	}
}

func TestPoolOptionsMapping(t *testing.T) {
	poolAllocate = true
	poolBasis = analyzer.PoolBasisUsage
	poolNamespaceLabel = "team"
	poolEmptyAsMissing = true
	labels := map[string]map[string]string{"ns": {"team": "platform"}}
	opts := poolOptions(&collector.ClusterInfo{NamespaceLabels: labels})

	if !opts.EnablePoolAllocation || opts.PoolBasis != analyzer.PoolBasisUsage {
		t.Errorf("options mapping wrong: %+v", opts)
	}
	if opts.NamespaceLabelKey != "team" || opts.NamespaceLabels["ns"]["team"] != "platform" {
		t.Errorf("label options wrong: %+v", opts)
	}
	if !opts.EmptyLabelIsMissing {
		t.Error("EmptyLabelIsMissing must default to true")
	}
	poolAllocate = false
}

func TestOutputPoolAllocation(t *testing.T) {
	pool := &analyzer.PoolAllocationReport{
		RequestedBasis: analyzer.PoolBasisUsage,
		EffectiveBasis: map[string]string{
			"cpu": analyzer.PoolBasisUsage, "memory": analyzer.PoolBasisRequests,
			"gpu": analyzer.PoolBasisRequests,
		},
		LabelKey:           "team",
		UnlabeledBucket:    analyzer.UnlabeledBucket,
		NodeTotalHourly:    0.10,
		AllocatedMonthly:   50,
		UnallocatedMonthly: 23,
		Namespaces: []analyzer.NamespacePoolCost{
			{Name: "platform", Hourly: 0.05, Monthly: 36.5},
		},
		LabelBuckets: []analyzer.LabelBucketCost{
			{Label: "platform", NamespaceCount: 1, Monthly: 36.5},
			{Label: analyzer.UnlabeledBucket, NamespaceCount: 1, Monthly: 13.5},
		},
	}

	out := captureOutput(func() { outputPoolAllocation(pool) })
	for _, want := range []string{
		"RESOURCE POOL ALLOCATION",
		"Requested basis: usage",
		"cpu=usage",
		"memory=requests",
		"NAMESPACE",
		"LABEL BUCKETS (team)",
		"(unlabeled)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}
