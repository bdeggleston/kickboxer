package cluster

import (
	"testing"
)

func TestExecutionOrder(t *testing.T) {

}

// tests that committing triggers a dataset mutation,
// if no dependencies are waiting on a decision
func TestNonPendingDependencyExecutionOccursOnCommit(t *testing.T) {

}

// tests that a commit doesn't mutate the dataset until pending
// dependencies have received a decision
func TestPendingDependencyResolutionOccuresBeforeExecution(t *testing.T) {

}

// tests that nodes prune their dependency graphs
// need to determine the protocol for this
func TestDependencyGraphPruning(t *testing.T) {

}
