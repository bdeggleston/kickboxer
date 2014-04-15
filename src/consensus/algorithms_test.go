package consensus

import (
	"fmt"
)


import (
	"launchpad.net/gocheck"
)

type tarjanChecker struct {
	*gocheck.CheckerInfo
}

func (sc *tarjanChecker) Check(params []interface {}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "2 arguments required"
	}
	actual, ok := params[0].([][]InstanceID)
	if !ok {
		return false, "first arg is not [][]InstanceID"
	}
	expected, ok := params[1].([][]InstanceID)
	if !ok {
		return false, "second arg is not [][]InstanceID"
	}

	if len(actual) != len(expected) {
		return false, ""
	}

	size := len(actual)
	actualSet := make([]InstanceIDSet, size)
	expectedSet := make([]InstanceIDSet, size)

	for i:=0; i<size; i++ {
		actualSet[i] = NewInstanceIDSet(actual[i])
		expectedSet[i] = NewInstanceIDSet(expected[i])
	}

	// the tarjan algorithm should also return the components
	// as a reverse topological sort of the connected components,
	// so we will check them in order
	for i:=0; i<size; i++ {
		if !actualSet[i].Equal(expectedSet[i]) {
			return false, fmt.Sprintf("%v not found\n%v\n!=\n%v", actualSet[i],  actual, expected)
		}
	}

	return true, ""
}

var TarjanCheck gocheck.Checker = &tarjanChecker{
	&gocheck.CheckerInfo{
		Name: "TarjanCheck",
		Params: []string{"obtained", "expected"},
	},
}

type TarjanTest struct {
	graph map[InstanceID][]InstanceID
}

var _ = gocheck.Suite(&TarjanTest{})

func (t *TarjanTest) SetUpTest(c *gocheck.C) {
	t.graph = make(map[InstanceID][]InstanceID)
}

func (t *TarjanTest) makeIds(num int) []InstanceID {
	ids := make([]InstanceID, num)
	for i := range ids {
		ids[i] = NewInstanceID()
	}
	return ids
}

// tests that the graph 0 -> 1 -> 2 -> 3 -> 4 returns 4 components
func (t *TarjanTest) TestNonStronglyConnectedGraph(c *gocheck.C) {
	ids := t.makeIds(5)
	expected := make([][]InstanceID, 5)
	for i := range ids {
		if i < 4 {
			t.graph[ids[i]] = []InstanceID{ids[i+1]}
		} else {
			t.graph[ids[i]] = []InstanceID{}
		}
		// nodes should be sorted in reverse topological order
		expected[4-i] = []InstanceID{ids[i]}
	}

	actual := tarjanConnect(t.graph)
	c.Check(actual, TarjanCheck, expected)
}

// tests that the graph:
//         4 <- 5 <- 6 -------> 7 -> 8 -> 11
//         \    ^   ^           ^    \
//         v    \   \           \    \
//   0 -> 1 -> 2 -> 3 -> 10     9 <---
//
// produces the components [[0],[1,2,3,4,5,6], [7,8,9], [10], [11]]
func (t *TarjanTest) TestStronglyConnected1(c *gocheck.C) {
	ids := t.makeIds(12)
	t.graph[ids[0]] = []InstanceID{ids[1]}
	t.graph[ids[1]] = []InstanceID{ids[2]}
	t.graph[ids[2]] = []InstanceID{ids[3], ids[5]}
	t.graph[ids[3]] = []InstanceID{ids[6], ids[10]}
	t.graph[ids[4]] = []InstanceID{ids[1]}
	t.graph[ids[5]] = []InstanceID{ids[4]}
	t.graph[ids[6]] = []InstanceID{ids[5], ids[7]}
	t.graph[ids[7]] = []InstanceID{ids[8]}
	t.graph[ids[8]] = []InstanceID{ids[9], ids[11]}
	t.graph[ids[9]] = []InstanceID{ids[7]}
	t.graph[ids[10]] = []InstanceID{}
	t.graph[ids[11]] = []InstanceID{}

	expected := [][]InstanceID{
		[]InstanceID{ids[11]},
		[]InstanceID{ids[7], ids[8], ids[9]},
		[]InstanceID{ids[10]},
		[]InstanceID{ids[1], ids[2], ids[3], ids[4], ids[5], ids[6]},
		[]InstanceID{ids[0]},
	}

	actual := tarjanConnect(t.graph)
	c.Check(actual, TarjanCheck, expected)
}

// test the graph on the tarjan wikipedia page
//  0 <- 2 <-- 5 <--> 6
//  \  ^ ^     ^     ^
//  v /  \     \     \
//  1 <- 3 <-> 4 <-- 7 <--(links to self)
//
// produces the components [[0,1,2], [3,4], [5,6], [7]]
func (t *TarjanTest) TestStronglyConnected2(c *gocheck.C) {
	ids := t.makeIds(8)
	t.graph[ids[0]] = []InstanceID{ids[1]}
	t.graph[ids[1]] = []InstanceID{ids[2]}
	t.graph[ids[2]] = []InstanceID{ids[0]}
	t.graph[ids[3]] = []InstanceID{ids[1], ids[2], ids[4]}
	t.graph[ids[4]] = []InstanceID{ids[3], ids[2]}
	t.graph[ids[5]] = []InstanceID{ids[2], ids[6]}
	t.graph[ids[6]] = []InstanceID{ids[5]}
	t.graph[ids[7]] = []InstanceID{ids[4], ids[7]}

	expected := [][]InstanceID{
		[]InstanceID{ids[0], ids[1], ids[2]},
		[]InstanceID{ids[3], ids[4]},
		[]InstanceID{ids[5], ids[6]},
		[]InstanceID{ids[7]},
	}

	actual := tarjanConnect(t.graph)
	c.Check(actual, TarjanCheck, expected)
}
