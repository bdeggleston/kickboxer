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

// tests that missing vertices are skipped
func (t *TarjanTest) TestMissingVertex1(c *gocheck.C) {
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

	t.graph[ids[0]] = append(t.graph[ids[0]], NewInstanceID())
	actual := tarjanConnect(t.graph)
	c.Check(actual, TarjanCheck, expected)
}

func (t *TarjanTest) TestMissingVertex2(c *gocheck.C) {
	ids := t.makeIds(3)
	t.graph[ids[2]] = []InstanceID{NewInstanceID(), ids[0], ids[1], NewInstanceID()}
	t.graph[ids[0]] = []InstanceID{NewInstanceID(), ids[1], NewInstanceID()}
	t.graph[ids[1]] = []InstanceID{NewInstanceID(), ids[2], NewInstanceID()}

	actual := tarjanConnect(t.graph)
	expected := NewInstanceIDSet(ids)
	c.Assert(len(actual), gocheck.Equals, 1)
	c.Check(NewInstanceIDSet(actual[0]), gocheck.DeepEquals, expected)
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

	expectedComponents := [][]InstanceID{
		[]InstanceID{ids[11]},
		[]InstanceID{ids[7], ids[8], ids[9]},
		[]InstanceID{ids[10]},
		[]InstanceID{ids[1], ids[2], ids[3], ids[4], ids[5], ids[6]},
		[]InstanceID{ids[0]},
	}

	actualComponents := tarjanConnect(t.graph)

	// since the given graph has several potential topsorts, we just need to check
	// that the correct strongly connected components have been identified
	c.Check(len(actualComponents), gocheck.Equals, len(expectedComponents))
	componentCheck:
		for _, expectedComponent := range expectedComponents {
			expected := NewInstanceIDSet(expectedComponent)
			for _, actualComponent := range actualComponents {
				actual := NewInstanceIDSet(actualComponent)
				if expected.Equal(actual) {
					continue componentCheck
				}
			}
			c.Fatalf("Component %v not found in %v", expectedComponent, actualComponents)
		}
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

// test the graph on the tarjan wikipedia page
//  2 <-> 1
//  \   ^
//  v  /
//   0
//
// produces the components [[0,1,2]]
func (t *TarjanTest) TestStronglyConnected3(c *gocheck.C) {
	ids := t.makeIds(3)
	t.graph[ids[0]] = []InstanceID{ids[1]}
	t.graph[ids[1]] = []InstanceID{ids[2]}
	t.graph[ids[2]] = []InstanceID{ids[0], ids[1]}

	actual := tarjanConnect(t.graph)
	expected := NewInstanceIDSet(ids)
	c.Assert(len(actual), gocheck.Equals, 1)
	c.Check(NewInstanceIDSet(actual[0]), gocheck.DeepEquals, expected)
}

// tests that a strongly connected graph, where components have
// to get through previously visited components to get to the
// graph root works properly
//
// This test demonstrates a hard to detect bug, where vertices
// were being marked 'off-stack' after they were first visited,
// not when they were actually removed from the stack
func (t *TarjanTest) TestStronglyConnected4(c *gocheck.C) {
	root := NewInstanceID()
	lvl1 := t.makeIds(10)
	lvl2 := t.makeIds(10)

	// the root points to all level1 vertices
	t.graph[root] = lvl1

	// each lvl1 vertex points to all lvl2 vertices,
	// but non of the lvl2 vertices
	for _, iid := range lvl1 {
		t.graph[iid] = lvl2
	}

	// and all lvl2 vertices point back to the root
	for _, iid := range lvl2 {
		t.graph[iid] = []InstanceID{root}
	}

	component := make([]InstanceID, 0, len(lvl1) + len(lvl2) + 1)
	component = append(component, root)
	component = append(component, lvl1...)
	component = append(component, lvl2...)

	expected := [][]InstanceID{
		component,
	}

	actual := tarjanConnect(t.graph)
	c.Check(actual, TarjanCheck, expected)
}

type TarjanBenchmark struct {
	graph map[InstanceID][]InstanceID
}

var _ = gocheck.Suite(&TarjanBenchmark{})

func (t *TarjanBenchmark) SetUpSuite(c *gocheck.C) {
	t.graph = make(map[InstanceID][]InstanceID)
	ids := make([]InstanceID, 12)
	for i := range ids {
		ids[i] = NewInstanceID()
	}
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
}

func (t *TarjanBenchmark) BenchmarkTarjan(c *gocheck.C) {
	// baseline: 12651 ns/op
	// removing maps, using structs: 6061 ns/op
	// using if, not min func: 5817 ns/op
	// allocating an output array with initial cap of 1: 5719 ns/op
	// allocating a component slice based on how much the stack grew: 5256 ns/op
	for i:=0; i<c.N; i++ {
		tarjanConnect(t.graph)
	}
}

