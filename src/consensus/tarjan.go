package consensus
/**
 * Tarjan implementation, ripped and adapted from:
 *  github.com/looplab/tarjan
*/

// Connections creates a slice where each item is a slice of strongly connected vertices.
//
// If a slice item contains only one vertex there are no loops. A loop on the
// vertex itself is also a connected group.
//
// The example shows the same graph as in the Wikipedia article.
func TarjanSort(graph map[InstanceID][]InstanceID) [][]InstanceID {
	g := &data{
		graph: graph,
		nodes: make([]vertex, 0, len(graph)),
		index: make(map[InstanceID]int, len(graph)),
	}
	for v, _ := range g.graph {
		if _, ok := g.index[v]; !ok {
			g.strongConnect(v)
		}
	}
	return g.output
}

// data contains all common data for a single operation.
type data struct {
	graph  map[InstanceID][]InstanceID
	nodes  []vertex
	stack  []InstanceID
	index  map[InstanceID]int
	output [][]InstanceID
}

// vertex stores data for a single vertex in the connection process.
type vertex struct {
	lowlink int
	stacked bool
}

// strongConnect runs Tarjan's algorithm recursivley and outputs a grouping of
// strongly connected vertices.
func (data *data) strongConnect(v InstanceID) *vertex {
	index := len(data.nodes)
	data.index[v] = index
	data.stack = append(data.stack, v)
	data.nodes = append(data.nodes, vertex{lowlink: index, stacked: true})
	node := &data.nodes[index]

	for _, w := range data.graph[v] {
		i, seen := data.index[w]
		if !seen {
			n := data.strongConnect(w)
			if n.lowlink < node.lowlink {
				node.lowlink = n.lowlink
			}
		} else if data.nodes[i].stacked {
			if i < node.lowlink {
				node.lowlink = i
			}
		}
	}

	if node.lowlink == index {
		var vertices []InstanceID
		i := len(data.stack) - 1
		for {
			w := data.stack[i]
			stackIndex := data.index[w]
			data.nodes[stackIndex].stacked = false
			vertices = append(vertices, w)
			if stackIndex == index {
				break
			}
			i--
		}
		data.stack = data.stack[:i]
		data.output = append(data.output, vertices)
	}

	return node
}
