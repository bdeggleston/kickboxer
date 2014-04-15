package consensus

// implementation of tarjan's strongly connected components algorithm
// give it a directed graph, and it will return a list of instance
// id sets, grouped into strongly connected components
func tarjanConnect(graph map[InstanceID][]InstanceID) [][]InstanceID {
	idx := 0
	stacked := make(map[InstanceID]bool, len(graph))
	lowlinks := make(map[InstanceID]int, len(graph))
	index := make(map[InstanceID]int, len(graph))
	output := make([][]InstanceID, 0)
	stack := make([]InstanceID, 0, len(graph))

	min := func(x, y int) int {
		if x < y {
			return x
		}
		return y
	}

	var strongConnect func(InstanceID)
	strongConnect = func(v InstanceID) {
		idx++

		index[v] = idx
		lowlinks[v] = idx

		stacked[v] = true
		stack = append(stack, v)

		// look at the out vertices
		for _, w := range graph[v] {
			if index[w] == 0 {
				// vertex hasn't been visited yet
				strongConnect(w)
				lowlinks[v] = min(lowlinks[v], lowlinks[w])
			} else if stacked[w] {
				// vertex is in the stack, so is part of the
				// current strongly connected component
				lowlinks[v] = min(lowlinks[v], lowlinks[w])
			}
		}

		// if this is the root for the current component
		if index[v] == lowlinks[v] {
			// pop vertices off the stack until we get to
			// the current vertex, and add them to the new
			// strongly connected component
			component := make([]InstanceID, 0, 1)
			i := len(stack) - 1
			var w InstanceID
			for v != w {
				w = stack[i]
				stack = stack[:i]
				component = append(component, w)
				i--
			}
			output = append(output, component)
		}

		stacked[v] = false
	}

	for v := range graph {
		if index[v] == 0 {
			strongConnect(v)
		}
	}
	return output
}

// TODO: for the topological sort, panic if there is more than 1 root node
// TODO: for the topological sort, make sure that the sorting is deterministic, and only one solution will be calculated for the same graph
