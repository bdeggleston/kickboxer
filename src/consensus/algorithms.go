package consensus

// implementation of tarjan's strongly connected components algorithm
// give it a directed graph, and it will return a list of instance
// id sets, grouped into strongly connected components, and sorted in
// reverse topological order of the components
func tarjanConnect(graphMap map[InstanceID][]InstanceID) [][]InstanceID {
	// at least one component will be returned
	output := make([][]InstanceID, 0, 1)

	type vertex struct {
		id InstanceID
		out []InstanceID
		index int
		lowlink int
		stacked bool
	}

	graph := make(map[InstanceID]*vertex, len(graphMap))
	vertices := make([]*vertex, len(graphMap))
	i := 0
	for iid, out := range graphMap {
		v := &vertex{id:iid, out:out}
		graph[iid] = v
		vertices[i] = v
		i++
	}

	index := 0
	stack := make([]*vertex, 0, len(vertices))

	var strongConnect func(*vertex)
	strongConnect = func(v *vertex) {
		index++

		v.index = index
		v.lowlink = index
		v.stacked = true


		initialStackSize := len(stack)
		stack = append(stack, v)

		// look at the out vertices
		var w *vertex
		for _, id := range v.out {
			w = graph[id]
			if w == nil {
				continue
			} else if w.index == 0 {
				// vertex hasn't been visited yet
				strongConnect(w)
				if w.lowlink < v.lowlink {
					v.lowlink = w.lowlink
				}
			} else if w.stacked {
				// vertex is in the stack, so is part of the
				// current strongly connected component
				if w.lowlink < v.lowlink {
					v.lowlink = w.lowlink
				}
			}
		}

		// if this is the root for the current component
		if v.index == v.lowlink {
			// pop vertices off the stack until we get to
			// the current vertex, and add them to the new
			// strongly connected component
			currentStackSize := len(stack)
			component := make([]InstanceID, 0, currentStackSize - initialStackSize)
			i := len(stack) - 1
			w = nil
			for w == nil || v.id != w.id {
				w = stack[i]
				stack = stack[:i]
				component = append(component, w.id)
				i--
			}
			output = append(output, component)
		}

		v.stacked = false
	}

	// iterate over the vertice
	for _, v := range vertices {
		if v.index == 0 {
			strongConnect(v)
		}
	}
	return output
}
