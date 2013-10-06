package main

import (
	"fmt"
	"cluster"
	"store"
)

func main() {
	fmt.Printf("Hello world!")
	_ = cluster.Cluster{}
	_ = store.Store{}

}
