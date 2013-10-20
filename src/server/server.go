package server

import (
	"cluster"
	"store"
)


type Server struct {
	cluster *cluster.Cluster
	store *store.Store
}
