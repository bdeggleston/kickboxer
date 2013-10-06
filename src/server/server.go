/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/5/13
 * Time: 8:22 PM
 * To change this template use File | Settings | File Templates.
 */
package server

import (
	"cluster"
	"store"
)


type Server struct {
	cluster *cluster.Cluster
	store *store.Store
}
