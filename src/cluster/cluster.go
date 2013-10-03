/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:47 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

type Token [16]byte

type Cluster struct {
	// nodes to communicate with to
	// discover the rest of the cluster
	seeds []node

	// the number of nodes a key should
	// be replicated to
	replicationFactor int32

}
