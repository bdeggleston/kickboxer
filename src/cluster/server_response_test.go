package cluster

import (
	"launchpad.net/gocheck"
)

import (
	"node"
)

type ServerResponseTest struct {}

var _ = gocheck.Suite(&ServerResponseTest{})

func (t *ServerResponseTest) TestServerDiscoverPeersResponse(c *gocheck.C) {
	clstr := makeRing(5, 3)
	server := &PeerServer{cluster:clstr}

	n := NewRemoteNodeInfo(
		node.NewNodeId(),
		"DC1",
		clstr.partitioner.GetToken("asdfghjkl"),
		"New Node",
		"127.0.0.5:9999",
		clstr,
	)
	msg := &DiscoverPeersRequest{NodeId:n.GetId()}
	response, err := server.executeRequest(n, msg)
	c.Assert(err, gocheck.IsNil)

	c.Assert(response, gocheck.FitsTypeOf, &DiscoverPeerResponse{})
	peerResponse := response.(*DiscoverPeerResponse)
	c.Check(len(peerResponse.Peers), gocheck.Equals, len(clstr.getPeerData()))
}

func (t *ServerResponseTest) TestStreamRequestResonse(c *gocheck.C) {

}


