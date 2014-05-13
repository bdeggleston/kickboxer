package cluster

import (
	"bytes"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"message"
	"node"
	"partitioner"
	"types"
)

type ClusterMessageTest struct {}

var _ = gocheck.Suite(&ClusterMessageTest{})

func (t *ClusterMessageTest) checkMessage(c *gocheck.C, src message.Message) {
	var err error
	buf := &bytes.Buffer{}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	// test num bytes
	c.Check(len(buf.Bytes()), gocheck.Equals, src.NumBytes() + message.MESSAGE_HEADER_SIZE)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (t *ClusterMessageTest) TestConnectionRequest(c *gocheck.C) {
	src := &ConnectionRequest{PeerData{
		NodeId:node.NewNodeId(),
		DCId:"DC5000",
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:partitioner.Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestConnectionAcceptedResponse(c *gocheck.C) {
	src := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:"DC5000",
		Name:"Test Node",
		Token:partitioner.Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestConnectionRefusedResponse(c *gocheck.C) {
	src := &ConnectionRefusedResponse{Reason:"you suck"}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestDiscoverPeersRequest(c *gocheck.C) {
	src := &DiscoverPeersRequest{
		NodeId:node.NewNodeId(),
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestDiscoverPeersResponse(c *gocheck.C) {
	src := &DiscoverPeerResponse{
		Peers: []*PeerData{
			&PeerData{
				NodeId:node.NewNodeId(),
				DCId:DatacenterId("DC5000"),
				Addr:"127.0.0.1:9998",
				Name:"Test Node1",
				Token:partitioner.Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
			},
			&PeerData{
				NodeId:node.NewNodeId(),
				DCId:DatacenterId("DC2000"),
				Addr:"127.0.0.1:9999",
				Name:"Test Node2",
				Token:partitioner.Token([]byte{1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0}),
			},
		},
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestReadRequest(c *gocheck.C) {
	src := &ReadRequest{
		Cmd:  "GET",
		Key:  "A",
		Args: []string{"B", "C"},
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestWriteRequest(c *gocheck.C) {
	src := &WriteRequest{
		ReadRequest: ReadRequest{
			Cmd:  "GET",
			Key:  "A",
			Args: []string{"B", "C"},
		},
		Timestamp: time.Now(),
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestQueryResponse(c *gocheck.C) {
	src := &QueryResponse{
		Data: [][]byte{
			types.NewUUID4().Bytes(),
			types.NewUUID4().Bytes(),
			types.NewUUID4().Bytes(),
		},
	}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamRequest(c *gocheck.C) {
	src := &StreamRequest{}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamResponse(c *gocheck.C) {
	src := &StreamResponse{}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamCompleteRequest(c *gocheck.C) {
	src := &StreamCompleteRequest{}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamCompleteResponse(c *gocheck.C) {
	src := &StreamCompleteResponse{}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamDataRequest(c *gocheck.C) {
	src := &StreamDataRequest{Data: []*StreamData{
		&StreamData{Key: "blake", Data: []byte("eggleston")},
		&StreamData{Key: "travis", Data: []byte("eggleston")},
		&StreamData{Key: "cameron", Data: []byte("eggleston")},
	}}
	t.checkMessage(c, src)
}

func (t *ClusterMessageTest) TestStreamDataResponse(c *gocheck.C) {
	src := &StreamDataResponse{}
	t.checkMessage(c, src)
}
