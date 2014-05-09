package cluster

import (
	"fmt"
	"net"
)

import (
	"node"
)

type PeerServer struct {

	cluster *Cluster
	listener net.Listener
	listenAddr string

	isRunning bool

}

func NewPeerServer(cluster *Cluster, listenAddr string) *PeerServer {
	return &PeerServer{
		cluster:cluster,
		listenAddr:listenAddr,
	}
}

// executes a request and returns a response message
func (s *PeerServer) executeRequest(node node.Node, request Message, requestType uint32) (Message, error) {
	switch requestType {
	case DISCOVER_PEERS_REQUEST:
		peerData := s.cluster.getPeerData()
		return &DiscoverPeerResponse{Peers:peerData}, nil

	case READ_REQUEST:
		//

	case WRITE_REQUEST:
		//

	case STREAM_REQUEST:
		//
		go s.cluster.streamToNode(node)
		return &StreamResponse{}, nil

	case STREAM_DATA_REQUEST:
		//

	case STREAM_COMPLETE_REQUEST:
		//

	default:
		return nil, fmt.Errorf("unexpected message type: %T", request)
	}
	panic("unreachable")
}

func (s *PeerServer) handleConnection(conn net.Conn) error {
	// check that the opening message is a ConnectionRequest
	msg, _, err := ReadMessage(conn)
	if err != nil {
		refusal := &ConnectionRefusedResponse{
			Reason:fmt.Sprintf("Error reading message: %v", err),
		}
		WriteMessage(conn, refusal)
		conn.Close()
		return fmt.Errorf("Error reading opening message")
	}

	// if it's not, refuse the request
	connectionRequest, ok := msg.(*ConnectionRequest)
	if !ok {
		errMsg := fmt.Sprintf("ConnectionRequest expected, got: %T", msg)
		refusal := &ConnectionRefusedResponse{
			Reason:errMsg,
		}
		WriteMessage(conn, refusal)
		conn.Close()
		return fmt.Errorf(errMsg)
	}

	// otherwise, accept it
	acceptance := &ConnectionAcceptedResponse{
		NodeId:s.cluster.GetNodeId(),
		Name:s.cluster.GetName(),
		Token:s.cluster.GetToken(),
	}
	if err := WriteMessage(conn, acceptance); err != nil {
		fmt.Println(err)
		return fmt.Errorf("Error writing acceptance: %v", err)
	}

	// register node with cluster
	node := NewRemoteNodeInfo(
		connectionRequest.NodeId,
		connectionRequest.DCId,
		connectionRequest.Token,
		connectionRequest.Name,
		connectionRequest.Addr,
		s.cluster,
	)
	s.cluster.addNode(node)

	for {
		// get the request
		request, requestType, err := ReadMessage(conn)
		if err != nil {
			errMsg := fmt.Sprintf("Error reading request: %v", err)
			fmt.Println(errMsg)
			conn.Close()
			return fmt.Errorf(errMsg)
		}

		// handle the close connection message
		if requestType == close_connection {
			conn.Close()
			return nil
		}

		// get the response
		response, err := s.executeRequest(node, request, requestType)
		if err != nil {
			errMsg := fmt.Sprintf("Error executing request: %v", err)
			fmt.Println(errMsg)
			conn.Close()
			return fmt.Errorf(errMsg)
		}

		// send response
		if err = WriteMessage(conn, response); err != nil {
			return fmt.Errorf("Error writing response: %v", err)
		}
	}
	return nil
}

func (s *PeerServer) GetAddr() string {
	return s.listenAddr
}


func (s *PeerServer) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// return if the server has been stopped
			if !s.isRunning {
				return
			}
			errMsg := fmt.Sprintf("Error accepting connection: %T %v", err, err)
			fmt.Println(errMsg)
			continue
		}
		go s.handleConnection(conn)
	}

}

func (s *PeerServer) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.listener = ln
	go s.acceptConnections()
	s.isRunning = true
	return nil
}

func (s *PeerServer) Stop() error {
	err := s.listener.Close()
	s.isRunning = false
	return err
}

