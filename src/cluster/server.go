/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/6/13
 * Time: 7:44 AM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"fmt"
	"net"
)

type PeerServer struct {

	cluster *Cluster
	listener net.Listener
	listenAddr string

}

func NewPeerServer(cluster *Cluster, listenAddr string) *PeerServer {
	return &PeerServer{
		cluster:cluster,
		listenAddr:listenAddr,
	}
}

func (s *PeerServer) handleConnection(conn net.Conn) {

}

func (s *PeerServer) GetAddr() string {
	return s.listenAddr
}


func (s *PeerServer) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println(err)
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
	return nil
}

func (s *PeerServer) Stop() error {
	return s.listener.Close()
}

