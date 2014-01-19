package consensus

import (
	"time"
)

import (
	"node"
)

// sends and explicit prepare request to the other nodes. Returns a channel for receiving responses on
// This method does not wait for the responses to return, because the scope needs to be locked during
// message sending, so the instance is not updated, but does not need to be locked while receiving responses
func (s *Scope) sendPrepare(instance *Instance, replicas []node.Node) (<- chan *PrepareResponse, error) {
	recvChan := make(chan *PrepareResponse, len(replicas))
	msg := &PrepareRequest{Scope:s.name, Ballot:instance.MaxBallot, InstanceID:instance.InstanceID}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PrepareResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PrepareResponse); ok {
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected Prepare response type: %T", response)
			}
		}
	}

	for _, replica := range replicas {
		go sendMsg(replica)
	}

	return recvChan, nil
}

func (s *Scope) receivePrepareResponseQuorum(recvChan <-chan *PrepareResponse, instance *Instance, quorumSize int, numReplicas int) ([]*PrepareResponse, error) {
	numReceived := 1  // this node counts as a response
	timeoutEvent := time.After(time.Duration(PREPARE_TIMEOUT) * time.Millisecond)
	var response *PrepareResponse
	responses := make([]*PrepareResponse, 0, numReplicas)
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("Prepare response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("Prepare timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}

	// finally, receive any additional responses
drain: for {
	select {
	case response = <-recvChan:
		responses = append(responses, response)
		numReceived++
	default:
		break drain
	}
}

	return responses, nil
}

// runs explicit prepare phase on instances where a command leader failure is suspected
// during execution,
// TODO:
//	what happens if 2 nodes send each other prepare messages at the same time?
func (s *Scope) prepareInstance(instance *Instance) error {
	replicas := s.manager.getScopeReplicas(s)

	// increments and sends the prepare messages in a single lock
	incrementInstanceAndSendPrepareMessage := func() (<-chan *PrepareResponse, error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.MaxBallot++
		if err := s.Persist(); err != nil {
			return nil, err
		}
		return s.sendPrepare(instance, replicas)
	}
	recvChan, err := incrementInstanceAndSendPrepareMessage()
	if err != nil { return err }

	// receive responses from at least a quorum of nodes
	quorumSize := ((len(replicas) + 1) / 2) + 1
	responses, err := s.receivePrepareResponseQuorum(recvChan, instance, quorumSize, len(replicas))
	if err != nil { return err }

	// find the highest response ballot
	maxBallot := uint32(0)
	for _, response := range responses {
		if ballot := response.Instance.MaxBallot; ballot > maxBallot {
			maxBallot = ballot
		}
	}

	// find the highest response status
	maxStatus := InstanceStatus(byte(0))
	for _, response := range responses {
		if status := response.Instance.Status; status > maxStatus {
			maxStatus = status
		}
	}

	switch maxStatus {
	case INSTANCE_PREACCEPTED:
		// run pre accept phase
		fallthrough
	case INSTANCE_ACCEPTED:
		// run accept phase
		fallthrough
	case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
		// commit instance


	}
	return nil
}

