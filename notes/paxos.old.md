#tentative paxos implementation

write request comes in from a client

## step 0

coordinator sends it's request to the master of the key range it would like to read/write

## step 1

master sends a numbered proposal to replicas of a requested key, along with any query predicates

outcomes:
    * a quorum of replicas have not seen a proposal with a greater or equal number, and respond with
        a promise to never again accept a proposal with a number <= to this propsal
    * one or more replicas have seen a >= numbered proposal and reject this proposal, notifying the coordinator
        of the highest proposal number they've seen. 
            * if this is the first rejection from this paxos round, the coordinator starts the process again with the number
                included in the rejection + 1. 
            * if this is the second rejection, the coordinator notifies the client that the write can't be completed
    * a quorum of replicas doesn't respond within the timeout, the client is notified the write can't be completed

## step 2

accept message is sent to replicas

outcomes:
    * a quorum of replicas reply with an accept message
    * a quorum of replicas have previously promised to a higher proposal. They reject the accept message. The client is notified the write can't be completed

Problems with applying the write at this stage:
    if 3 commit messages are sent out which actually mutate the data, 1 is accepted. and 2 are rejected, what happens with the 1 commit?
        This appears to be the purpose of the learners, which will detect and repair inconsistencies like these.

        What happens in this situation? A3 is now inconsistent, I'd like to avoid having to reconcile both timestamps and transaction ids.
        Especially since there will be cases where it's impossible to reconcile (ie: A1 is down, there's no way to know whether A2 or A3 is correct).

            P1  A1  A2  A3  P2
            |   |   |   |   |
            X-->|-->|-->|   |  Proposal 1
            |   |   |   |   |
            |<--X---X---X   |  Promise 1
            |   |   |   !   |  Network partition begins
            |   |   |       |
            |   |<--|<------X  Proposal 2
            |   X-->X------>|  Promise 2
            |   |   |       |
            |   |   |   *   |  Network partition ends
            X-->|-->|-->|   |  Commit 1
            |   |   |   |   |
            |<--X---X   |   |  Reject 1
            |<----------X   |  Commit 1 (What happens?)
            |   |   |   |   |  Abort 1
            |   |   |   |   |
            |   |<--|<------X  Commit 2
            |   X-->X------>|  Accept 1
            |   |   |   X-->|  Reject 1
            |   |   |   |   |  Terminate 1



## step 3 (maybe)

a quorum write is performed normally. Normal timestamp reconciliation will work normally.

At this point, the write may still fail, or the client may receive a false negative if the writes succeed and the replica's ack responses timeout, but the consensus process guarantees the order of operations across the replicas.

Problems with applying the write at this stage:
    the accepted value cannot be read by other clients between the time it's accepted and the time the blessed write happens. 
        Is this actually a problem?
            Using the consensus process would basically be useless as a locking mechanism
        
## additional notes

In basic paxos, the acceptors only notify a learner, which actually does the work, then notifies the client

additionally, you usually need to elect a master proposer, to prevent a neverending one up loop between conflicting proposers




