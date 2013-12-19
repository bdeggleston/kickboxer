# tentative consensus implementation

Collapsed Paxos, with

## Queries

For any given key, there needs to be a consensus master to handle transactions.

### step 1

client sends a read/write request with consistency consensus to a coordinator node

### step 2

coordinator forwards request to the current consensus master for that key (master covered later)

### step 3

master sends a numbered proposal to replicas of a requested key, along with any query predicates

outcomes:
    * a quorum of replicas have not seen a proposal with a greater or equal number, and respond with
        a promise to never again accept a proposal with a number <= to this propsal. Query proceeds
    * one or more replicas have seen a >= numbered proposal and reject this proposal, notifying the
        coordinator of the highest proposal number they've seen. And their info about the current master
    * a quorum of replicas doesn't respond within the timeout, the client is notified the write can't be
        completed

### step 4

accept request is sent to replicas

outcomes:
    * a quorum of replicas reply with an accept message
    * a quorum of replicas have previously promised to a higher proposal. They reject the accept message,
        reply with the highest proposal number they've seen, and info about their current master.
        The client is notified the write can't be completed. (Should the coordinator try again?)

### step 5

master performs quorum write normally. Normal timestamp reconciliation will work.

At this point, the write may still fail, or the client may receive a false negative if the writes succeed
and the replica's ack responses timeout, but the consensus process guarantees the order of operations across
the replicas.

### Optimizations:
    * clients are kept informed about key range masters, allowing steps 1 & 2 to be skipped.

## Masters

Each owned key range (ie: given a replication factor of 1, the keys owned by a single node), needs to have
a consensus master, which will serve all consensus requests during it's term as master. Non master replicas
must refuse/redirect queries to the current master.

Having consensus masters has 2 benefits.
    1. All consensus requests are funneled through a single node. This avoids situations
        where multiple proposers get stuck in a loop of propose/reject while they each
        continuously one up each other, stalling multiple requests.
    2. Reads do not actually have to execute the consensus protocol to return the agreed upon
        value, since the master will have the agreed upon value locally. It may even be possible
        to just verify with the other replicas that the master is still, in fact, the master, saving
        a potential disk seek on the replica. Also, consider that the actual write comes after the
        commit message is accepted.

### Master Election and Failover

TODO: figure this out
The vanilla paxos protocol allows a node to be safely promoted to master. However, it makes the system
vulnerable to rapid master turnover when there are load problems, probably making them worse. Also, network
problems could cause rapid master churn.

#### How a master gets elected
    * A replica begins a consensus round. Proposing itself (or another node) as master, using the propose/promise
      accept/ack cycle described above for queries, but skipping step 5. The tentative

#### How a master retains it's status

#### How a master loses it's status
    *

#### When does a new master get elected?
    * When a coordinator can't reach the agreed upon master?
        - what about a partition between the coordinator and the rest of the nodes?

### Optimizations:
     * Split the token range of a node into smaller pieces, each ownable by a different master. This will spread
       the masters for a given node across it's replicas and reduce the chances of one node operating
       as the master for multiple node key ranges
     * Master nodes voluntarily give up their master status for one or more of if their controlled token ranges
       if machine load is significantly higher than the other replicase
     * To think about: what would prevent a master from collapsing it's propose and accept messages into a single
       request? If another master has been selected by through legit paxos, and the other nodes recognize another
       node as the master, wouldn't they reject the commit message? Also, consider that the actual write comes
       after the commit message is accepted.

## Notes:
    * Any data related to maintaining consensus (proposal numbers, master election, etc) must be flushed to disk before
      an acknowledgement is sent

    * I'm considering implementing the consensus system as a hierarchy. This would allow consensus at a cluster
      level, a dc level, a key level, or a sub key level (ie: updating 2 hash values in the same key concurrently).

