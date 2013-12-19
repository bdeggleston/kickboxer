terminology:
    * instance: A single epaxos request flow
    * committing: agreement by quorum of replicas on the order of a command
    * execution: actual mutation of dataset

multi paxos:
 for each incrementing instance, mpaxos determines which command to execute

egalitarian paxos
 epaxos determines the ordering of the instances

# Egalitarian Paxos implementation

A variant of paxos that is masterless and typically requires fewer requests

## Request cycle

### Preparation

The client sends a command to one of the queried key's replicas.

### Step 1

The leader sends a numbered `pre accept` message to the key's replicas, as well as a
list of any interfering dependency commands it has seen.

outcomes:
    * If the proposal doesn't interfere with any of the
    * If a replica has an interfering command dependency(ies), it replies with it's
      info. The leader applies that info and restarts the process
    * If the leader's history matches the replica's history, the replica accepts the proposal

### Step 2

    * Leader commits locally and transmits a commit message to all replicas
    * Client is informed of success


What happens if the leader fails, the commit message isn't received by all replicas
If a replica accepted the proposal, but did not receive a commit message, either the leader failed,
or it's data is stale.


```
Success case
R1  R2  R3
|   |   |
X-->|-->|  Command 1 Proposal [mutate x]
X-->|-->|  Command 2 Proposal [mutate y] (doesn't interfere with proposal 1)
|<--X---X  Command 1 Accept [mutate x ok]
X-->|-->|  Command 1 Commit [mutate x]
S   |   |  Success
```

```
Interfering command
R1  R2  R3
|   |   |
|   |   !  Network partition, R3 unreachable
X-->|      Command 1 Proposal  [mutate x]
|<--X      Command 1 PreAccept  [mutate x ok]
X-->|      Command 1 Commit [mutate x]
|   |
|   |
|   |   *  Network partition ends, R3 reachable
|   |<--X  Command 2 Proposal [mutate x] (interferes with Command 1)
R-->R-->|  Command 2 Rejected, Command 1 dependency returned
|   |   W  Command 1 Applied
|<--|<--X  Command 2 Proposal [mutate x]
X-->X-->|  Command 2 PreAccepted
|<--|<--X  Command 2 Commit [mutate x]
|   |   S  Success
|   |   |
```

What happens when the leader fails to commit the command to a quorum
of replicas after they've accepted?

```
Lost commit message
R1  R2  R3
|   |   |
|   |   !  Network partition!
X-->|      Command 1 Proposal [mutate x]
|<--X      Command 1 PreAccept [mutate x ok]
|   |
|   !      Network partition!
|
X-->?-->?  Command 1 Commit [mutate x]
S          Client Success?
|   *   *  Network partition ends
|   |   |
!   |   |  Network partition!
    |   |
    |<--X  Command 2 (mutate x) (depends on result of C1)
    X-->|  Command 2 Refused, C1 PreAccept added to R3 dependency graph.
    |   |    R2 & R3 now have C1 marked as PreAccepted. They need to determine the outcome of this command
    |   |    before they can proceed. Since a quorum of nodes has accepted, they can commit the command
```
