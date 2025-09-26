# Raft Implementation

Every node is in one of the three possible state/role:
-   Follower
-   Candidate
-   Leader

Every node starts with a _Follower_ role. Waits for leader to contact (ping or whatever), has a timeout for that. Once the timeout is 
reached, it starts a new election and transitions into a _Candidate_ role.

### Leader Elections

If there is no contact from leader for a specified time period, the follower transitions into a candidate role and starts election.

Every Election has a unique monotonically increasing term id, hence the candidates send a msg to all followers with unique term id basically asking for vote

The follower ignores the msg if it has already voted for that term

The Leader should have the most up-to-date log records, hence while voting for the candidate, the follower votes to the candidate only 
if the candidates latest log record is equal or ahead of the follower

So the follower votes for the candidate only if:
-   It has not voted for the same term before
-   The candidate has more up-to-date log than it

### Log Replication

Once the leader is elected, the leader starts taking requests from the clients. Upon receiving a request, the leader attaches term Id
and msg Id to the record and sends to all the followers and waits for the majority to respond





## Implementation details

### Leader elections

-   Leader has to send heartbeats to everyone
-   Candidate asks for vote, waits for votes with a timeout
-   Follower sending acks to candidate as vote


#### Msgs

Candidate asking for vote:
rpc name -> RequestVote

RequestVoteArgs {
    -   NodeId
    -   TermId
    -   {TermId, MsgId} for last msg
}

Followers respond with empty ack:
RequestVoteAck {
     
}