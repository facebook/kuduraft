----------------------------- MODULE flexiraft -----------------------------

EXTENDS Naturals, FiniteSets, Sequences, TLC

(* The set of server IDs.                                                  *)
CONSTANTS Server

(* The set of requests that can go into the log.                           *)
CONSTANTS Value

(* Server states.                                                          *)
CONSTANTS Follower, Candidate, Leader

(* A reserved value.                                                       *)
CONSTANTS Nil

(* Message types:                                                          *)
CONSTANTS RequestVoteRequest, RequestVoteResponse,
          AppendEntriesRequest, AppendEntriesResponse

(* Size of the data commit quorum.                                         *)
CONSTANTS DataQuorumSize

(* DataQuorumSize should be at least 1.                                    *)
ASSUME DataQuorumSize >= 1

(* DataQuorumSize cannot be greater than the number of servers. It also    *)
(* defeats the purpose if the quorum consists of all servers.              *)
ASSUME DataQuorumSize < Cardinality(Server)

----

(***************************************************************************)
(*************************** Global variables ******************************)
(***************************************************************************)


(* A bag of records representing requests and responses sent from one      *)
(* server to another. Represented as a function mapping each unique        *)
(* message to its frequency in the message channel                         *)
VARIABLE messages

(* A history variable used in the proof.                                   *)
(* Keeps track of successful elections, including the initial logs of the  *)
(* leader and voters' logs. Implemented as a set of structs.               *)
VARIABLE elections

(* A history variable used in the proof. This would not be present in an   *)
(* implementation. Keeps track of all the logs in the system.              *)
(* A set of sequences where each sequence is a separate log.               *)
VARIABLE allLogs

(* Number of client requests.                                              *)
VARIABLE numClientRequests

(* Number of restarts.                                                     *)
VARIABLE numRestarts

(* Number of server timeouts.                                              *)
VARIABLE numTimeouts

(* A set of variables to control the upper bound on the number of states.  *)
stateSpaceVars == <<numClientRequests, numRestarts, numTimeouts>>

----

(***************************************************************************)
(*************************** Server variables ******************************)
(***************************************************************************)
(* The following variables are all functions with domain Server            *)

(* The server's term number.                                               *)
VARIABLE currentTerm

(* The server's state (Follower, Candidate, or Leader).                    *)
VARIABLE state

(* The candidate the server voted for in its current term, or Nil if it    *)
(* if it hasn't voted for any.                                             *)
VARIABLE votedFor
serverVars == <<currentTerm, state, votedFor>>

(* A mapping from server to a sequence of log entries. The index into this *)
(* sequence is the index of the log entry.                                 *)
VARIABLE log

(* A mapping from server to the index of the latest entry in the log the   *)
(* state machine may apply.                                                *)
VARIABLE commitIndex
logVars == <<log, commitIndex>>

----

(***************************************************************************)
(************************* Candidate variables *****************************)
(***************************************************************************)
(* The following variables are used only on candidates                     *)

(* The set of servers from which the candidate has received a RequestVote  *)
(* response in its currentTerm.                                            *)
VARIABLE votesResponded

(* The set of servers from which the candidate has received a vote in its  *)
(* currentTerm.                                                            *)
VARIABLE votesGranted

(* A history variable used in the proof. This would not be present in an   *)
(* implementation.                                                         *)
(* Function from each server that voted for this candidate in its          *)
(* currentTerm to that voter's log.                                        *)
VARIABLE voterLog
candidateVars == <<votesResponded, votesGranted, voterLog>>

----

(***************************************************************************)
(*************************** Leader variables ******************************)
(***************************************************************************)
(* The following variables are used only on leaders.                       *)

(* The next entry to send to each follower.                                *)
VARIABLE nextIndex

(* The latest entry that each follower has acknowledged is the same as the *)
(* leader's. This is used to calculate commitIndex on the leader.          *)
VARIABLE matchIndex
leaderVars == <<nextIndex, matchIndex, elections>>

(* End of per server variables.                                            *)
----

(***************************************************************************)
(*************************** Quorum variables ******************************)
(***************************************************************************)
(* The following variables are used to define flexiraft quorums.           *)

(* A mapping from servers to a set of subsets of Server that constitute    *)
(* the data quorum.                                                        *)
VARIABLE dataQuorum

(* A mapping from servers to the term in which the data quorum was used    *)
(* in. For leader, this is the term of the election victory and for        *)
(* followers, it's the term that was used to replicate entries.            *)
VARIABLE dataQuorumTerm

(* A mapping from servers to a set of subsets of Server that constitute    *)
(* the leader election quorum.                                             *)
VARIABLE leaderElectionQuorum

quorumVars == <<dataQuorum, dataQuorumTerm, leaderElectionQuorum>>

----

(* All variables; used for stuttering (asserting state hasn't changed).    *)
vars == <<messages, allLogs, stateSpaceVars, serverVars, candidateVars,
          leaderVars, logVars, quorumVars>>

----
(***************************************************************************)
(******************************** Helpers **********************************)
(***************************************************************************)

(* Defines the leader election quorum from the data quorum.                *)
(* Only important property is that every leader election quorum overlaps   *)
(* with every data commit quorum.                                          *)

(* All non empty proper subsets of server that intersect with all members  *)
(* of data commit quorum.                                                  *)
IntersectingQuorums(dQuorum) == {
    i \in SUBSET(Server) :
        /\ i # {}
        /\ i # Server
        /\ (\A j \in dQuorum : i \intersect j # {})
}

(* Leader election quorums should also intersect with each other.          *)
(* Making sure that there is at least one leader election quorum which is  *)
(* not dependent on the leader for dead master promotion.                  *)
ValidLeaderElectionQuorums(dQuorum, leader) == {
    i \in SUBSET(IntersectingQuorums(dQuorum)) :
        /\ i # {}
        /\ \A j, k \in i : j \intersect k # {}
        /\ \E q \in i : leader \notin q
}

(* Choose the leader election quorum with at least 3 elements such that    *)
(* there is no single node that is a point of failure, making the quorum   *)
(* choice more realistic but this condition is not mandatory.              *)
(* If the same node is part of all leader election quorums, quorum loss    *)
(* would result from a single node's failure.                              *)
LeaderElectionQuorum(dQuorum, leader) ==
    CHOOSE i \in ValidLeaderElectionQuorums(dQuorum, leader) :
        /\ Cardinality(i) >= 3
        /\ (\E j, k, l \in i :
                /\ j # k
                /\ j # l
                /\ k # l
                /\ (j \intersect k) # (j \intersect l))

(* The term of the last entry in a log, or 0 if the log is empty.          *)
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

(* Helper for Send and Reply. Given a message m and bag of messages,       *)
(* return a new bag of messages with one more m in it.                     *)
WithMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        [msgs EXCEPT ![m] = msgs[m] + 1]
    ELSE
        msgs @@ (m :> 1)

(* Helper for Discard and Reply. Given a message m and bag of messages,    *)
(* return a new bag of messages with one less m in it.                     *)
WithoutMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        IF msgs[m] > 1 THEN
            [msgs EXCEPT ![m] = msgs[m] - 1]
        ELSE
            [m2 \in (DOMAIN msgs \ {m}) |-> msgs[m2]]
    ELSE
        msgs

(* Add a message to the bag of messages.                                   *)
Send(m) == messages' = WithMessage(m, messages)

(* Remove a message from the bag of messages. Used when a server is done   *)
(* processing a message.                                                   *)
Discard(m) == messages' = WithoutMessage(m, messages)

(* Combination of Send and Discard                                         *)
Reply(response, request) ==
    messages' = WithoutMessage(request, WithMessage(response, messages))

(* Return the minimum value from a set, or undefined if the set is empty.  *)
Min(s) == CHOOSE x \in s : \A y \in s : x <= y
(* Return the maximum value from a set, or undefined if the set is empty.  *)
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

----

InitHistoryVars == /\ elections = {}
                   /\ allLogs   = {}
                   /\ voterLog  = [i \in Server |-> [j \in {} |-> <<>>]]
InitServerVars == /\ currentTerm = [i \in Server |-> 1]
                  /\ state       = [i \in Server |-> Follower]
                  /\ votedFor    = [i \in Server |-> Nil]
InitCandidateVars == /\ votesResponded = [i \in Server |-> {}]
                     /\ votesGranted   = [i \in Server |-> {}]
InitStateSpaceVars == /\ numClientRequests = 0
                      /\ numTimeouts = 0
                      /\ numRestarts = 0
(* The values nextIndex[i][i] and matchIndex[i][i] are never read, since   *)
(* the leader does not send itself messages. It's still easier to include  *)
(* these in the functions.                                                 *)
InitLeaderVars == /\ nextIndex  = [i \in Server |-> [j \in Server |-> 1]]
                  /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
InitLogVars == /\ log          = [i \in Server |-> << >>]
               /\ commitIndex  = [i \in Server |-> 0]
InitQuorumVars ==
    /\ dataQuorum = [i \in Server |-> {}]
    /\ dataQuorumTerm = [i \in Server |-> 1]
    /\ leaderElectionQuorum = [i \in Server |-> {}]
Init == /\ messages = [m \in {} |-> 0]
        /\ InitHistoryVars
        /\ InitServerVars
        /\ InitCandidateVars
        /\ InitLeaderVars
        /\ InitLogVars
        /\ InitStateSpaceVars
        /\ InitQuorumVars

----

(***************************************************************************)
(*************************** State transitions *****************************)
(***************************************************************************)

(* Server i restarts from stable storage.                                  *)
(* It loses everything but its currentTerm, votedFor, quorumVars and log.  *)
Restart(i) ==
    /\ state'          = [state EXCEPT ![i] = Follower]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ nextIndex'      = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ commitIndex'    = [commitIndex EXCEPT ![i] = 0]
    /\ numRestarts'    = numRestarts + 1
    /\ UNCHANGED <<messages, currentTerm, votedFor, log, elections,
                   numClientRequests, numTimeouts, quorumVars>>

(* Server i times out and starts a new election.                           *)
Timeout(i) ==
    /\ state[i] \in {Follower, Candidate}
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
    \* Most implementations would probably just set the local vote
    \* atomically, but messaging localhost for it is weaker.
    /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ numTimeouts'    = numTimeouts + 1
    /\ UNCHANGED <<messages, leaderVars, logVars, numClientRequests,
                   numRestarts, quorumVars>>

(* Candidate i sends j a RequestVote request.                              *)
RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ j \notin votesResponded[i]
    /\ Send([mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i],
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> Len(log[i]),
             msource       |-> i,
             mdest         |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* Leader i sends j an AppendEntries request containing up to 1 entry.     *)
(* While implementations may want to send more than 1 at a time, this spec *)
(* uses just 1 because it minimizes atomic regions WLOG.                   *)
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ LET prevLogIndex == nextIndex[i][j] - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           \* Send up to 1 entry, constrained by the end of the log.
           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
       IN Send([mtype                 |-> AppendEntriesRequest,
                mterm                 |-> currentTerm[i],
                mprevLogIndex         |-> prevLogIndex,
                mprevLogTerm          |-> prevLogTerm,
                mentries              |-> entries,
                \* mlog is used as a history variable for the proof.
                \* It would not exist in a real implementation.
                mlog                  |-> log[i],
                mcommitIndex          |-> Min({commitIndex[i],
                                               lastEntry}),
                mdataQuorum           |-> dataQuorum[i],
                mdataQuorumTerm       |-> dataQuorumTerm[i],
                mleaderElectionQuorum |-> leaderElectionQuorum[i],
                msource               |-> i,
                mdest                 |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* Candidate i transitions to leader.                                      *)
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ \/ /\ votesGranted[i] \in leaderElectionQuorum[i]
          /\ currentTerm[i] = dataQuorumTerm[i] + 1
       \/ Cardinality(votesGranted[i]) +
              DataQuorumSize > Cardinality(Server)
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    \* Some subsets of the pre-defined size with leader in them.
    \* It's not mandatory for the leader to be in the data quorum.
    \* Note that the spec does not iterate over all possibilites of data
    \* quorum here. Any non empty set satisfying the conditions will do.
    \* Setting the cardinality to greater than one to keep it more
    \* realistic and avoid any single point of failure. This condition is
    \* not necessary.
    /\ LET allSubsets == {
            x \in SUBSET(Server) :
            /\ i \in x
            /\ Cardinality(x) = DataQuorumSize}
            newDataQuorum == CHOOSE y \in SUBSET(allSubsets) :
                                 Cardinality(y) > 1
        IN /\ dataQuorum' = [dataQuorum EXCEPT ![i] = newDataQuorum]
           /\ leaderElectionQuorum' =
                  [leaderElectionQuorum EXCEPT ![i] =
                      LeaderElectionQuorum(newDataQuorum, i)]
    /\ dataQuorumTerm' = [dataQuorumTerm EXCEPT ![i] = currentTerm[i]]
    /\ elections'  = elections \cup
                         {[eterm     |-> currentTerm[i],
                           eleader   |-> i,
                           elog      |-> log[i],
                           evotes    |-> votesGranted[i],
                           evoterLog |-> voterLog[i]]}
    /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars,
                   stateSpaceVars>>

(* Leader i receives a client request to add v to the log.                 *)
ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ LET entry == [term  |-> currentTerm[i],
                     value |-> v]
           newLog == Append(log[i], entry)
       IN  log' = [log EXCEPT ![i] = newLog]
    /\ numClientRequests' = numClientRequests + 1
    /\ UNCHANGED <<messages, serverVars, candidateVars,
                   nextIndex, matchIndex, elections, commitIndex,
                   numRestarts, numTimeouts, quorumVars>>

(* Leader i advances its commitIndex.                                      *)
(* This is done as a separate step from handling AppendEntries responses,  *)
(* in part to minimize atomic regions, and in part so that leaders of      *)
(* single-server clusters are able to mark entries committed.              *)
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET \* The set of servers that agree up through index.
           Agree(index) == {i} \cup {k \in Server :
                                         matchIndex[i][k] >= index}
           \* The maximum indexes for which a data quorum agrees
           agreeIndexes == {index \in 1..Len(log[i]) :
                                Agree(index) \in dataQuorum[i]}
           \* New value for commitIndex'[i]
           newCommitIndex ==
              IF /\ agreeIndexes /= {}
                 /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
              THEN
                  Max(agreeIndexes)
              ELSE
                  commitIndex[i]
       IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log,
                   stateSpaceVars, quorumVars>>

----

(***************************************************************************)
(**************************** Message handlers *****************************)
(***************************************************************************)

(* Convention: i = recipient, j = sender, m = message                      *)

(* Server i receives a RequestVote request from server j with              *)
(* m.mterm <= currentTerm[i].                                              *)
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= Len(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 \* mlog is used just for the elections history variable
                 \* for the proof. It would not exist in a real
                 \* implementation.
                 mlog         |-> log[i],
                 msource      |-> i,
                 mdest        |-> j],
                 m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars,
                      logVars, stateSpaceVars, quorumVars>>

(* Server i receives a RequestVote response from server j with             *)
(* m.mterm = currentTerm[i].                                               *)
HandleRequestVoteResponse(i, j, m) ==
    \* This tallies votes even when the current state is not Candidate, but
    \* they won't be looked at, so it doesn't matter.
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
          /\ voterLog' = [voterLog EXCEPT ![i] =
                              voterLog[i] @@ (j :> m.mlog)]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterLog>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* Server i receives an AppendEntries request from server j with           *)
(* m.mterm <= currentTerm[i]. This just handles m.entries of length        *)
(* 0 or 1, but implementations could safely accept more by treating them   *)
(* the same as multiple independent requests of 1 entry.                   *)
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= Len(log[i])
                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ /\ \* reject request
                \/ m.mterm < currentTerm[i]
                \/ /\ m.mterm = currentTerm[i]
                   /\ state[i] = Follower
                   /\ \lnot logOk
             /\ Reply([mtype           |-> AppendEntriesResponse,
                       mterm           |-> currentTerm[i],
                       msuccess        |-> FALSE,
                       mmatchIndex     |-> 0,
                       msource         |-> i,
                       mdest           |-> j],
                       m)
             /\ UNCHANGED <<serverVars, logVars, quorumVars>>
          \/ \* return to follower state
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Candidate
             /\ state' = [state EXCEPT ![i] = Follower]
             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages,
                            quorumVars>>
          \/ \* accept request
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Follower
             /\ logOk
             /\ dataQuorum' = [dataQuorum EXCEPT ![i] = m.mdataQuorum]
             /\ dataQuorumTerm' =
                    [dataQuorumTerm EXCEPT ![i] = m.mdataQuorumTerm]
             /\ leaderElectionQuorum' =
                    [leaderElectionQuorum EXCEPT ![i] =
                        m.mleaderElectionQuorum]
             /\ LET index == m.mprevLogIndex + 1
                IN \/  \* already done with request
                       /\ \/ m.mentries = << >>
                          \/ /\ m.mentries /= << >>
                             /\ Len(log[i]) >= index
                             /\ log[i][index].term = m.mentries[1].term
                       \* This could make our commitIndex decrease (for
                       \* example if we process an old, duplicated request),
                       \* but that doesn't really affect anything.
                       /\ commitIndex' = [commitIndex EXCEPT ![i] =
                                              m.mcommitIndex]
                       /\ Reply([mtype           |-> AppendEntriesResponse,
                                 mterm           |-> currentTerm[i],
                                 msuccess        |-> TRUE,
                                 mmatchIndex     |-> m.mprevLogIndex +
                                                     Len(m.mentries),
                                 msource         |-> i,
                                 mdest           |-> j],
                                 m)
                       /\ UNCHANGED <<serverVars, log>>
                   \/ \* conflict: remove 1 entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) >= index
                       /\ log[i][index].term /= m.mentries[1].term
                       /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
                                          log[i][index2]]
                          IN log' = [log EXCEPT ![i] = new]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
                   \/ \* no conflict: append entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) = m.mprevLogIndex
                       /\ log' = [log EXCEPT ![i] =
                                      Append(log[i], m.mentries[1])]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
       /\ UNCHANGED <<candidateVars, leaderVars, numTimeouts,
                      stateSpaceVars>>

(* Server i receives an AppendEntries response from server j with          *)
(* m.mterm = currentTerm[i].                                               *)
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ \/ /\ m.msuccess \* successful
          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
       \/ /\ \lnot m.msuccess \* not successful
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                               Max({nextIndex[i][j] - 1, 1})]
          /\ UNCHANGED <<matchIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, elections,
                   stateSpaceVars, quorumVars>>

(* Any RPC with a newer term causes the recipient to advance its           *)
(* term first.                                                             *)
UpdateTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ currentTerm'    = [currentTerm EXCEPT ![i] = m.mterm]
    /\ state'          = [state       EXCEPT ![i] = Follower]
    /\ votedFor'       = [votedFor    EXCEPT ![i] = Nil]
       \* messages is unchanged so m can be processed further.
    /\ UNCHANGED <<messages, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* Responses with stale terms are ignored.                                 *)
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* Receive a message.                                                      *)
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \* Any RPC with a newer term causes the recipient to advance
       \* its term first. Responses with stale terms are ignored.
       \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)

(* End of message handlers.                                                *)

----

(***************************************************************************)
(**************************** Network caveats  *****************************)
(***************************************************************************)

(* The network duplicates a message.                                       *)
DuplicateMessage(m) ==
    /\ Send(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

(* The network drops a message.                                            *)
DropMessage(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   stateSpaceVars, quorumVars>>

----

(* Defines how the variables may transition.                               *)

ServerTimeout == \E i \in Server : Timeout(i)
RequestVoteStep == \E i,j \in Server : RequestVote(i, j)
BecomeLeaderStep == \E i \in Server : BecomeLeader(i)
ClientRequestStep == \E i \in Server, v \in Value : ClientRequest(i, v)
AdvanceCommitIndexStep == \E i \in Server : AdvanceCommitIndex(i)
AppendEntriesStep == \E i,j \in Server : AppendEntries(i, j)
ReceiveMessageStep == \E m \in DOMAIN messages : Receive(m)
UpdateAllLogs == allLogs' = allLogs \cup {log[i] : i \in Server}

Next == /\ \/ \E i \in Server : Restart(i)
           \/ ServerTimeout
           \/ RequestVoteStep
           \/ BecomeLeaderStep
           \/ ClientRequestStep
           \/ AdvanceCommitIndexStep
           \/ AppendEntriesStep
           \/ ReceiveMessageStep
           \/ \E m \in DOMAIN messages : DuplicateMessage(m)
           \/ \E m \in DOMAIN messages : DropMessage(m)
        \* History variable that tracks every log ever
        /\ UpdateAllLogs

----

(***************************************************************************)
(************************* Algorithmic Properties **************************)
(***************************************************************************)

(* For all servers, term increases monotonically.                          *)
MonotonicTermIncrease == \A s \in Server : currentTerm[s] <= currentTerm'[s]
MonotonicTermIncreaseConstraint == [][MonotonicTermIncrease]_vars

(* A leader's log grows monotonically.                                     *)
MonotonicLeaderLogIncrease ==
    \A e \in elections : currentTerm[e.eleader] = e.eterm =>
        \A index \in 1..Len(log[e.eleader]) :
            log'[e.eleader][index] = log[e.eleader][index]
MonotonicLeaderLogIncreaseConstraint == [][MonotonicLeaderLogIncrease]_vars

(* There is at most one leader per term.                                   *)
AtMostOneLeaderPerTerm ==
    \A i, j \in elections : i.eterm = j.eterm => i.eleader = j.eleader

(* Log Matching Property                                                   *)
(* A term and index uniquely identify a prefix of the raft log.            *)
TermAndIndexDeterminesLogPrefix ==
    \A l, m \in allLogs :
        \A lindex \in DOMAIN l : (
            /\ lindex \in DOMAIN m
            /\ l[lindex].term = m[lindex].term ) =>
                \A pindex \in 1..lindex : l[pindex] = m[pindex]

(* The term for any log entry cannot be bigger than the server's current   *)
(* term.                                                                   *)
CurrentTermIsMax ==
    \A s \in Server :
        \A i \in 1..Len(log[s]) :
            log[s][i].term <= currentTerm[s]

(* State machine safety property                                           *)
(* If an entry is deemed committed by any server, all future leaders shall *)
(* have that entry in their log.                                           *)
StateMachineSafety ==
    \A i \in Server:
        /\ commitIndex[i] <= Len(log[i])
        /\ \A index \in DOMAIN log[i]:
            (index <= commitIndex[i]) =>
                \A election \in elections:
                    (election.eterm > currentTerm[i]) =>
                        election.elog[index].term = log[i][index].term
----

(* The specification must start with the initial state and transition      *)
(* according to Next.                                                      *)
Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(RequestVoteStep /\ UpdateAllLogs)
    /\ WF_vars(ClientRequestStep /\ UpdateAllLogs)
    /\ SF_vars(BecomeLeaderStep /\ UpdateAllLogs)
    /\ SF_vars(AdvanceCommitIndexStep /\ UpdateAllLogs)
    /\ SF_vars(AppendEntriesStep /\ UpdateAllLogs)
    /\ SF_vars(ReceiveMessageStep /\ UpdateAllLogs)

=============================================================================
