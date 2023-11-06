package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// Single Raft Peer State
const (
	Follower = iota
	Candidiate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile state about election
	state             int       // state for a single raft peer
	leaderId          int       // id for the leader
	electionTimeout   int64     // election timeout for this peer
	heartbeatInterval int       // interval for sending heartbeat
	lastActiveTime    time.Time // used for recording election elapsed time and heartbeat receiving time
	lastBroadcastTime time.Time // used for sending heartbeat

	// persistent state
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logEntries  []LogEntry // log entries

	// volatile state
	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextInds  []int // index of the next log entry for all peers
	matchInds []int // index of highest log entry known to be replicated on all peers

	// channel for applying committed log entry
	applyCh chan ApplyMsg // used for exchanging committed log entry
}

// Each entry contains command for state machine and term when entry was received by leader
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.isLeader()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.logEntries)
	states := buffer.Bytes()
	//DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]", rf.me, rf.currentTerm, rf.votedFor, rf.logEntries)
	rf.persister.Save(states, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.logEntries)
	//DPrintf("RaftNode[%d] persist restores, currentTerm[%d] voteFor[%d] log[%v]", rf.me, rf.currentTerm, rf.votedFor, rf.logEntries)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update last active time
	rf.lastActiveTime = time.Now()
	// init reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// return false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// if term > current, convert to Follower and update the term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// whether votedFor is -1 or candidateId
	if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
		// whether candidate log is up-to-date
		lastLogIndex := 1
		lastLogTerm := 0
		if len(rf.logEntries) >= 1 {
			lastLogIndex = len(rf.logEntries)
			lastLogTerm = rf.logEntries[lastLogIndex-1].Term
		}
		// candidate term is larger than receiver term
		// candidate last log term is the same as receiver last log term
		// candidate last log index is the same as receiver last log index
		if (args.LastLogTerm > lastLogTerm) ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}
	rf.persist()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%d] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] argsLogLen[%d]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.state, len(rf.logEntries), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, rf.logEntries, len(args.Entries))
	}()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	// return false if leader term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// higher term is found
	if args.Term > rf.currentTerm {
		rf.toFollower()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
	}
	// Update leader Id and Active time
	rf.lastActiveTime = time.Now()
	rf.leaderId = args.LeaderId
	// return false if follower's log doesn't contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	if len(rf.logEntries) < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.logEntries)
		return
	}
	if args.PrevLogIndex > 0 && rf.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logEntries[args.PrevLogIndex-1].Term
		for index := 1; index <= args.PrevLogIndex; index++ {
			if rf.logEntries[index-1].Term == rf.logEntries[args.PrevLogIndex-1].Term {
				reply.ConflictIndex = index
				break
			}
		}
		return
	}
	// log replication
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, logEntry)
		} else {
			if rf.logEntries[index-1].Term != logEntry.Term {
				rf.logEntries = rf.logEntries[:index-1]
				rf.logEntries = append(rf.logEntries, logEntry)
			}
		}
	}
	rf.persist()
	// Update follower commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > len(rf.logEntries) {
			rf.commitIndex = len(rf.logEntries)
		}
	}
	// Return
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check whether the peer is leader
	if !rf.isLeader() {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	index = len(rf.logEntries)
	term = rf.currentTerm
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		// Your code here (2A)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Check whether election is needed
			timeout := time.Duration(rf.electionTimeout) * time.Millisecond
			elapses := time.Since(rf.lastActiveTime)
			// Follower ->  Candidate
			if rf.isFollower() {
				if elapses > timeout {
					rf.toCandidate()
				}
			}
			if rf.isCandidate() && elapses > timeout {
				rf.lastActiveTime = time.Now()
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()
				// Prepare for RequestVote RPC
				lastLogIndex := 1
				lastLogTerm := 0
				if len(rf.logEntries) >= 1 {
					lastLogIndex = len(rf.logEntries)
					lastLogTerm = rf.logEntries[lastLogIndex-1].Term
				}
				requestVoteArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.mu.Unlock()
				// Send RequestVote RPC in Parallel
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						requestVoteReply := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &requestVoteArgs, &requestVoteReply); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &requestVoteReply}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}
				// Calculate vote count received
				voteCount := 1
				finishCount := 1
				maxTerm := 0
				for voteResult := range voteResultChan {
					finishCount += 1
					if voteResult.resp != nil {
						if voteResult.resp.VoteGranted {
							voteCount += 1
						}
						if voteResult.resp.Term > maxTerm {
							maxTerm = voteResult.resp.Term
						}
					}
					if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
						goto VOTE_END
					}
				}
			VOTE_END:
				rf.mu.Lock()
				// state change, ignore
				if !rf.isCandidate() {
					return
				}
				// higher term is found, convert to Follower
				if maxTerm > rf.currentTerm {
					rf.toFollower()
					rf.votedFor = -1
					rf.currentTerm = maxTerm
					rf.leaderId = -1
					rf.persist()
					return
				}
				// Become Leader
				if voteCount > len(rf.peers)/2 {
					rf.toLeader()
					rf.leaderId = rf.me
					rf.nextInds = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextInds[i] = len(rf.logEntries) + 1
					}
					rf.matchInds = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchInds[i] = 0
					}
					rf.lastBroadcastTime = time.Now()
					return
				}
			}
		}()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		rf.electionTimeout = 300 + (rand.Int63() % 300)
	}
}

func (rf *Raft) isFollower() bool {
	return rf.state == Follower
}

func (rf *Raft) isCandidate() bool {
	return rf.state == Candidiate
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) toFollower() {
	rf.state = Follower
}

func (rf *Raft) toCandidate() {
	rf.state = Candidiate
}

func (rf *Raft) toLeader() {
	rf.state = Leader
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Ensure the peer is leader
			if !rf.isLeader() {
				return
			}
			// Update broadcast time
			if time.Since(rf.lastBroadcastTime) < time.Duration(rf.heartbeatInterval)*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()
			// Send appendEntries RPC in parallel
			type AppendResult struct {
				peerId int
				resp   *AppendEntriesReply
			}
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      make([]LogEntry, 0),
					PrevLogIndex: rf.nextInds[peerId] - 1,
				}
				if appendEntriesArgs.PrevLogIndex > 0 {
					appendEntriesArgs.PrevLogTerm = rf.logEntries[appendEntriesArgs.PrevLogIndex-1].Term
				}
				appendEntriesArgs.Entries = append(appendEntriesArgs.Entries, rf.logEntries[rf.nextInds[peerId]-1:]...)
				go func(id int, aeArgs *AppendEntriesArgs) {
					appendEntriesReply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, aeArgs, &appendEntriesReply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer func() {
							DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d] argsLogLen[%d]",
								rf.me, rf.currentTerm, id, len(rf.logEntries), rf.nextInds[id], rf.matchInds[id], rf.commitIndex, len(aeArgs.Entries))
						}()
						// term change
						if rf.currentTerm != aeArgs.Term {
							return
						}
						// higher term is found
						if appendEntriesReply.Term > rf.currentTerm {
							rf.toFollower()
							rf.votedFor = -1
							rf.leaderId = -1
							rf.currentTerm = appendEntriesReply.Term
							rf.persist()
							return
						}
						// log replication success
						if appendEntriesReply.Success {
							rf.nextInds[id] = aeArgs.PrevLogIndex + len(aeArgs.Entries) + 1
							rf.matchInds[id] = rf.nextInds[id] - 1
							// Update commit index
							sortedMatchIndex := make([]int, 0)
							sortedMatchIndex = append(sortedMatchIndex, len(rf.logEntries))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								sortedMatchIndex = append(sortedMatchIndex, rf.matchInds[i])
							}
							sort.Ints(sortedMatchIndex)
							newCommitIndex := sortedMatchIndex[len(sortedMatchIndex)/2]
							if newCommitIndex > rf.commitIndex && rf.logEntries[newCommitIndex-1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
						} else {
							if appendEntriesReply.ConflictTerm != -1 {
								conflictIndex := -1
								for index := appendEntriesArgs.PrevLogIndex; index >= 1; index-- {
									if rf.logEntries[index-1].Term == appendEntriesReply.Term {
										conflictIndex = index
										break
									}
								}
								if conflictIndex != -1 {
									rf.nextInds[id] = conflictIndex + 1
								} else {
									rf.nextInds[id] = appendEntriesReply.ConflictIndex
								}
							} else {
								rf.nextInds[id] = appendEntriesReply.ConflictIndex + 1
							}
						}
					}
				}(peerId, &appendEntriesArgs)
			}
		}()
	}
}

func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyCh <- appliedMsg
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.leaderId = -1
	rf.electionTimeout = 300 + (rand.Int63() % 300) // election timeout from 300ms to 600ms
	rf.heartbeatInterval = 100                      // heartbeat interval is 100ms
	rf.lastActiveTime = time.Now()
	rf.lastBroadcastTime = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to send requestVote RPC
	go rf.electionLoop()

	// start ticker goroutine to send appendEntries RPC
	go rf.appendEntriesLoop()

	// start ticker goroutine to apply committed log entry
	go rf.applyLogLoop()

	return rf
}
