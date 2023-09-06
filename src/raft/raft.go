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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	state     int                 // state for a single raft peer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// time parameters
	electionTimeout   int64     // election timeout for this peer
	heartbeatInterval int       // interval for sending heartbeat
	lastActiveTime    time.Time // used for recording election elapsed time and heartbeat receiving time
	lastBroadcastTime time.Time // used for sending heartbeat

	// persistent state
	currentTerm int         // latest term server has seen
	votedFor    *int        // candidateId that received vote in current term
	logEntries  []*LogEntry // log entries

	// volatile state
	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextInds  []int // index of the next log entry for all peers
	matchInds []int // index of highest log entry known to be replicated on all peers

	voteCount int // votes that received from other peers

	heartbeatChannel chan bool // used for leader notification
}

// Each entry contains command for state machine and term when entry was received by leader
type LogEntry struct {
	Command string
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
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
		rf.state = Follower
		rf.votedFor = nil
	}
	// whether votedFor is null or candidateId
	if (rf.votedFor == nil) || (*(rf.votedFor) == args.CandidateId) {
		// whether candidate log is up-to-date
		receiverLastLogIndex := 0
		receiverLastLogTerm := 0
		if len(rf.logEntries) >= 1 {
			receiverLastLogIndex = len(rf.logEntries) - 1
			receiverLastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		}
		// candidate term is larger than receiver term
		// candidate last log term is the same as receiver last log term
		// candidate last log index is the same as receiver last log index
		if (args.LastLogTerm > receiverLastLogTerm) ||
			(args.LastLogTerm == receiverLastLogTerm && args.LastLogIndex >= receiverLastLogIndex) ||
			(args.LastLogIndex == receiverLastLogIndex && args.LastLogTerm >= receiverLastLogTerm) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			DPrintf("[DEBUG] %v vote for %v", rf.me, args.CandidateId)
			return
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[DEBUG] Receive heartbeat from %v for %v", args.LeaderId, rf.me)
	DPrintf("[DEBUG] %v Convert to Follower", rf.me)
	rf.state = Follower
	// return false if leader term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// heartbeat
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = nil
	}
	DPrintf("[DEBUG] Update last active time for %v", rf.me)
	rf.lastActiveTime = time.Now()
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

func (rf *Raft) electionProcess() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		// should start a leader election
		if !rf.isLeader() && (time.Since(rf.lastActiveTime) > time.Duration(rf.electionTimeout)) {
			DPrintf("[DEBUG] Start a leader election for %v state %v", rf.me, rf.state)
			// update last active time
			rf.lastActiveTime = time.Now()
			// checkout raft state from Follower to Candidate
			rf.state = Candidiate
			// Increment currentTerm
			rf.currentTerm++
			// Vote for self
			rf.voteCount = 1
			rf.votedFor = &rf.me
			// Send RequestVote RPC to all other servers
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		rf.electionTimeout = 300 + (rand.Int63() % 300)
		// time.Sleep(time.Millisecond)
	}
}

func (rf *Raft) heartbeatProcess() {
	for !rf.killed() {
		// Only Leader need to send heartbeat to other peers
		if rf.isLeader() && (time.Since(rf.lastBroadcastTime) >= time.Duration(rf.heartbeatInterval)) {
			DPrintf("[DEBUG] Send heartbeat to other peers for %v", rf.me)
			rf.lastBroadcastTime = time.Now()
			rf.startHeartbeat()
		}
		// pause for a random amount of time between 100 and 200 milliseconds.
		// ms := 100 + (rand.Int63() % 100)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

// func (rf *Raft) checkElection() bool {
// 	if rf.heartbeatAccpeted {
// 		DPrintf("[DEBUG] Heartbeat is received within election timeout for %v", rf.me)
// 		// Reset election timer
// 		rf.heartbeatAccpeted = false
// 		return false
// 	} else {
// 		DPrintf("[DEBUG] Heartbeat is not received within election timeout for %v", rf.me)
// 		return true
// 	}
// }

func (rf *Raft) checkLeader() bool {
	majority := (len(rf.peers) + 1) / 2
	if rf.voteCount >= majority {
		return true
	} else {
		return false
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

func (rf *Raft) startHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				appendEntriesArgs := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				appendEntriesReply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
				if !ok {
					return
				}
				if appendEntriesReply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = appendEntriesArgs.Term
					rf.votedFor = nil
				}
			}(i)
		}
	}
}

type voteResult struct {
	id    int
	reply *RequestVoteReply
}

func (rf *Raft) startElection() {
	// send RequestVote RPC to other peers
	voteChannel := make(chan *voteResult, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				candidiateLastLogIndex := 0
				candidiateLastLogTerm := 0
				if len(rf.logEntries) >= 1 {
					candidiateLastLogIndex = len(rf.logEntries) - 1
					candidiateLastLogTerm = rf.logEntries[candidiateLastLogIndex].Term
				}

				requestVoteArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: candidiateLastLogIndex,
					LastLogTerm:  candidiateLastLogTerm,
				}
				requestVoteReply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
				if ok {
					voteChannel <- &voteResult{id: i, reply: &requestVoteReply}
				} else {
					voteChannel <- &voteResult{id: i, reply: nil}
				}
			}(i)
		}
	}

	maxTerm := 0
	finishCount := 0
	for res := range voteChannel {
		finishCount++
		if res.reply != nil {
			if res.reply.VoteGranted {
				rf.voteCount++
			}
			if res.reply.Term > maxTerm {
				maxTerm = res.reply.Term
			}
		}
		if finishCount == len(rf.peers) - 1 || rf.checkLeader() {
			DPrintf("[DBEUG] received vote count : %v id %v", rf.voteCount, rf.me)
			break
		}
	}
	// is not candidate, ignore
	if !rf.isCandidate() {
		DPrintf("[DEBUG] Not Candidiate")
		rf.voteCount = 0
		rf.votedFor = nil
		return
	}
	// term is out of date
	if maxTerm > rf.currentTerm {
		DPrintf("[DEBUG] Term is out of date for %v", rf.me)
		rf.state = Follower
		rf.currentTerm = maxTerm
		rf.votedFor = nil
		rf.voteCount = 0
		return
	}
	// Become leader
	if rf.checkLeader() {
		DPrintf("[DEBUG] Become leader for %v", rf.me)
		rf.state = Leader
		rf.votedFor = nil
		rf.lastBroadcastTime = time.Now()
		rf.voteCount = 0
		return
	} else {
		DPrintf("[DEBUG] Can not become leader for %v", rf.me)
		rf.state = Follower
		rf.votedFor = nil
		rf.voteCount = 0
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
	rf.logEntries = make([]*LogEntry, 0)
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.electionTimeout = 300 + (rand.Int63() % 300) // election timeout from 300ms to 600ms
	rf.heartbeatInterval = 100                      // heartbeat interval is 100ms
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0
	rf.lastActiveTime = time.Now()
	rf.lastBroadcastTime = time.Now()
	DPrintf("[DEBUG] Create a single peer : %v", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionProcess()

	// start ticker goroutine to send heartbeat as a leader
	go rf.heartbeatProcess()

	return rf
}
