//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it
//   thinks it is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

const (
	electionDelay           = 500
	electionDelayRandomizer = 100
	heartbeatDelay          = 100
	applyStateDelay         = 10
)

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Log entry struct
type Entry struct {
	Term    int
	Command interface{}
}

// Raft struct
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	// Persistent
	currentTerm int
	votedFor    int
	Log         []Entry

	// Volatile
	commitIndex int
	lastApplied int

	// Leader, reinitialized after each re-election
	nextIndex  []int
	matchIndex []int

	// Custom state
	isLeader    bool
	receivedMsg bool
	applyCh     chan ApplyCommand
}

// RequestVoteArgs: RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply: RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs: AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesReply: AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// GetState
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool

	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mux.Unlock()

	return me, term, isleader
}

// RequestVote: RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// already voted for a different candidate
	if rf.currentTerm == args.Term && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	lastIdx := len(rf.Log) - 1
	lastTerm := rf.Log[lastIdx].Term

	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx) {
		reply.VoteGranted = true
		rf.receivedMsg = true
	} else {
		reply.VoteGranted = false
	}

	rf.isLeader = false
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
}

// AppendEntries: AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// convert to follower
	if args.Term > rf.currentTerm {
		rf.isLeader = false
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.receivedMsg = true

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.Log) || (args.PrevLogIndex >= 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	reply.Success = true

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	start := args.PrevLogIndex + 1
	end := min(len(rf.Log), len(args.Entries)+start)

	i := start
	for ; i < end; i++ {
		if rf.Log[i].Term != args.Entries[i-start].Term {
			rf.Log = rf.Log[:i]
			break
		}
	}

	// 4. Append any new entries not already in the log
	for ; i < len(args.Entries)+start; i++ {
		rf.Log = append(rf.Log, args.Entries[i-start])
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	}
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	if !rf.isLeader {
		return -1, rf.currentTerm, false
	}

	rf.Log = append(rf.Log, Entry{rf.currentTerm, command})
	rf.matchIndex[rf.me] = len(rf.Log) - 1
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	// index, term, isLeader
	return rf.matchIndex[rf.me], rf.currentTerm, true
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// Do nothing
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		peers: peers,
		me:    me,

		// Persistent
		currentTerm: 0,
		votedFor:    -1,
		Log:         []Entry{{-1, 0}},

		// Volatile
		lastApplied: 0,
		commitIndex: 0,

		// Leader, reinitialized after each re-election
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		// Custom state
		isLeader:    false,
		receivedMsg: false,
		applyCh:     applyCh,
	}

	// spin off goroutines for elections, heartbeats and applying state
	go rf.electionScheduler()
	go rf.heartbeatScheduler()
	go rf.applyStateScheduler()

	return rf
}

// election scheduler (at randomized intervals)
func (rf *Raft) electionScheduler() {
	for {
		// randomize interval
		duration := electionDelay + rand.Intn(electionDelayRandomizer)
		time.Sleep(time.Duration(duration) * time.Millisecond)

		rf.mux.Lock()
		// start election if no message received and server is not leader
		if !rf.receivedMsg && !rf.isLeader {
			go rf.startElection(rf.currentTerm)
		}

		// reset whether we received message before sleeping
		rf.receivedMsg = false
		rf.mux.Unlock()
	}
}

// heartbeat scheduler (at fixed intervals)
func (rf *Raft) heartbeatScheduler() {
	for {
		time.Sleep(time.Duration(heartbeatDelay) * time.Millisecond)

		// send heartbeat (append entries RPC)
		rf.heartbeatSender()
	}
}

// scheduler to apply state (at fixed intervals)
func (rf *Raft) applyStateScheduler() {
	for {
		time.Sleep(time.Duration(applyStateDelay) * time.Millisecond)

		rf.mux.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			applyCmd := ApplyCommand{
				Index:   rf.lastApplied,
				Command: rf.Log[rf.lastApplied].Command,
			}
			rf.applyCh <- applyCmd
		}
		rf.mux.Unlock()
	}
}

// candidate starts election
func (rf *Raft) startElection(term int) {
	rf.mux.Lock()

	// sanity check that we are not behind
	if rf.currentTerm != term {
		rf.mux.Unlock()
		return
	}

	// update state
	rf.currentTerm++
	rf.votedFor = rf.me
	numVotes := 1
	term = rf.currentTerm
	rf.mux.Unlock()

	// send VoteRequest to all servers in parallel
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendVote(i, &numVotes, term)
	}
}

func (rf *Raft) sendVote(server int, numVotes *int, term int) {
	rf.mux.Lock()

	// sanity check that we are not behind
	if rf.currentTerm != term {
		rf.mux.Unlock()
		return
	}

	// create args struct
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	}
	rf.mux.Unlock()

	// send vote
	reply := RequestVoteReply{}
	rf.sendRequestVote(server, &args, &reply)

	rf.mux.Lock()
	// convert to follower
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.mux.Unlock()
		return
	}

	// resposnse received
	if rf.currentTerm != term {
		rf.mux.Unlock()
		return
	}

	// vote granted
	if reply.VoteGranted {
		*numVotes++

		// become leader (consensus reached)
		if *numVotes == 1+len(rf.peers)/2 {
			rf.isLeader = true

			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.Log)
				rf.matchIndex[i] = 0
			}

			// leader sends heartbeat to all servers
			rf.mux.Unlock()
			rf.heartbeatSender()
			return
		}
	}

	rf.mux.Unlock()
}

func (rf *Raft) heartbeatSender() {
	// sanity check that server is leader
	rf.mux.Lock()
	if !rf.isLeader {
		rf.mux.Unlock()
		return
	}
	rf.mux.Unlock()

	// send heartbeats to servers in parallel
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartbeat(i)
	}
}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mux.Lock()

	// sanity check that server is leader
	if !rf.isLeader {
		rf.mux.Unlock()
		return
	}

	// create args struct
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].Term,
		Entries:      append([]Entry(nil), rf.Log[rf.nextIndex[server]:]...),
		LeaderCommit: rf.commitIndex,
	}
	rf.mux.Unlock()

	// send heartbeat
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)

	rf.mux.Lock()
	// convert to follower
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.mux.Unlock()
		return
	}

	if !rf.isLeader {
		rf.mux.Unlock()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = max(len(args.Entries)+args.PrevLogIndex+1, rf.nextIndex[server])

		rf.matchIndex[server] = max(len(args.Entries)+args.PrevLogIndex, rf.matchIndex[server])

		indices := append([]int(nil), rf.matchIndex...)
		sort.Sort(sort.Reverse(sort.IntSlice(indices)))

		newCommitIndex := indices[len(rf.peers)/2]
		if newCommitIndex > rf.commitIndex && rf.Log[newCommitIndex].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
		}
	} else {
		diff := max(len(rf.Log)-1-rf.nextIndex[server], 1)
		rf.nextIndex[server] = max(len(rf.Log)-(2*diff), 1)
	}

	rf.mux.Unlock()
}
