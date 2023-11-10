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
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

const electionDelay = 300
const electionDelayRandomizer = 10
const heartbeatDelay = 20

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

// Raft struct
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	// Persistent
	currentTerm int
	votedFor    int

	// Custom state
	isLeader    bool
	receivedMsg bool
}

// RequestVoteArgs: RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// RequestVoteReply: RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs: AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
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

	// handle same term
	if rf.currentTerm == args.Term {
		if rf.votedFor == args.CandidateId {
			rf.isLeader = false
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			rf.receivedMsg = true
		} else {
			reply.VoteGranted = false
		}
		return
	}

	// convert to follower
	if args.Term > rf.currentTerm {
		rf.isLeader = false
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		rf.receivedMsg = true
	}
}

// AppendEntries: AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm
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

	reply.Success = true
	rf.receivedMsg = true
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
	index := -1
	term := -1
	isLeader := true

	// TODO - Your code here (2B)

	return index, term, isLeader
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
	// TODO - Your code here, if desired
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

		currentTerm: 0,
		votedFor:    -1,

		isLeader:    false,
		receivedMsg: false,
	}

	go rf.electionScheduler()
	go rf.heartbeatScheduler()

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
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
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
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
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

	rf.mux.Unlock()
}
