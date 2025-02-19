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
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg		  // applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
	applyCond *sync.Cond		  // applyCond is to control when to apply the log entry to the state machine.

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/**
	 *  1. Persistent state on all servers:
	 *  (Updated on stable storage before responding to RPCs)
	 */

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidate Id that received vote incurrent term (or null if none)
	votedFor interface{}

	// log entries; each entry contains command for state machine, 
	// and term when entry was received by leader (first index is 1!!!)
	log []LogEntry

	/**
	 *  2. Volatile state on all servers:
	 */
	
	// last timer recorded
	lastTimer time.Time

	// "leader" / "candidate" / "follower"
	role string

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	/**
	 *  3. Volatile state on leaders:
	 *  (Reinitialized after election)
	 */
	
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex[] int

	// AppendEntriesRPCMu[] sync.Mutex

	// RequestVoteRPCMu[] sync.Mutex

	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex[] int
}

type LogEntry struct {
	Command      interface{}
	Term         int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock();
	term = rf.currentTerm;
	isleader = false
	if rf.role == "leader" {
		isleader = true
	}
	rf.mu.Unlock();

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Failed to decode persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
// 
type AppendEntriesArgs struct {
	// leader's term
	Term int

	// so follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogIndex int

	// term of prevLogIndex entry.
	PrevLogTerm int

	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []LogEntry

	// leader's commitIndex
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself.
	Term int

	// true if follower contained entry matching prevLogIndex and prevLogTerm.
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Printf("AppendEntries log - [%v]: ", rf.me)
	for j := 0; j < len(rf.log); j++ {
		fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
	}
	fmt.Printf("\n")

	reply.Term = rf.currentTerm
	
	// Reply false if term < currentTerm.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.lastTimer = time.Now()

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = "follower"
	}

	// If the AppendEntries RPC is a heartbeat, return true when the follower's log is up-to-date.
	if args.Entries == nil {
		reply.Success = true

		// lastLogIndex := len(rf.log) - 1
		// lastLogTerm := rf.log[lastLogIndex].Term

		// leader: 0.null-1, 1.xxx.5
		// follower: 0.null-1, 1.yyy-2, 2.zzz-2
		// heartbeat return false
		// if args.LeaderCommit > 0 && args.LeaderCommit > lastLogIndex {
		// if args.LeaderCommit > 0 && (args.LeaderCommit > lastLogIndex || args.LeaderCommit <= lastLogIndex && args.Term > lastLogTerm) {
		// 	reply.Success = false

		// 	fmt.Printf("inconsistent log - [%v]: ", rf.me)
		// 	for j := 0; j < len(rf.log); j++ {
		// 		fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
		// 	}
		// 	fmt.Printf("\n")
		// }

		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false

			fmt.Printf("inconsistent log - [%v]: ", rf.me)
			for j := 0; j < len(rf.log); j++ {
				fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
			}
			fmt.Printf("\n")
		} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Success = false

			fmt.Printf("inconsistent log - [%v]: ", rf.me)
			for j := 0; j < len(rf.log); j++ {
				fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
			}
			fmt.Printf("\n")

			// Committed log entries cannot be deleted.
			if rf.commitIndex + 1 > args.PrevLogIndex {
				panic("Leader try to delete committed log entries")
			}

			// ATTENTION: If an existing entry conflicts with a new one (same index but different terms), 
			// delete the existing entry and all that follow it!!!
			rf.log = rf.log[:args.PrevLogIndex]
		}

		// Use heartbeat to update commitIndex.
		// It's nessecary to check lastLogTerm == args.Term when modify commitIndex,
		// becuse the follower whose log is wrong may store the wrong log entry.

		lastLogIndex := len(rf.log) - 1

		// if args.LeaderCommit > rf.commitIndex && lastLogTerm == args.Term {
		if args.LeaderCommit > rf.commitIndex {
			if lastLogIndex > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastLogIndex
			}
			rf.applyCond.Signal()
			fmt.Printf("heartbeat: {me: %d   updated commitIndex: %d}\n", rf.me, rf.commitIndex)
		}

		return
	}

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	// It's nessary to check args.PrevLogIndex < len(rf.log) to avoid array out of range.
	if args.PrevLogIndex >= len(rf.log) || 
	   args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		return
	} 
	
	// Committed log entries cannot be overwritten.
	if rf.commitIndex > args.PrevLogIndex {
		panic("Leader try to cover committed log entries")
	}
	
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
	rf.log = rf.log[:args.PrevLogIndex + 1]

	// Append any new entries not already in the log.
	rf.log = append(rf.log, args.Entries...)
	
	// Commit message will be implicit in the next AppendEntries RPC.
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastLogIndex := len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		if lastLogIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
		rf.applyCond.Signal()
		fmt.Printf("AppendEntries: {me: %d   updated commitIndex: %d   lastLogIndex: %d   args.LeaderCommit: %d}\n", rf.me, rf.commitIndex, lastLogIndex, args.LeaderCommit)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// candidate's term.
	Term int

	// candidate requesting vote.
	CandidateId int

	// index of candidate's last log entry.
	LastLogIndex int

	// term of candidate's last log entry.
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself.
	Term int

	// true means candidate received vote.
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// If RPC request contains term T > currentTerm (term has expired): set currentTerm = T, convert to follower.
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = "follower"
	}

	// If votedFor is null or candidateId, and candidate's log is at
 	// least as up-to-date as receiver's log, grant vote.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if args.LastLogTerm > lastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastTimer = time.Now()
		return
	}

	if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastTimer = time.Now()
		return
	}

	reply.VoteGranted = false
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	// if the server is not leader, return false immediately.
	if rf.role != "leader" {
		isLeader = false
		return index, term, isLeader
	}

	// ATTENTION: "index = rf.commitIndex + 1" is wrong, because
	// the new leader must continue to send log entries from the old leader in the new period.
	// The old log entries may have been committed and applied by the old leader.
	index = len(rf.log) 
	isLeader = true

	// Leader's own job.
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm})

	// Try to reach a consensus
	go rf.reachAgreement(index)

	rf.persist()
	return index, term, isLeader
}

// The leader receives a command from client, 
// and tries to reach a consensus among the other servers after it's own job.
func (rf *Raft) reachAgreement(index int) {
	rf.mu.Lock()

	if rf.role != "leader" {
		rf.mu.Unlock()
		return
	}

	// The number of votes received

	currentTerm := rf.currentTerm
	me := rf.me
	peers_num := len(rf.peers)

	// Leader's commitIndex
	leaderCommit := rf.commitIndex

	fmt.Printf("reachAgreement log - [%v]: ", rf.me)
	for j := 0; j < len(rf.log); j++ {
		fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
	}
	fmt.Printf("\n")

	rf.mu.Unlock()

	// Send AppendEntries RPCs in parallel.
	for i := 0; i < peers_num; i++ {
		if i == me {
			continue
		}

		// Each goroutine sends one RPC.
		go func(server int) {
			// rf.AppendEntriesRPCMu[server].Lock()
			// defer rf.AppendEntriesRPCMu[server].Unlock()

			rf.mu.Lock()

			if rf.role != "leader" {
				rf.mu.Unlock()
				return
			}

			nextIndex := rf.nextIndex[server]
			log := rf.log
			logLen := len(log)
			for {
				// index of log entry immediately preceding new ones
				prevLogIndex := nextIndex - 1

				if prevLogIndex >= logLen {
					panic("prevLogIndex: " + strconv.Itoa(prevLogIndex) + "; len of log: " + strconv.Itoa(logLen))
				}

				// term of prevLogIndex entry.
				prevLogTerm := log[prevLogIndex].Term

				// log entries to store (empty for heartbeat; may send more than one for efficiency)
				entries := log[nextIndex:]

				fmt.Printf("%d ---sendAppendEntries---> %d, %d.term=%d, len of log=%d, index=%d, cmd=%v, leaderCommit=%d, prevLogIndex=%d\n", me, server, me, currentTerm, logLen, index, log[index], leaderCommit, prevLogIndex)

				rf.mu.Unlock()

				args := &AppendEntriesArgs{currentTerm, me, prevLogIndex, prevLogTerm, entries, leaderCommit}
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, args, reply)

				rf.mu.Lock()

				if rf.role != "leader" {
					rf.mu.Unlock()
					return
				}

				fmt.Printf("%d ---sendAppendEntries OK---> %d, %d.term=%d, %d.term=%d, success=%t, len of log=%d, index=%d, cmd=%v, leaderCommit=%d, prevLogIndex=%d\n", me, server, me, currentTerm, server, reply.Term, reply.Success, logLen, index, log[index], leaderCommit, prevLogIndex)

				if !ok {
					rf.mu.Unlock()
					return
				}
				
				// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower.

				if currentTerm != rf.currentTerm {
					fmt.Printf("currentTerm: %d, rf.currentTerm: %d\n", currentTerm, rf.currentTerm)
					panic("reachAgreement: currentTerm != rf.currentTerm")
				}
				
				if reply.Term > currentTerm {
					rf.currentTerm = reply.Term 
					rf.role = "follower"

					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					if index + 1 > rf.nextIndex[server] {
						rf.nextIndex[server] = index + 1
					}
					if index > rf.matchIndex[server] {
						rf.matchIndex[server] = index
					}

					// fmt.Printf("matchIndex: ")
					// for i := 0; i < peers_num; i++ {
					// 	fmt.Printf("%d ", rf.matchIndex[i])
					// }
					// fmt.Println("")

					rf.mu.Unlock()
					return
				}

				// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
				nextIndex--

				if nextIndex == 0 {
					panic("nextIndex can't be zero")
				}
			}
		}(i)
	}
}

func (rf *Raft) applyToStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()

		// Wait for commitIndex's change.
		rf.applyCond.Wait()

		// for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log) - 1 {
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied, false, nil, -1, -1}
			rf.applyCh <- applyMsg
			fmt.Printf("me: %d   applyMsg: %v\n", rf.me, applyMsg)
		}

		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// message from the leader or another candidate recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		// Check if the heartbeat has timed out. (1500 ms)
		if rf.role != "leader" && time.Since(rf.lastTimer) > 1500 * time.Millisecond {
			rf.role = "candidate"
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		for rf.role == "candidate" {
			if rf.killed() {
				rf.mu.Unlock()
				return
			}

			// Enter startElection() with Lock.
			rf.startElection()
			// Exit startElection() with Lock.

			rf.mu.Unlock()
			// Sleep for a random time. (100 ms - 300 ms)
			time.Sleep(time.Duration(rand.Intn(200) + 100) * time.Millisecond)
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		// Sleep for a random time. (100 ms - 300 ms)
		time.Sleep(time.Duration(rand.Intn(200) + 100) * time.Millisecond)
	}
}

// Start an eleciton.
func (rf *Raft) startElection() {
	// Enter startElection() with Lock.
	
	// Increment current term.
	rf.currentTerm++

	currentTerm := rf.currentTerm
	me := rf.me
	peers_num := len(rf.peers)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// Vote for itself.
	rf.votedFor = me

	// Reset timeout itself.
	rf.lastTimer = time.Now()

	// The number of votes received
	vote := 1
	var vote_mu sync.Mutex

	// Wait Group
	var wg sync.WaitGroup

	rf.persist()
	rf.mu.Unlock()

	// Send RequestVote RPCs in parallel.
	for i := 0; i < peers_num; i++ {
		// Already vote for itself.
		if i == me {
			continue
		}

		wg.Add(1)
		
		// Each goroutine sends one RPC.
		go func(server int) {
			// rf.RequestVoteRPCMu[server].Lock()
			// defer rf.RequestVoteRPCMu[server].Unlock()
			defer wg.Done()

			args := &RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
			reply := &RequestVoteReply{}
			
			// If we send RequestVote RPC with rf.mu lock, there will be a deadlock.
			// Call() in sendRequestVote is guaranteed to return, but may not return soon.
			ok := rf.sendRequestVote(server, args, reply)

			// Check if the server is still the candidate. 
			// The candidate may notice there is a term bigger than its own through RPC, and changes to follower.
			rf.mu.Lock()

			if rf.role != "candidate" {
				rf.mu.Unlock()
				return
			}

			if !ok {
				rf.mu.Unlock()
				return
			}

			if currentTerm != rf.currentTerm {
				fmt.Printf("currentTerm: %d, rf.currentTerm: %d\n", currentTerm, rf.currentTerm)
				panic("startElection: currentTerm != rf.currentTerm")
			}

			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower.
			if reply.Term > currentTerm {
				rf.currentTerm = reply.Term 
				rf.role = "follower"

				rf.persist()
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				vote_mu.Lock()
				vote++
				vote_mu.Unlock()
			} 

			vote_mu.Lock()
			// If the candidate gets enough votes, 
			// then converts to leader immediately!!!
			if 2 * vote >= peers_num {
				vote_mu.Unlock()
				// Successfully elected as the leader.
				rf.role = "leader"

				fmt.Println("Elected ", rf.me, " as leader at term", rf.currentTerm)

				// Initialize the leader's data.
				for i := range rf.nextIndex {
					// rf.nextIndex[i] = rf.commitIndex + 1
					rf.nextIndex[i] = len(rf.log)
				}
				for i := range rf.matchIndex {
					rf.matchIndex[i] = 0
				}
				rf.commitIndex = len(rf.log) - 1

				// the leader sends heartbeats.
				go rf.sendHeartbeat()

				// the leader's background job to increase commitIndex when matchIndexes is enough.
				go rf.increCommitIndex()

				rf.mu.Unlock()
				return
			}
			vote_mu.Unlock()

			rf.mu.Unlock()
		}(i)
	}

	// Wait all rpc return
	wg.Wait()

	// Exit startElection() with Lock.
	rf.mu.Lock()
}


// Once elected as leader, the server starts to send heartbeats every 100ms.
func (rf *Raft) sendHeartbeat() {
	// Enter sendHeartbeat() with lock.
	rf.mu.Lock()
	if rf.role != "leader" {
		rf.mu.Unlock()
		return
	}

	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	me := rf.me
	peers_num := len(rf.peers)

	// If leader hasn't been killed, 
	// and the reply term isn't bigger than current term.
	for !rf.killed() && rf.role == "leader" {
		// Reset timeout itself.
		rf.lastTimer = time.Now()

		// leader's commitIndex
		leaderCommit := rf.commitIndex

		rf.mu.Unlock()

		for i := 0; i < peers_num; i++ {
			if i == me {
				continue
			}

			// Each goroutine sends one RPC.
			go func(server int) {
				// rf.AppendEntriesRPCMu[server].Lock()
				// defer rf.AppendEntriesRPCMu[server].Unlock()

				rf.mu.Lock()
				// index of log entry immediately preceding new ones
				prevLogIndex := rf.nextIndex[server] - 1

				// term of prevLogIndex entry.
				prevLogTerm := rf.log[prevLogIndex].Term

				args := &AppendEntriesArgs{currentTerm, me, prevLogIndex, prevLogTerm, nil, leaderCommit}
				reply := &AppendEntriesReply{}

				// Check if the server is still the leader. 
				// The leader may notice there is a term bigger than its own through RPC, and changes to follower.
				if rf.role != "leader" {
					rf.mu.Unlock()
					return
				}

				logLen := len(rf.log)
				fmt.Printf("%d ---sendHeartbeat---> %d, %d.term=%d, len of log=%d, leaderCommit=%d\n", me, server, me, currentTerm, logLen, leaderCommit)
				
				rf.mu.Unlock()
				
				// Send heartbeat
				// If we send AppendEntries RPC with rf.mu lock, there will be a deadlock.
				// Call() in sendAppendEntries is guaranteed to return, but may not return soon.
				// So the server becomes leader once it gets enough votes.
				ok := rf.sendAppendEntries(server, args, reply)

				// Check if the server is still the leader. 
				// The leader may notice there is a term bigger than its own through RPC, and changes to follower.
				rf.mu.Lock()

				if rf.role != "leader" {
					rf.mu.Unlock()
					return
				}

				if !ok {
					rf.mu.Unlock()
					return
				}

				fmt.Printf("%d ---sendHeartbeat OK---> %d, %d.term=%d, %d.term=%d, success=%t, len of log=%d, leaderCommit=%d\n", me, server, me, currentTerm, server, reply.Term, reply.Success, logLen, leaderCommit)

				if currentTerm != rf.currentTerm {
					fmt.Printf("currentTerm: %d, rf.currentTerm: %d\n", currentTerm, rf.currentTerm)
					panic("sendHeartbeat: currentTerm != rf.currentTerm")
				}
				
				// If a leader discovers that its term number has expired, 
				// it immediately reverts to follower.
				if reply.Term > currentTerm {
					rf.currentTerm = reply.Term 
					rf.role = "follower"

					rf.persist()
					rf.mu.Unlock()
					return
				}

				// If heartbeat reply is false, the log of the follower is inconsistent with the leader.
				// So make the follower's log consistent with the leader's log.
				if reply.Success {
					rf.mu.Unlock()
					return
				}

				fmt.Printf("log - [%v]: ", rf.me)
				for j := 0; j < len(rf.log); j++ {
					fmt.Printf("%d.%v-%v ", j, rf.log[j].Command, rf.log[j].Term)
				}
				fmt.Printf("\n")

				nextIndex := rf.nextIndex[server]
				log := rf.log
				for {
					// index of log entry immediately preceding new ones
					prevLogIndex := nextIndex - 1
	
					// term of prevLogIndex entry.
					prevLogTerm := log[prevLogIndex].Term
	
					// log entries to store (empty for heartbeat; may send more than one for efficiency)
					entries := log[nextIndex:]
	
					rf.mu.Unlock()
	
					args := &AppendEntriesArgs{currentTerm, me, prevLogIndex, prevLogTerm, entries, leaderCommit}
					reply := &AppendEntriesReply{}
	
					ok := rf.sendAppendEntries(server, args, reply)
	
					rf.mu.Lock()

					if rf.role != "leader" {
						rf.mu.Unlock()
						return
					}

					fmt.Printf("%d ---sendAppendEntries after heartbeat OK---> %d, %d.term=%d, %d.term=%d, success=%t, len of log=%d, len of entries=%d\n", me, server, me, currentTerm, server, reply.Term, reply.Success, len(log), len(entries))
					fmt.Printf("server: %d, nextIndex[server]: %d\n", server, nextIndex)
					if !ok {
						rf.mu.Unlock()
						return
					}

					if currentTerm != rf.currentTerm {
						fmt.Printf("currentTerm: %d, rf.currentTerm: %d\n", currentTerm, rf.currentTerm)
						panic("sendHeartbeat: currentTerm != rf.currentTerm")
					}
					
					// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower.
					if reply.Term > currentTerm {
						rf.currentTerm = reply.Term 
						rf.role = "follower"

						rf.persist()
						rf.mu.Unlock()
						return
					}

					fmt.Printf("server: %d, nextIndex[server]: %d\n", server, nextIndex)
	
					if reply.Success {
						if leaderCommit + 1 > rf.nextIndex[server] {
							rf.nextIndex[server] = leaderCommit + 1
						}
						if leaderCommit > rf.matchIndex[server] {
							rf.matchIndex[server] = leaderCommit
						}

						// fmt.Printf("matchIndex: ")
						// for i := 0; i < peers_num; i++ {
						// 	fmt.Printf("%d ", rf.matchIndex[i])
						// }
						// fmt.Println("")

						rf.mu.Unlock()
						return
					}

					fmt.Printf("server: %d, nextIndex[server]: %d\n", server, nextIndex)
					
					// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
					nextIndex--

					fmt.Printf("server: %d, nextIndex[server]: %d\n", server, nextIndex)
	
					if nextIndex == 0 {
						panic("nextIndex can't be zero")
					}
				}
			}(i)
		}

		time.Sleep(150 * time.Millisecond)
		rf.mu.Lock()
	}

	// leader convert to follower
	rf.role = "follower"
}

func (rf *Raft) increCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	peers_num := len(rf.peers)
	me := rf.me

	for !rf.killed() && rf.role == "leader" {
		commitIndex := rf.commitIndex

		fmt.Printf("matchIndex: ")
		for i := 0; i < peers_num; i++ {
			fmt.Printf("%d ", rf.matchIndex[i])
		}
		fmt.Println("")

		// to find the min value of matchIndex except mine (always 0)
		var minMatchIndex int
		for i := 0; i < peers_num; i++ {
			if i == me {
				continue
			}

			if (i == 0 && me != 0) || (i == 1 && me == 0) {
				minMatchIndex = rf.matchIndex[i]
			}

			if rf.matchIndex[i] < minMatchIndex {
				minMatchIndex = rf.matchIndex[i]
			}
		}

		fmt.Println("minMatchIndex: ", minMatchIndex)

		N := minMatchIndex
		for N < len(rf.log) {
			fmt.Println("N: ", N)
			num := 1
			for i := 0; i < peers_num; i++ {
				if i == me {
					continue
				}
				
				// ATTENTION: A leader is not allowed to update commitIndex to somewhere in a previous term (or, for that matter, a future term). 
				// Thus, as the rule says, you specifically need to check that log[N].term == currentTerm. 
				// This is because Raft leaders cannot be sure an entry is actually committed (and will not ever be changed in the future) if it’s not from their current term. 
				// This is illustrated by Figure 8 in the raft paper.
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					num++
				}
			}

			if 2 * num >= peers_num && N > commitIndex {
				commitIndex = N
			}

			N++
		}

		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.applyCond.Signal()
			fmt.Printf("increCommitIndex: {me: %d   updated commitIndex: %d}\n", rf.me, rf.commitIndex)
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
		rf.mu.Lock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{"null", -1}}
	rf.role = "follower"
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	// rf.AppendEntriesRPCMu = make([]sync.Mutex, len(rf.peers))
	// rf.RequestVoteRPCMu = make([]sync.Mutex, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// apply commited log entries to state machine
	go rf.applyToStateMachine()

	return rf
}
