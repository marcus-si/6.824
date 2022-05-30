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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

type Log struct {
	Term    int
	Command interface{}
}

const (
	Follower int = iota
	Candidate
	Leader
)

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []Log
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	logs              []Log
	role              int
	nextCommitIndex   int
	lastAppliedIndex  int
	electionTimeout   time.Time
	nextHeartBeatTime time.Time

	peersNextIndex []int //for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	peersMatchIndex []int //for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	applyCond          *sync.Cond
	applyChangeChan    chan (ApplyMsg)
	appendEntriesChan  chan int
	appendEntriesCond  *sync.Cond
	handleCommandMutex sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
	currentTerm, votedFor, logs, ok := rf.decodePersist(data)
	// Your code here (2C).
	// Example:

	if ok {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

func (rf *Raft) decodePersist(data []byte) (int, int, []Log, bool) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return 0, -1, nil, false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("unable to read persisted data\n")
	} else {
		return currentTerm, votedFor, logs, true
	}
	return 0, -1, nil, false
}

func (rf *Raft) createSnapshot(lastIncludedIndex int, lastIncludedTerm int) {
	if lastIncludedIndex > 0 && lastIncludedIndex <= getFullLogLength(rf) {
		rf.persistSnapshot(lastIncludedIndex, lastIncludedTerm)
	}
}

func (rf *Raft) persistSnapshot(lastIncludedIndex int, lastIncludedTerm int) {
	//state
	s := new(bytes.Buffer)
	c := labgob.NewEncoder(s)
	c.Encode(lastIncludedTerm)
	c.Encode(rf.votedFor)
	logs := getRemainLogs(rf, lastIncludedIndex)
	c.Encode(logs)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(lastIncludedIndex)

	snapshot := rf.snapshot
	if len(snapshot) == 0 {
		snapshot = rf.persister.ReadSnapshot()
	}
	prevIncludedIndex, _, xlogs, ok := rf.decodeSnapshot(snapshot)
	if prevIncludedIndex >= lastIncludedIndex {
		return
	}
	snapshotLogs := make([]interface{}, lastIncludedIndex-prevIncludedIndex)
	for i := prevIncludedIndex; i < lastIncludedIndex; i++ {
		snapshotLogs[i-prevIncludedIndex] = rf.logs[getRelativeIndex(rf, i)].Command
	}
	if ok && len(xlogs) > 0 {
		snapshotLogs = append(xlogs, snapshotLogs...)
	}
	if snapshotLogs[0] != nil {
		snapshotLogs = append([]interface{}{nil}, snapshotLogs...) //the first command must be nil
	}
	e.Encode(snapshotLogs)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(s.Bytes(), data)
	rf.snapshot = data
	// rf.lastIncludedTerm = lastIncludedTerm
	// fmt.Printf("instance %d, log length for snapshot %v, prev included index %d,  included index %d, included term %d, logs %v\n", rf.me, len(snapshotLogs), prevIncludedIndex, lastIncludedIndex, lastIncludedTerm, snapshotLogs)
}

func getRemainLogs(rf *Raft, lastIncludedIndex int) []Log {
	fullLogLenth := getFullLogLength(rf)
	if fullLogLenth > lastIncludedIndex {
		remainLogs := make([]Log, fullLogLenth-lastIncludedIndex)
		copy(remainLogs, rf.logs[getRelativeIndex(rf, lastIncludedIndex):])
		return remainLogs
	} else {
		return make([]Log, 0)
	}
}

func (rf *Raft) readSnapshot() {
	fmt.Println(rf.me, "read snapshot")
	persistedSnapshot := rf.persister.ReadSnapshot()
	lastIncludedIndex, lastIncludedeTerm, _, ok := rf.decodeSnapshot(persistedSnapshot)
	if ok {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedeTerm
		rf.snapshot = persistedSnapshot
		rf.nextCommitIndex = lastIncludedIndex
		rf.lastAppliedIndex = lastIncludedIndex
	}
}

func (rf *Raft) decodeSnapshot(snapshot []byte) (int, int, []interface{}, bool) {
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		var logs []interface{}
		if d.Decode(&lastIncludedIndex) != nil || d.Decode(&logs) != nil {
			log.Fatal("unalbe to read snapshot\n")
		} else {
			// fmt.Printf("instance %d from snapshot included index %d,  term %d, included logs %v\n", rf.me, lastIncludedIndex, rf.currentTerm, logs)
			return lastIncludedIndex, rf.currentTerm, logs, true
		}
	}
	return 0, 0, nil, false
}

type SnapshotArgs struct {
	LeaderTerm        int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type SnapshotReply struct {
	FollowerCurrentTerm int
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
// index: start from 1
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// if index < 1 {
	// 	return
	// }
	rf.mu.Lock()
	fullLogLength := getFullLogLength(rf)
	if index <= rf.lastIncludedIndex || index > fullLogLength {
		return
	}
	// fmt.Printf("before index %d, logs %v, applied index %d, included index %d\n", index, rf.logs, rf.lastAppliedIndex, rf.lastIncludedIndex)
	lastIncludedTerm := rf.logs[getRelativeIndex(rf, index-1)].Term
	updateSnapshot(rf, index, lastIncludedTerm, snapshot)
	// fmt.Printf("after index %d, last applied index %d, logs %v\n", index, rf.lastAppliedIndex, rf.logs)
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	reply.FollowerCurrentTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.LeaderTerm > rf.currentTerm {
		downgradeToFollower(rf, args.LeaderTerm)
		rf.mu.Unlock()
		return
	}
	updateSnapshot(rf, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)

	fmt.Printf("installed snapshot: instance %d, leader %d, lastIncludedIndex %d, lastAppliedIndex %d, leader included term %d, logs %v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, rf.lastAppliedIndex, args.LastIncludedTerm, rf.logs)
	rf.mu.Unlock()
}

func updateSnapshot(rf *Raft, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	//update log first
	rf.logs = getRemainLogs(rf, lastIncludedIndex)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	if lastIncludedIndex > rf.nextCommitIndex {
		rf.nextCommitIndex = lastIncludedIndex
		fmt.Printf("instance %d update nextcommitindex after replay snapshot\n", rf.me)
	}
	rf.snapshot = snapshot
	rf.applyChange()
}

func (rf *Raft) callInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) startInstallSnapshot(sid int) {
	// fmt.Printf("Leader %d install snapshot to %d, nextIndex %d, isLeader %v\n", rf.me, sid, rf.lastIncludedIndex, rf.role == Leader)
	if len(rf.snapshot) == 0 {
		return
	}
	lastIncludedIndex, _, _, ok := rf.decodeSnapshot(rf.snapshot)
	if !ok {
		return
	}
	lastIncludedTerm := getPrevTerm(rf, lastIncludedIndex-1)
	if lastIncludedTerm == 0 {
		lastIncludedTerm = rf.lastIncludedTerm
	}
	snapshotArgs := SnapshotArgs{rf.currentTerm, rf.me, lastIncludedIndex, lastIncludedTerm, rf.snapshot}
	snapshotReply := SnapshotReply{}
	if !rf.callInstallSnapshot(sid, &snapshotArgs, &snapshotReply) {
		return
	}
	if rf.currentTerm < snapshotReply.FollowerCurrentTerm {
		downgradeToFollower(rf, snapshotReply.FollowerCurrentTerm)
		return
	}
	rf.peersNextIndex[sid] = lastIncludedIndex
	// fmt.Printf("Done. Leader %d install snapshot to %d, nextIndex %d, isLeader %v\n", rf.me, sid, rf.lastIncludedIndex, rf.role == Leader)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId           int
	CandidateCurrentTerm  int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int //term in the last log record
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoterId          int
	VoterCurrentTerm int
	VoteGranted      bool
}

func getLastLogIndexAndTerm(rf *Raft) (prevLogIndex int, prevLogTerm int) {
	prevLogIndex = getFullLogLength(rf) - 1
	prevLogTerm = getPrevTerm(rf, prevLogIndex)
	return
}

func getPrevTerm(rf *Raft, prevIndex int) (prevTerm int) {
	prevTerm = 0
	if prevIndex >= rf.lastIncludedIndex && prevIndex < getFullLogLength(rf) {
		prevTerm = rf.logs[getRelativeIndex(rf, prevIndex)].Term
	} else if prevIndex == rf.lastIncludedIndex-1 {
		prevTerm = rf.lastIncludedTerm
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.VoterId = rf.me
	reply.VoterCurrentTerm = rf.currentTerm

	if args.CandidateCurrentTerm < rf.currentTerm { //Figure 2 -> RequestVote RPC -> Receiver implementation -> 1
		return
	}

	//Figure 2 -> RequestVote RPC -> Receiver implementation -> 2
	myLastLogIndex, myLastLogTerm := getLastLogIndexAndTerm(rf)
	logOk := (args.CandidateLastLogTerm > myLastLogTerm) || (args.CandidateLastLogTerm == myLastLogTerm && args.CandidateLastLogIndex >= myLastLogIndex)
	// fmt.Printf("instance %d, request args %v, mylastlog index %d, my lastlog term %d\n", rf.me, args, myLastLogIndex, myLastLogTerm)
	termHigher := args.CandidateCurrentTerm > rf.currentTerm
	termOk := termHigher || (args.CandidateCurrentTerm == rf.currentTerm && (rf.votedFor == args.CandidateId || rf.votedFor == -1))

	reply.VoterCurrentTerm = rf.currentTerm
	if logOk && termOk {
		rf.currentTerm = args.CandidateCurrentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.persist()
		rf.electionTimeout = getNextElectionTimeout()
	} else if termHigher {
		downgradeToFollower(rf, args.CandidateCurrentTerm) //Figure 2 -> Rules for Servers -> All Servers -> Dot 2
	}
	// fmt.Printf("instance %d grant %v to candidate %d, args %v, instance term %v, instance votedFor %d\n", rf.me, reply.VoteGranted, args.CandidateId, args, rf.currentTerm, rf.votedFor)
}

//Figure 2 -> Rules for Servers -> All Servers -> Candidates (§5.2)
func (rf *Raft) doElection() {
	// fmt.Println("instance", rf.me, "start election at term", rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimeout = getNextElectionTimeout()
	rf.role = Candidate
	rf.persist()
	newTerm := rf.currentTerm
	lastLogIndex, lastLogTerm := getLastLogIndexAndTerm(rf)
	args := RequestVoteArgs{rf.me, newTerm, lastLogIndex, lastLogTerm}
	majority := len(rf.peers) / 2
	voteCount := 1
	var becomeLeader sync.Once
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(sid int, voteCount *int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(sid, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoterCurrentTerm > rf.currentTerm {
					downgradeToFollower(rf, reply.VoterCurrentTerm)
					return
				}
				if reply.VoteGranted {
					*voteCount++
					// fmt.Printf("candidate %d get vote from %d, total vote %d\n", rf.me, reply.VoterId, *voteCount)
					if *voteCount > majority && rf.currentTerm == args.CandidateCurrentTerm && rf.role == Candidate { //Figure 2:
						becomeLeader.Do(func() {
							rf.role = Leader
							rf.sendHeartBeats(false)
							resetPeersIndex(rf)
							fmt.Printf("New leader is %d at term %d with args %v, logs%v, last included index %d, last applied index %d, next commit index %d\n", rf.me, rf.currentTerm, args, rf.logs, rf.lastIncludedIndex, rf.lastAppliedIndex, rf.nextCommitIndex)
						})
					}
				}
			}
		}(i, &voteCount)
	}
}

func resetPeersIndex(rf *Raft) {
	peersNum := len(rf.peers)
	rf.peersMatchIndex = make([]int, peersNum)
	rf.peersNextIndex = make([]int, peersNum)
	fullLogLength := getFullLogLength(rf)
	for i := 0; i < peersNum; i++ {
		rf.peersNextIndex[i] = fullLogLength
		rf.peersMatchIndex[i] = -1
	}
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

type AppendEntriesArgs struct {
	LeaderCurrentTerm     int
	LeaderId              int
	PrevLogIndex          int
	PrevLogTerm           int
	Entries               []Log
	LeaderNextCommitIndex int
}

type AppendEntriesReply struct {
	FollowerCurrentTerm       int
	FollowerLastIncludedIndex int
	LogNotMatched             bool
	Success                   bool
	XTerm                     int
	XIndex                    int
	XLen                      int
}

func kickOffApplyChange(rf *Raft, leaderNextCommitIndex int) {
	if leaderNextCommitIndex > rf.nextCommitIndex {
		rf.nextCommitIndex = leaderNextCommitIndex
		logLength := getFullLogLength(rf)
		if rf.nextCommitIndex > logLength {
			rf.nextCommitIndex = logLength
		} //Figure2: AppendEntries RPC -> Receiver implementation -> 5
		rf.applyChange()
	}
}

func getXInfo(rf *Raft, prevLogIndex int, lastIncludedIndex int) (xIndex int, xTerm int, xLen int) {
	xIndex = -1
	xTerm = 0
	xLen = getFullLogLength(rf)
	// fmt.Printf("instance %d, last included index %d, logs %v\n", rf.me, rf.lastIncludedIndex, rf.logs)
	if prevLogIndex >= lastIncludedIndex && prevLogIndex < xLen {
		xIndex = prevLogIndex
		xTerm = rf.logs[getRelativeIndex(rf, prevLogIndex)].Term
		if rf.lastIncludedTerm == xTerm {
			xIndex = lastIncludedIndex - 1
			return
		}
		for i := xIndex - 1; i >= lastIncludedIndex; i-- { //find the first index of xTerm
			if xTerm == rf.logs[getRelativeIndex(rf, i)].Term {
				xIndex = i
				continue
			}
			break
		}
	} else if prevLogIndex < lastIncludedIndex {
		xIndex = lastIncludedIndex - 1
		xTerm = rf.lastIncludedTerm
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.LogNotMatched = false
	reply.FollowerCurrentTerm = rf.currentTerm
	if args.LeaderCurrentTerm < rf.currentTerm { //Figure2: AppendEntries RPC -> Receiver implementation -> 1
		return
	}

	if args.LeaderCurrentTerm > rf.currentTerm {
		downgradeToFollower(rf, args.LeaderCurrentTerm)
		return
	}
	rf.electionTimeout = getNextElectionTimeout()

	myLogLength := getFullLogLength(rf)
	myLogMatchTerm := 0
	lastIncludedIndex := rf.lastIncludedIndex
	if myLogLength > args.PrevLogIndex {
		if args.PrevLogIndex >= lastIncludedIndex {
			myLogMatchTerm = rf.logs[getRelativeIndex(rf, args.PrevLogIndex)].Term
		} else if args.PrevLogIndex == lastIncludedIndex-1 {
			myLogMatchTerm = rf.lastIncludedTerm
		} else {
			myLogMatchTerm = args.PrevLogTerm - 1 //make sure log unmatched
		}
	}
	logOk := myLogLength > args.PrevLogIndex && myLogMatchTerm == args.PrevLogTerm //Figure2: AppendEntries RPC -> Receiver implementation -> 2
	if !logOk {
		reply.LogNotMatched = true
		reply.FollowerLastIncludedIndex = lastIncludedIndex
		reply.XIndex, reply.XTerm, reply.XLen = getXInfo(rf, args.PrevLogIndex, lastIncludedIndex)
		// fmt.Printf("instance %d, args %v, my logs %v, reply %v, last included index %d\n", rf.me, args, rf.logs, reply, rf.lastIncludedIndex)
		return
	}

	logChanged := false
	if args.PrevLogIndex+1 != myLogLength {
		rf.logs = rf.logs[0:getRelativeIndex(rf, args.PrevLogIndex+1)] //Figure2: AppendEntries RPC -> Receiver implementation -> 3
		logChanged = true
	}

	if args.Entries != nil && len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...) //Figure2: AppendEntries RPC -> Receiver implementation -> 4
		logChanged = true
	}

	if logChanged {
		rf.persist()
	}

	kickOffApplyChange(rf, args.LeaderNextCommitIndex)
	reply.LogNotMatched = false
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeats(shouldCheckNextHeartBeatTime bool) {
	if shouldCheckNextHeartBeatTime && time.Now().Before(rf.nextHeartBeatTime) {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// if withLogs && len(rf.logs) <= rf.peersNextIndex[i] {
		// 	return
		// }
		// fmt.Println("leader", rf.me, "install snapshot to", i, "next index", rf.peersNextIndex[i], "last included index", rf.lastIncludedIndex)
		// if rf.peersNextIndex[i] < rf.lastIncludedIndex {
		// 	rf.startInstallSnapshot(i)
		// }
		// fmt.Println("updated: leader", rf.me, "install snapshot to", i, "next index", rf.peersNextIndex[i], "last included index", rf.lastIncludedIndex)
		prevIndex := rf.peersNextIndex[i] - 1
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex, getPrevTerm(rf, prevIndex), buildEntries(rf, prevIndex), rf.nextCommitIndex}
		go func(sid int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(sid, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.FollowerCurrentTerm > rf.currentTerm { //Figure 2: Rules for Servers -> All Servers -> Bullet 2
				downgradeToFollower(rf, reply.FollowerCurrentTerm)
				return
			}
			if args.LeaderCurrentTerm != rf.currentTerm {
				return
			}
			if reply.Success {
				fmt.Printf("%d reply success is %v\n", sid, reply)
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				logLength := getFullLogLength(rf)
				if newMatchIndex >= logLength {
					newMatchIndex = logLength - 1
				}
				if newMatchIndex > rf.peersMatchIndex[sid] {
					// fmt.Println(sid, "reply success new match index", newMatchIndex, "log length", logLength)
					rf.peersMatchIndex[sid] = newMatchIndex
					rf.peersNextIndex[sid] = newMatchIndex + 1
				}
			} else if reply.LogNotMatched {
				fmt.Println(sid, "reply not matched", args, reply)
				updateNextIndex(rf, &reply, sid)

			}
			updateLeaderNextCommitIndex(rf)
		}(i)
	}
	rf.nextHeartBeatTime = time.Now().Add(HEART_BEAT_INTERVAL)
}

func updateNextIndex(rf *Raft, reply *AppendEntriesReply, sid int) {
	// fmt.Println(sid, "old next index", rf.peersNextIndex[sid])
	nextIndex := reply.XLen //full length
	if reply.XIndex != -1 {
		nextIndex = reply.XIndex
		index := getRelativeIndex(rf, nextIndex)
		if (index == -1 && rf.lastIncludedTerm == reply.XTerm) || (index >= 0 && index < len(rf.logs) && rf.logs[index].Term == reply.XTerm) {
			nextIndex++
		}
	}
	if nextIndex < reply.FollowerLastIncludedIndex || nextIndex < rf.lastIncludedIndex || nextIndex > getFullLogLength(rf) {
		rf.startInstallSnapshot(sid)
	} else {
		rf.peersNextIndex[sid] = nextIndex
	}
	// fmt.Println(sid, "new next index", rf.peersNextIndex[sid])
}

func getFullLogLength(rf *Raft) int {
	return len(rf.logs) + rf.lastIncludedIndex
}

func getRelativeIndex(rf *Raft, fullIndex int) int {
	// fmt.Printf("index %d, full index %d\n", rf.lastIncludedIndex, fullIndex)
	if rf.lastIncludedIndex == 0 {
		return fullIndex
	}
	return fullIndex - rf.lastIncludedIndex //lastIncludedIndex starts from 1
}

func updateLeaderNextCommitIndex(rf *Raft) {
	if rf.role != Leader {
		return
	}
	if len(rf.logs) == 0 {
		return
	}
	logLength := getFullLogLength(rf)
	// fmt.Printf("leader current commit index %d\n", rf.nextCommitIndex)
	for i := logLength - 1; i >= rf.nextCommitIndex && i >= rf.lastIncludedIndex; i-- {
		if rf.logs[getRelativeIndex(rf, i)].Term != rf.currentTerm {
			return
		}
		logCopiedCount := 1
		for sid := 0; sid < len(rf.peers); sid++ {
			if sid != rf.me && rf.peersMatchIndex[sid] >= i && rf.peersMatchIndex[sid] < logLength {
				logCopiedCount++
			}
			if logCopiedCount > len(rf.peers)/2 {
				rf.nextCommitIndex = i + 1
				rf.applyChange()
				rf.sendHeartBeats(false)
				return
			}
		}
	}
}

func (rf *Raft) applyChange() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) startApplyChange() {
	for !rf.killed() {
		rf.mu.Lock()
		fullLogLength := getFullLogLength(rf)

		if rf.lastIncludedIndex > rf.lastAppliedIndex && len(rf.snapshot) > 0 {
			i1, t1, l1, _ := rf.decodeSnapshot(rf.snapshot)
			snapshotMsg := rf.buildSnapshotApplyMessage(rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot)
			rf.lastAppliedIndex = rf.lastIncludedIndex
			fmt.Printf("instance %d replay snapshot, new last applied index %d, index, term and logs in snapshot %d, %d, %v\n", rf.me, rf.lastAppliedIndex, i1, t1, l1)
			rf.mu.Unlock()
			rf.applyChangeChan <- snapshotMsg
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		} else {
			// fmt.Printf("instance %d lastAppliedIndex %d, next commit index %d,  current term %v\n", rf.me, rf.lastAppliedIndex, rf.nextCommitIndex, rf.currentTerm)
			for rf.lastAppliedIndex < rf.nextCommitIndex && rf.lastAppliedIndex < fullLogLength {
				// fmt.Printf("instance %d index %d , last included index %d,  logs%v\n", rf.me, rf.lastAppliedIndex+1, rf.lastIncludedIndex, rf.logs)
				fmt.Printf("instance %d index %d command %v, current term %v\n", rf.me, rf.lastAppliedIndex+1, rf.logs[getRelativeIndex(rf, rf.lastAppliedIndex)].Command, rf.currentTerm)
				index := getRelativeIndex(rf, rf.lastAppliedIndex)
				msg := rf.buildApplyMsgForCommit(rf.logs[index].Command, rf.lastAppliedIndex+1)
				rf.lastAppliedIndex++
				rf.createSnapshot(rf.lastAppliedIndex, rf.currentTerm)
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				rf.applyChangeChan <- msg
				rf.mu.Lock()
			}
		}

		rf.applyCond.Wait()
		rf.mu.Unlock()
	}
}

func (rf *Raft) buildApplyMsgForCommit(command interface{}, commitIndex int) ApplyMsg {
	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = true
	applyMsg.Command = command
	applyMsg.CommandIndex = commitIndex
	return applyMsg
}

func (rf *Raft) buildSnapshotApplyMessage(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) ApplyMsg {
	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = false
	applyMsg.SnapshotIndex = lastIncludedIndex
	applyMsg.SnapshotTerm = lastIncludedTerm
	applyMsg.Snapshot = snapshot
	applyMsg.SnapshotValid = true
	return applyMsg
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
	rf.handleCommandMutex.Lock()
	defer rf.handleCommandMutex.Unlock()
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	if !isLeader {
		rf.mu.Unlock()
		return -1, term, false
	}
	index = getFullLogLength(rf) + 1
	rf.logs = append(rf.logs, Log{term, command})
	// fmt.Printf("Leader %d at term %d command %v\n", rf.me, term, command)
	rf.persist()
	rf.sendHeartBeats(false)
	rf.mu.Unlock()
	time.Sleep(HEART_BEAT_INTERVAL / 5)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.killed() && term == rf.currentTerm {
		fmt.Printf("Return: Leader %d at term %d, index %d, command %v\n", rf.me, term, index, command)
		return index, term, isLeader
	} else {
		fmt.Printf("Return: Leader %d at term %d, index %d, command %v\n", rf.me, rf.currentTerm, -1, command)
		return -1, rf.currentTerm, false
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

func getNextElectionTimeout() time.Time {
	rand.Seed(time.Now().UnixNano())
	randomTime := time.Duration(350+(rand.Int63()%250)) * time.Millisecond
	return time.Now().Add(randomTime)
}

func buildEntries(rf *Raft, prevLogIndex int) []Log {
	logLength := getFullLogLength(rf)
	next := prevLogIndex + 1
	relativeIndex := getRelativeIndex(rf, next)
	if logLength == 0 || next >= logLength || relativeIndex < 0 {
		return nil
	}
	entries := make([]Log, logLength-next)
	copy(entries, rf.logs[relativeIndex:])
	return entries
}

func downgradeToFollower(rf *Raft, term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimeout = getNextElectionTimeout()
	rf.persist()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
const HEART_BEAT_INTERVAL = 100 * time.Millisecond

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(HEART_BEAT_INTERVAL)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.sendHeartBeats(true)
			rf.electionTimeout = getNextElectionTimeout()
		} else if time.Now().After(rf.electionTimeout) {
			rf.doElection()
		}
		rf.mu.Unlock()
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
	rf.role = Follower
	rf.nextCommitIndex = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastAppliedIndex = 0
	rf.logs = make([]Log, 0)
	resetPeersIndex(rf)
	rf.electionTimeout = time.Now()
	rf.applyChangeChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendEntriesChan = make(chan int, len(rf.peers)-1)
	rf.appendEntriesCond = sync.NewCond(&sync.Mutex{})

	rf.lastIncludedIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.startApplyChange()

	return rf
}
