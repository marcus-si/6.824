package raft

//only leaders can initiate a log and server client requests
//if a leader never dies, the current term will not change
//a leader will be blocked when commiting changes but unable to get a majority of supports, and its terms will not change
// Higher term trick: for Leader, my term < your term; for Candidate, my term <= your term
// Check the chan length to make sure the chan is empty, otherwise there will be data conflicts
//send higher term info only when checking replys of RequestVote or AppendEntries
// check lengths of RequestVoteChan and AppendEntriesChan to avoid duplicate votes or AppendEntries
//make channels as the member of Raft to separate channels among Raft instances
// the difference between HeartBeat interval and election timeout must be bigger enough, e.g 1 election timeout = 5 * heatbeats interval

//Different ways to handle AE:
// Follower -> update term if getting a strictly greater one, update logs
// Candidate -> downgrade to Follower
// Leader -> downgrade to Follower if getting a greater or equal term

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

	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	Follower uint32 = iota
	Candidate
	Leader
)

const HeartsBeatsInterval = time.Duration(150) * time.Millisecond

// const TimerExtendedUnit = 5 * HeartsBeatsInterval

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
	currentTerm       uint32
	votedFor          int
	logs              []Log
	commitIndex       int
	currentRole       uint32
	votesReceivedFrom []int
	sentLength        map[int]int
	ackedLength       map[int]int

	//chan
	appendEntriesChan      chan *AppendEntriesArgs
	requestVoteChan        chan *RequestVoteArgs
	electionHigherTermChan chan HigherTermInfo
	beatHigherTermChan     chan HigherTermInfo
	voteOkChan             chan struct{}
	voteFailedChan         chan struct{}
	lostLeaderPositionChan chan struct{}
	applyChangChan         chan ApplyMsg

	runCommandMutex sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// time.Sleep(600 * time.Millisecond)
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = rf.currentRole == Leader
	defer func() {
		rf.mu.Unlock()
	}()
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId           int
	CandidateCurrentTerm  uint32
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
	VoterCurrentTerm uint32
	VoteGranted      bool
}

func (rf *Raft) startServer() {
	for !rf.killed() {
		switch rf.currentRole {
		case Follower:
			// fmt.Printf("instance %d following \n", rf.me)
			rf.following()
		case Candidate:
			// fmt.Printf("instance %d electing \n", rf.me)
			rf.electing()
		case Leader:
			// fmt.Printf("instance %d leading \n", rf.me)
			rf.beating()
		}
	}
}

type HigherTermInfo struct {
	term     uint32
	leaderId int
}

//monitoring election timeout and prepare to change to Candidate
func (rf *Raft) following() {
	// now := time.Now()
	electionTimeout := getRandSleepMilliseconds()
	timer := time.NewTimer(electionTimeout) //sleep more time at first to allow other candidates have more time to request votes
	// now, electionTimeout = resetTimer(timer, electionTimeout, now)
	for !rf.killed() {
		// fmt.Printf("instance %d following, timeout in %d milliseconds\n", rf.me, electionTimeout/time.Millisecond)
		select {
		case args := <-rf.appendEntriesChan:
			// fmt.Printf("Follower instance %d received AE, reset timer\n", rf.me)
			// now, electionTimeout = resetTimer(timer, electionTimeout, now)
			timer.Reset(getRandSleepMilliseconds())
			updateTerm(rf, args.LeaderCurrentTerm) //update terms if Leader has a strictly bigger term
			// fmt.Printf("Follower instance %d updated term\n", rf.me)
		case <-rf.requestVoteChan: //grant vote
			// fmt.Printf("Follower instance %d granted vote, reset timer\n", rf.me)
			// now, electionTimeout = resetTimer(timer, electionTimeout, now)
			timer.Reset(getRandSleepMilliseconds())
		case <-timer.C:
			// fmt.Printf("Follower instance %d become candidate\n", rf.me)
			rf.changeRole(Follower, Candidate)
			return
		}
	}
}

func updateTerm(rf *Raft, newTerm uint32) {
	rf.mu.Lock()
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
	}
	rf.mu.Unlock()
}

func doElection(rf *Raft, voteCountMap map[int]int) {
	rf.mu.Lock()
	if rf.currentRole != Candidate { //check the candidate status again
		rf.mu.Unlock()
		return
	}
	rf.votedFor = rf.me
	voteCountMap[rf.me] = 1
	rf.currentTerm++
	prevLogIndex, prevLogTerm := getPrevLogIndexAndTerm(rf.logs)
	args := RequestVoteArgs{rf.me, rf.currentTerm, prevLogIndex, prevLogTerm}
	rf.mu.Unlock()
	majority := int(math.Ceil(float64(len(rf.peers)+1) / 2))
	voteLock := sync.Mutex{}
	stateSent := false
	// fmt.Printf("instance %d start an election at term %d, peers %d\n", rf.me, args.CandidateCurrentTerm, len(rf.peers))
	failedCount := 0
	for i := 0; i < len(rf.peers); i++ {
		//use go routine as a request may be blocked by others
		// wg.Add(1)
		if i == rf.me {
			continue
		}
		go func(sid int) {
			reply := RequestVoteReply{}
			// fmt.Printf("instance %d send vote request to %d\n", rf.me, sid)
			// fmt.Printf("reply from %d to %d is %v, my term is %d, voter term is %d,\n", reply.VoterId, rf.me, reply, args.CandidateCurrentTerm, reply.VoterCurrentTerm)
			if rf.sendRequestVote(sid, &args, &reply) {
				// fmt.Printf("reply from %d to %d is %v, voter term is %d\n", reply.VoterId, rf.me, reply, reply.VoterCurrentTerm)
				voteLock.Lock()
				// fmt.Printf("reply to %d from %d is %v, my term is %d, voter term is %d, count map is %v\n", rf.me, reply.VoterId, reply, args.CandidateCurrentTerm, reply.VoterCurrentTerm, voteCountMap)
				if reply.VoteGranted {
					voteCountMap[reply.VoterId] = 1
					if len(voteCountMap) >= majority && !stateSent {
						rf.voteOkChan <- struct{}{}
						stateSent = true
						return
					}
				} else if reply.VoterCurrentTerm >= args.CandidateCurrentTerm && !stateSent {
					// fmt.Printf("instance %d get a higher or equal term %d\n", rf.me, reply.VoterCurrentTerm)
					rf.electionHigherTermChan <- HigherTermInfo{reply.VoterCurrentTerm, -1}
					// setRoleCurrentTermAndVoteFor(rf, Candidate, Follower, reply.VoterCurrentTerm, -1)
					stateSent = true
					return
				}
				voteLock.Unlock()
			} else {
				voteLock.Lock()
				// fmt.Printf("instance %d is unable to send vote request to %d\n", rf.me, sid)
				failedCount++
				if failedCount >= majority && !stateSent {
					rf.voteFailedChan <- struct{}{}
					stateSent = true
				}
				voteLock.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) electing() {
	// fmt.Println("electing")
	resultMap := make(map[int]int)
	electionTimeout := getRandSleepMilliseconds()
	timer := time.NewTimer(electionTimeout)
	for {
		doElection(rf, resultMap)
		select {
		case higherTerm := <-rf.electionHigherTermChan:
			// fmt.Printf("Candidate instance %d handled election higher term, downgrade to Follower\n", rf.me)
			setRoleCurrentTermAndVoteFor(rf, Candidate, Follower, higherTerm.term, -1)
			return
		case <-rf.voteOkChan:
			fmt.Printf("new leader is %d\n", rf.me)
			rf.changeRole(Candidate, Leader)
			sendBeats(rf) // send heartbeats immediately after becoming the leader
			return
		case args := <-rf.appendEntriesChan:
			// fmt.Printf("candidate instance %d received AE, downgrade to follower\n", rf.me)
			// rf.changeRole(Candidate, Follower)
			setRoleCurrentTermAndVoteFor(rf, Candidate, Follower, args.LeaderCurrentTerm, -1)
			return
		case <-rf.requestVoteChan:
			// fmt.Printf("candidate instance %d granted vote, downgrade to follower\n", rf.me)
			rf.changeRole(Candidate, Follower)
			return
		case <-rf.voteFailedChan:
			// fmt.Printf("instance %d did not win, waiting\n", rf.me)
			downgradeToFollower(rf)
			// time.Sleep(getRandSleepMilliseconds() * time.Duration(len(rf.peers)*int(time.Second)))
			return
		case <-timer.C:
			downgradeToFollower(rf)
			return
		}
	}
}

func (rf *Raft) changeRole(oldRole uint32, newRole uint32) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	if oldRole != rf.currentRole { //role has been changed
		return
	}
	if rf.currentRole == Leader && newRole != Leader {
		rf.lostLeaderPositionChan <- struct{}{}
	}
	rf.currentRole = newRole
	// fmt.Printf("instance %d role: %d -> %d\n", rf.me, rf.currentRole, newRole)
}

func downgradeToFollower(rf *Raft) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	if rf.currentRole != Candidate {
		return
	}
	rf.currentRole = Follower
	if rf.votedFor == rf.me {
		rf.votedFor = -1
	}
}

func setRoleCurrentTermAndVoteFor(rf *Raft, oldRole uint32, newRole uint32, newTerm uint32, voteFor int) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	if rf.currentRole != oldRole { //already changed
		fmt.Printf("instance %d already changed to %d from %d\n", rf.me, newRole, oldRole)
		return
	}
	if newTerm >= rf.currentTerm { //check term again to ensure current term is still less than the new term
		rf.currentRole = newRole
		rf.currentTerm = newTerm
		rf.votedFor = voteFor
		// fmt.Printf("instance %d role changed: %d -> %d, reset timer %v\n", rf.me, oldRole, newRole, rf.shouldDelayElection)
	}
	if rf.currentRole == Leader && newRole != Leader {
		rf.lostLeaderPositionChan <- struct{}{}
	}
}

func sendBeats(rf *Raft) {
	rf.mu.Lock()
	if rf.currentRole != Leader { //check the leader status again
		rf.mu.Unlock()
		return
	}
	prevLogIndex, prevLogTerm := getPrevLogIndexAndTerm(rf.logs)
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, nil, rf.commitIndex}
	rf.mu.Unlock()
	higherTermSent := false
	higherTermLock := sync.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// fmt.Printf("leader %d sending beats to follower %d\n", rf.me, i)
		go func(sid int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(sid, &args, &reply) {
				if reply.FollowerCurrentTerm > args.LeaderCurrentTerm {
					higherTermLock.Lock()
					if !higherTermSent {
						// fmt.Printf("leader: %d, term: %d; instance: %d, term:%d\n", rf.me, args.LeaderCurrentTerm, sid, reply.FollowerCurrentTerm)
						rf.beatHigherTermChan <- HigherTermInfo{reply.FollowerCurrentTerm, -1}
						higherTermSent = true
					}
					higherTermLock.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) beating() {
	timer := time.NewTimer(HeartsBeatsInterval)
	for !rf.killed() {
		sendBeats(rf)
		select {
		case ae := <-rf.appendEntriesChan:
			// fmt.Printf("instance %d downgrade to follower from leader AE\n", rf.me)
			// rf.changeRole(Leader, Follower)
			// downgrade to Follower if receiving a greater term
			setRoleCurrentTermAndVoteFor(rf, Leader, Follower, ae.LeaderCurrentTerm, -1)
			return
		case <-rf.requestVoteChan:
			// fmt.Printf("instance %d downgrade to follower from leader RV\n", rf.me)
			rf.changeRole(Leader, Follower)
			return
		case higherTerm := <-rf.beatHigherTermChan:
			// fmt.Printf("instance %d downgrade to follower from leader\n", rf.me)
			setRoleCurrentTermAndVoteFor(rf, Leader, Follower, higherTerm.term, higherTerm.leaderId)
			return
		case <-timer.C:
			timer.Reset(HeartsBeatsInterval)
		}
	}
}

func getPrevLogIndexAndTerm(logs []Log) (prevLogIndex int, prevLogTerm int) {
	prevLogIndex = len(logs) - 1
	prevLogTerm = -1
	if prevLogIndex >= 0 {
		prevLogTerm = logs[prevLogIndex].Term
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("instance %d received vote request from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	// fmt.Printf("request vote from %d to %d at term %d with args %v, my Vote for is %d and my term is %d\n", args.CandidateId, rf.me, args.CandidateCurrentTerm, args, rf.votedFor, rf.currentTerm)
	voteGranted := false
	reply.VoterCurrentTerm = rf.currentTerm
	// fmt.Printf("request vote instance %d: current term changed from %d to %d\n", rf.me, rf.currentTerm, args.CandidateCurrentTerm)
	if len(rf.requestVoteChan) == 0 { //ensure no vote in progress
		myLastLogIndex, myLastLogTerm := getPrevLogIndexAndTerm(rf.logs)
		logOk := (args.CandidateLastLogTerm > myLastLogTerm) || (args.CandidateLastLogTerm == myLastLogTerm && args.CandidateLastLogIndex >= myLastLogIndex)
		termOk := (args.CandidateCurrentTerm > rf.currentTerm) || (args.CandidateCurrentTerm == rf.currentTerm && (rf.votedFor == args.CandidateId || rf.votedFor == -1))
		if logOk && termOk {
			voteGranted = true
			rf.currentTerm = args.CandidateCurrentTerm
			rf.votedFor = args.CandidateId
			// fmt.Printf("reply to %d from %d for term %d, voted\n", args.CandidateId, rf.me, args.CandidateCurrentTerm)
			rf.requestVoteChan <- args
		}
	}
	reply.VoteGranted = voteGranted
	reply.VoterId = rf.me
	// fmt.Printf("reply to %d from %d for term %d with reply %v, new vote for is %d, instance is Follower %v\n", args.CandidateId, rf.me, args.CandidateCurrentTerm, reply, rf.votedFor, rf.currentRole == Follower)
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
	LeaderCurrentTerm uint32
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	// LeaderLogLength   int
	Entries           []Log
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	FollowerCurrentTerm uint32
	FollowerLastIndex   int
	FollowerLastTerm    int
	FollowerCommitIndex int
	Success             bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	reply.FollowerCurrentTerm = rf.currentTerm
	reply.Success = false
	reply.FollowerLastIndex, reply.FollowerLastTerm = getPrevLogIndexAndTerm(rf.logs)
	reply.FollowerCommitIndex = rf.commitIndex
	myLogLength := len(rf.logs)
	myLogMatchTerm := -1
	if myLogLength > args.PrevLogIndex && args.PrevLogIndex > -1 {
		myLogMatchTerm = rf.logs[args.PrevLogIndex].Term
	}

	// if len(rf.appendEntriesChan) == 1 { //when AE in progress
	// 	return
	// }

	termOk := args.LeaderCurrentTerm >= rf.currentTerm
	logOk := myLogLength > args.PrevLogIndex && myLogMatchTerm == args.PrevLogTerm

	// fmt.Printf("termOk:%v, leader term: %d, follower term %d\n", termOk, args.LeaderCurrentTerm, rf.currentTerm)
	// fmt.Printf("logOk:%v, instance %d: term %d, prevLogIndex %d, prevLogTerm %d\n", logOk, rf.me, rf.currentTerm, reply.FollowerLastIndex, reply.FollowerLastTerm)

	if termOk && logOk {
		reply.Success = true
		rf.currentTerm = args.LeaderCurrentTerm
		rf.votedFor = args.LeaderId

		//update args.PrevLogIndex
		uncopiedEntryIndex := 0
		if args.PrevLogIndex == rf.commitIndex-1 {
			for i := 0; i < len(args.Entries) && rf.commitIndex+i < len(rf.logs); i++ {
				if rf.logs[rf.commitIndex+i].Term == args.Entries[i].Term {
					args.PrevLogIndex++
					uncopiedEntryIndex++
					// fmt.Printf("instance %d update prev log index, old: %d, new:%d\n", rf.me, args.PrevLogIndex-uncopiedEntryIndex, args.PrevLogIndex)
					continue
				}
				break
			}
		}
		if uncopiedEntryIndex > 0 {
			args.Entries = args.Entries[uncopiedEntryIndex:]
			// rf.logs = rf.logs[0:args.PrevLogIndex]
		}
		//delete uncommited logs
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]

		//append new entries
		if args.Entries != nil && len(args.Entries) > 0 {
			// for _, e := range args.Entries {
			// 	fmt.Printf("Follower %d, entry command %v\n", rf.me, e.Command)
			// }

			rf.logs = append(rf.logs, args.Entries...)
		}
		// fmt.Printf("instance %d logs %v, entries %v\n", rf.me, rf.logs, args.Entries)

		//update commit index
		// fmt.Printf("instance %d, go to update commit index from %d to %d\n", rf.me, rf.commitIndex, args.LeaderCommitIndex)
		if args.LeaderCommitIndex > rf.commitIndex {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = args.LeaderCommitIndex
			newLength := len(rf.logs)
			if rf.commitIndex > newLength {
				rf.commitIndex = newLength
			}
			// fmt.Printf("instance %d, logs %v\n", rf.me, rf.logs)
			// fmt.Printf("commit index gap is %d, entry length %d\n", rf.commitIndex-oldCommitIndex, len(args.Entries))
			// start := oldCommitIndex -1;
			// if len(args.Entries) > 0 {
			// start := newLength - len(args.Entries) - 1
			// }
			// gap := rf.commitIndex - oldCommitIndex
			// if gap > 1 {
			// 	start = start - gap + 3
			// }
			for i := oldCommitIndex; i < rf.commitIndex; i++ {
				// fmt.Printf("instance %d commit index %d, leader commit index %d, follower command %v\n", rf.me, i+1, args.LeaderCommitIndex, rf.logs[i].Command)
				applyMsg := rf.buildApplyMsgForCommit(rf.logs[i].Command, i+1)
				// time.Sleep(20 * time.Millisecond)
				rf.applyChangChan <- applyMsg
			}
		}
	}
	if termOk {
		// fmt.Printf("instance %d sending ae chan for leader %d\n", rf.me, args.LeaderId)
		rf.appendEntriesChan <- args
	}
}

func (rf *Raft) buildApplyMsgForCommit(command interface{}, commitIndex int) ApplyMsg {
	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = true
	applyMsg.Command = command
	applyMsg.CommandIndex = commitIndex
	// fmt.Printf("build apply msg instance: %d, index:%d, command %v\n", rf.me, commitIndex, command)
	return applyMsg
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	// fmt.Printf("instance %d got command %v\n", rf.me, command)
	// fmt.Printf("instance %d is Leader %v at term %d\n", rf.me, isLeader, term)
	term, isLeader = rf.GetState()
	// fmt.Printf("instance %d, command %v, term %d, isLeader %v\n", rf.me, command, term, isLeader)
	if !isLeader {
		return index, term, isLeader
	}
	// time.Sleep(10 * time.Millisecond)
	rf.runCommandMutex.Lock()
	defer func() {
		rf.runCommandMutex.Unlock()
	}()
	return rf.handleCommand(term, isLeader, command)
}

func (rf *Raft) handleCommand(term int, isLeader bool, command interface{}) (int, int, bool) {
	logs := make([]Log, len(rf.logs))
	rf.mu.Lock()
	// for i := 0; i < len(rf.logs); i++ {
	// fmt.Printf("instance %d, log lenth %d, commitIndex %d, isLeader %v\n", rf.me, len(rf.logs), rf.commitIndex, rf.currentRole == Leader)
	// }
	index := rf.commitIndex
	copy(logs, rf.logs)
	var entries []Log
	// if index < len(rf.logs) {
	// 	logs = rf.logs[0:index]
	// 	entries = rf.logs[index:]
	// }
	newLog := Log{term, command}
	entries = append(entries, newLog)
	rf.logs = append(rf.logs, newLog)
	// newLength := len(rf.logs)
	rf.mu.Unlock()

	newIndex, ok := rf.leaderSendAppendEntries(logs, entries, term, index)
	if ok {
		rf.mu.Lock()
		if rf.commitIndex == index {
			rf.commitIndex = newIndex
			// fmt.Printf("leader %d, current index %d, current term %d, peers %v, command is %v, entry command is %v\n", rf.me, newIndex, term, len(rf.peers), command, entries[0].Command)
			rf.applyChangChan <- rf.buildApplyMsgForCommit(rf.logs[index].Command, newIndex)
			// for i := index; i < newIndex; i++ {
			// 	// fmt.Printf("leader %d, index %d, command %v\n", rf.me, i, rf.logs[newLength-newIndex+i].Command)
			// }
		}
		rf.mu.Unlock()

	}
	return newIndex, term, isLeader
}

func (rf *Raft) leaderSendAppendEntries(logs []Log, entries []Log, term int, index int) (int, bool) {
	// rf.mu.Lock()
	// if rf.currentRole != Leader || term != rf.currentTerm || index != rf.commitIndex {
	// 	// fmt.Printf("Leader %d status changed, unable to send AE\n", rf.me)
	// 	rf.mu.Unlock()
	// 	return -1, ok
	// }
	// fmt.Printf("log length %d, entries length %d, \n", len(logs), len(entries))
	leaderPrevLogIndex, leaderPrevLogTerm := getPrevLogIndexAndTerm(logs)
	args := AppendEntriesArgs{uint32(term), rf.me, leaderPrevLogIndex, leaderPrevLogTerm, entries, index}
	// if len(args.Entries) > 0 {
	// 	fmt.Println("args entries command: ", args.Entries[0].Command)
	// }
	nextCommitIndex := index + 1
	aeOkChan := make(chan int, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(sid int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(sid, &args, &reply)
			if reply.Success {
				aeOkChan <- sid
			} else if args.LeaderCurrentTerm >= reply.FollowerCurrentTerm {
				if newArgs, ok := updateAppendEntriesArgsByReply(logs, args, &reply); ok { //try agin when failed
					// fmt.Printf("leader %d: instance %d new args %v current reply %v\n", newArgs.LeaderId, sid, newArgs, reply)
					newReply := AppendEntriesReply{}
					rf.sendAppendEntries(sid, &newArgs, &newReply)
					// fmt.Printf("leader %d: instance %d args %v reply %v\n", newArgs.LeaderId, sid, newArgs, newReply)
					if newReply.Success {
						aeOkChan <- sid
					}
				}
			}
		}(i)
	}
	peersNum := len(rf.peers)
	aeOkMap := make(map[int]int, peersNum-1)
	majority := int(math.Ceil(float64(peersNum+1)/2)) - 1
	timeout := time.NewTimer(time.Duration(2) * HeartsBeatsInterval)
	for len(aeOkMap) < majority {
		select {
		case sid := <-aeOkChan:
			aeOkMap[sid] = 1
			// fmt.Printf("instace %d OK at commit index %d\n", sid, index)
		case <-rf.lostLeaderPositionChan:
			// fmt.Printf("instace %d lost leader position\n", rf.me)
			return nextCommitIndex, false
		case <-timeout.C:
			// fmt.Printf("leader %d timeout \n", rf.me)
			return nextCommitIndex, false
		}

	}
	return nextCommitIndex, true
}

func updateAppendEntriesArgsByReply(logs []Log, currentArgs AppendEntriesArgs, currentReply *AppendEntriesReply) (AppendEntriesArgs, bool) {
	if currentReply.FollowerCurrentTerm > currentArgs.LeaderCurrentTerm {
		return currentArgs, false
	}
	if currentArgs.PrevLogIndex == -1 {
		return currentArgs, true
	}
	if currentReply.FollowerLastIndex > -1 && currentReply.FollowerLastIndex < len(logs) && logs[currentReply.FollowerLastIndex].Term == currentReply.FollowerLastTerm {
		var newEntries []Log
		newEntries = append(newEntries, logs[currentReply.FollowerLastIndex+1:]...)
		currentArgs.Entries = append(newEntries, currentArgs.Entries...)
		currentArgs.PrevLogIndex = currentReply.FollowerLastIndex
		currentArgs.PrevLogTerm = logs[currentReply.FollowerLastIndex].Term
		return currentArgs, true
	}
	index := currentReply.FollowerCommitIndex
	var newEntries []Log
	newEntries = append(newEntries, logs[index:]...)
	currentArgs.Entries = append(newEntries, currentArgs.Entries...)
	currentArgs.PrevLogIndex = currentReply.FollowerCommitIndex - 1
	currentArgs.PrevLogTerm = -1
	if currentArgs.PrevLogIndex > -1 {
		currentArgs.PrevLogTerm = logs[currentArgs.PrevLogIndex].Term
	}
	return currentArgs, true

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
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getRandSleepMilliseconds() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomTime := time.Duration(450+(rand.Int63()%450)) * time.Millisecond
	return randomTime
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
	// fmt.Println("total peers: ", len(peers))
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 0)
	rf.commitIndex = 0
	rf.currentRole = Follower
	rf.votesReceivedFrom = make([]int, 0)
	rf.sentLength = make(map[int]int)
	rf.ackedLength = make(map[int]int)

	//init chan
	rf.appendEntriesChan = make(chan *AppendEntriesArgs, 1)
	rf.requestVoteChan = make(chan *RequestVoteArgs, 1)
	rf.electionHigherTermChan = make(chan HigherTermInfo, 1)
	rf.beatHigherTermChan = make(chan HigherTermInfo)
	rf.voteOkChan = make(chan struct{})
	rf.lostLeaderPositionChan = make(chan struct{})
	rf.applyChangChan = applyCh
	rf.voteFailedChan = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.startServer()

	return rf
}
