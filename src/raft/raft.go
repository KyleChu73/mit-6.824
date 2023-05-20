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

	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/debug"
	"6.824/labrpc"
	"github.com/sasha-s/go-deadlock"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	deadlock.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     State
	onRPCChan chan int

	// Persistent state on all servers:

	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
}

type State int

const (
	followerState State = iota
	candidateState
	leaderState
)

func (s State) String() string {
	var ret string
	switch s {
	case followerState:
		ret = "Follower"
	case candidateState:
		ret = "Candidate"
	case leaderState:
		ret = "Leader"
	}

	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	term = rf.currentTerm
	isleader = rf.state == leaderState
	rf.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// this doesn't hold lock
func (rf *Raft) fmtServerInfo() string {
	return fmt.Sprintf("S%d %s (votedFor=%d)", rf.me, rf.state, rf.votedFor)
}

// example RequestVote RPC handler.
// 作为一个 follower 收到，进行投票
// 作为一个 candidate 收到，
// 作为一个 leader 收到，
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false

	rf.Lock()

	// 1.	Reply false if term < currentTerm (§5.1)
	// 2.	If votedFor is null or candidateId, and candidate’s log is at
	// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	if args.Term >= rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.currentTerm {
			debug.Debug(debug.DTerm, "%s, curTerm %d < term %d",
				rf.fmtServerInfo(), rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
			rf.resetToFollower() // 一定要重置
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			debug.Debug(debug.DVote, "%s, voted to S%d Candidate", rf.fmtServerInfo(), args.CandidateId)
		}
	}

	reply.Term = rf.currentTerm
	rf.Unlock()

	rf.onRPCChan <- 1
}

// this doesn't own lock!
func (rf *Raft) resetToFollower() {
	if rf.state != followerState {
		debug.Debug(debug.DTerm, "%s, convert to follower", rf.fmtServerInfo())
	}
	rf.state = followerState
	rf.votedFor = -1
}

// Invoked by candidates to gather votes (§5.2).
func (rf *Raft) CallRequestVote(target int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[target].Call("Raft.RequestVote", args, reply)
	return ok
}

type WrappedRequestVoteReply struct {
	ok   bool
	from int
	*RequestVoteReply
}

func (rf *Raft) callRequestVoteChan(target int, args *RequestVoteArgs, ch chan *WrappedRequestVoteReply) {
	reply := RequestVoteReply{}
	debug.Debug(debug.DVote, "S%d Candidate, send RequestVote to S%d", args.CandidateId, target)
	ok := rf.peers[target].Call("Raft.RequestVote", args, &reply)
	select {
	case ch <- &WrappedRequestVoteReply{ok: ok, from: target, RequestVoteReply: &reply}:
	default:
		// do not block
	}
}

func (rf *Raft) sendRequestVoteToAll(elected chan<- int, quit <-chan int) {
	grantedVotes := 1
	replyChan := make(chan *WrappedRequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.Lock()
		if rf.state == candidateState {
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			go rf.callRequestVoteChan(i, &args, replyChan)
		} else if rf.state == followerState {
			rf.Unlock()
			return
		}
		rf.Unlock()
	}

	for !rf.killed() {
		var reply *WrappedRequestVoteReply
		select {
		case <-quit:
			//   While waiting for votes, a candidate may receive an
			// AppendEntries RPC from another server claiming to be
			// leader. If the leader’s term (included in its RPC) is at least
			// as large as (>=) the candidate’s current term, then the candidate
			// recognizes the leader as legitimate and returns to follower
			// state.
			// so... I'll become a follower
			// TODO go rf.callRequestVoteChan() leak?
			return

		case reply = <-replyChan:
			// 在下面的过程中，candidate 可能已经恢复成 follower 了
			// （可能已有其他人成为leader，见 AppendEntries），
			// 但是还没来的及 quit。
			// 要时刻注意这个逻辑。
			if reply.ok {
				rf.Lock()
				// 任何时候 term > rf.currentTerm 都要这么做
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.resetToFollower()
					rf.Unlock()
					return
				}
				if reply.VoteGranted {
					grantedVotes++
					debug.Debug(debug.DVote, "%s, received vote from S%d, %d(expected >%d)",
						rf.fmtServerInfo(), reply.from, grantedVotes, len(rf.peers)/2)
					if grantedVotes > len(rf.peers)/2 && rf.state == candidateState {
						select {
						case elected <- 1:
						default:
						}
						rf.Unlock()
						return
					}
				}
				rf.Unlock()
			} else {
				rf.Lock()
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				if rf.state == candidateState {
					go rf.callRequestVoteChan(reply.from, &args, replyChan)
				}
				rf.Unlock()
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones 紧接在新条目之前的日志条目的索引
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// 作为一个 follower 收到：心跳或日志
// 作为一个 condidate 收到：如果是一个更新的 leader，转换成 follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: race?
	rf.Lock()

	// empty for heartbeat
	if args.Entries == nil || len(args.Entries) == 0 {
		debug.Debug(debug.DLog, "%s, received hearbeat from S%d Leader",
			rf.fmtServerInfo(), args.LeaderId)
	} else {
		debug.Debug(debug.DLog2, "%s, received AppendEntries from S%d Leader",
			rf.fmtServerInfo(), args.LeaderId)
	}

	// Receiver implementation:
	// 1.	Reply false if term < currentTerm (§5.1)
	// 2.	Reply false if log doesn’t contain an entry at prevLogIndex
	// 		whose term matches prevLogTerm (§5.3)
	// 3.	If an existing entry conflicts with a new one (same index
	// 		but different terms), delete the existing entry and all that
	// 		follow it (§5.3)
	// 4.	Append any new entries not already in the log
	// 5.	If leaderCommit > commitIndex, set commitIndex =
	// 		min(leaderCommit, index of last new entry)

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if rf.currentTerm < args.Term ||
		// If the leader’s term (included in its RPC) is at least
		// as large as the candidate’s current term, then the candidate
		// recognizes the leader as legitimate and returns to follower
		// state. If the term in the RPC is smaller than the candidate’s
		// current term, then the candidate rejects the RPC and continues in candidate state.
		(rf.state == candidateState && rf.currentTerm == args.Term) {
		debug.Debug(debug.DTerm, "%s, curTerm %d < term %d",
			rf.fmtServerInfo(), rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		if rf.state != followerState {
			debug.Debug(debug.DTerm, "%s, convert to follower", rf.fmtServerInfo())
		}
		rf.state = followerState
		rf.votedFor = -1 // 一定要重置
	}
	rf.Unlock()

	rf.onRPCChan <- 1
}

type WrappedAppendEntriesReply struct {
	ok   bool
	from int
	*AppendEntriesReply
}

func (rf *Raft) callAppendEntriesChan(target int, args *AppendEntriesArgs, ch chan *WrappedAppendEntriesReply) {
	reply := AppendEntriesReply{}
	debug.Debug(debug.DLog, "S%d Leader, send heartbeat to S%d", args.LeaderId, target)
	ok := rf.peers[target].Call("Raft.AppendEntries", args, &reply)
	select {
	case ch <- &WrappedAppendEntriesReply{ok: ok, from: target, AppendEntriesReply: &reply}:
	default:
		// do not block
	}
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) CallAppendEntries(target int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[target].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatToAll(quit <-chan int) {
	replyChan := make(chan *WrappedAppendEntriesReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.Lock()
		if rf.state == leaderState {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				Entries:  nil,
			}
			go rf.callAppendEntriesChan(i, &args, replyChan)
		} else if rf.state == followerState {
			rf.Unlock()
			return
		}
		rf.Unlock()
	}

	for !rf.killed() {
		var reply *WrappedAppendEntriesReply
		select {
		case <-quit:
			return
		case reply = <-replyChan:
			if reply.ok {
				rf.Lock()
				// 任何时候 term > rf.currentTerm 都要这么做
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.resetToFollower()
					rf.Unlock()
					return
				}
				rf.Unlock()
			}
		}
	}
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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

func randomElectionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(250)) * time.Millisecond // TODO: 150ms ~ 300ms ok?
}

const heartbeatInterval = 100 * time.Millisecond

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Go程 ticker 会开始一轮新的选举，如果他最近没有收到心跳（超时）
// 但是要注意，leader 是发出心跳的那方
func (rf *Raft) ticker() {

followerLoop:
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		electionTimeout := randomElectionTimeout()
		rf.Lock()
		debug.Debug(debug.DTimer, "%s, starting election timeout(%d ms)",
			rf.fmtServerInfo(), electionTimeout/time.Millisecond)
		rf.Unlock()
		timer := time.NewTimer(electionTimeout) // ms

	followerReceivingHeartbeat:
		for !rf.killed() {
			select {
			case <-rf.onRPCChan:
				electionTimeout = randomElectionTimeout()
				timer.Reset(electionTimeout)
				rf.Lock()
				debug.Debug(debug.DTimer, "%s, reset election timeout(%d ms)",
					rf.fmtServerInfo(), electionTimeout/time.Millisecond)
				rf.Unlock()
			case <-timer.C:
				rf.Lock()
				debug.Debug(debug.DTimer, "%s, election timeout!", rf.fmtServerInfo())
				rf.Unlock()
				break followerReceivingHeartbeat
			}
		}
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate(Fig. 2)
		rf.Lock()
		debug.Debug(debug.DInfo, "%s, convert to candidate", rf.fmtServerInfo())
		rf.state = candidateState
		rf.Unlock()

	candidateElection:
		for !rf.killed() {
			rf.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			debug.Debug(debug.DInfo, "%s, start election", rf.fmtServerInfo())
			rf.Unlock()
			electionTimeout = randomElectionTimeout()
			timer.Reset(electionTimeout)
			// Send RequestVote RPCs to all other servers
			elect := make(chan int)
			quitReqVotes := make(chan int)
			go rf.sendRequestVoteToAll(elect, quitReqVotes)
			for !rf.killed() {
				select {
				case <-elect:
					break candidateElection

				case <-rf.onRPCChan:
					//   While waiting for votes, a candidate may receive an
					// AppendEntries RPC from another server claiming to be
					// leader. If the leader’s term (included in its RPC) is at least
					// as large as the candidate’s current term, then the candidate
					// recognizes the leader as legitimate and returns to follower
					// state. If the term in the RPC is smaller than the candidate’s
					// current term, then the candidate rejects the RPC and continues
					// in candidate state. (§5.2)
					rf.Lock()
					if rf.state == followerState {
						timer.Stop()
						rf.Unlock()
						quitReqVotes <- 1
						continue followerLoop
					}
					rf.Unlock()
					// ignore

				case <-timer.C:
					// If election timeout elapses: start new election. (Fig. 2)
					// ... When this happens, each candidate will time out
					// and start a new election by incrementing its term
					// and initiating another round of RequestVote RPCs. (§5.2)
					rf.Lock()
					debug.Debug(debug.DTimer, "%s, start new election",
						rf.fmtServerInfo())
					rf.Unlock()
					quitReqVotes <- 1
					continue candidateElection
				}
			}
		}

		rf.Lock()
		rf.state = leaderState
		debug.Debug(debug.DLeader, "S%d become leader in term %d", rf.me, rf.currentTerm)
		rf.Unlock()
		quitHeartbeat := make(chan int)
		rf.Lock()
		debug.Debug(debug.DLog, "%s, first sending heartbeat", rf.fmtServerInfo())
		rf.Unlock()
		go rf.sendHeartbeatToAll(quitHeartbeat)
		timer = time.NewTimer(heartbeatInterval) // 立刻发送心跳
	leaderHeartbeatLoop:
		for !rf.killed() {
			select {
			case <-rf.onRPCChan:
				rf.Lock()
				if rf.state == followerState {
					timer.Stop()
					rf.Unlock()
					quitHeartbeat <- 1
					break leaderHeartbeatLoop
				}
				rf.Unlock()
			case <-timer.C:
				quitHeartbeat <- 1
				rf.Lock()
				debug.Debug(debug.DLog, "%s, sending heartbeat", rf.fmtServerInfo())
				rf.Unlock()
				go rf.sendHeartbeatToAll(quitHeartbeat)
				timer.Reset(heartbeatInterval)
			}
		}
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
	rf.state = followerState

	rf.onRPCChan = make(chan int)

	rf.currentTerm = 0
	rf.votedFor = -1 // null

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
