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
	"6.824/debug"
	"6.824/labgob"
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

func init() {
	labgob.Register(LogEntry{})
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
	state        State
	onRPCChan    chan int
	appMsgBuffer []*ApplyMsg
	appMsgCond   *sync.Cond

	// Persistent state on all servers:
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
// this function doesn't hold lock!
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatalf("readPersist: decode failed\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
	}
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
func fmtLogs(logs []LogEntry, start int) string {
	ret := "["
	for i, log := range logs {
		cmd := fmt.Sprintf("%v", log.Command)
		l := len(cmd)
		if l > 3 {
			cmd = cmd[:3]
			cmd += "…"
		}
		ret += fmt.Sprintf("(%d,%d)", start, log.Term)
		if i != len(logs)-1 {
			ret += ","
		}
		start++
	}
	ret += "]"

	return ret
}

// this doesn't hold lock
func (rf *Raft) fmtServerInfo() string {
	return fmt.Sprintf("S%d %s (votedFor=%d,term=%d,logs=%s)",
		rf.me, rf.state, rf.votedFor, rf.currentTerm, fmtLogs(rf.log[1:], 1))
}

type logSlot struct {
	term     int
	logIndex int
}

func (l logSlot) String() string {
	return fmt.Sprintf("(%d,%d)", l.logIndex, l.term)
}

func (a logSlot) newerThan(b *logSlot) bool {
	if a.term != b.term {
		return a.term > b.term
	}
	return a.logIndex > b.logIndex
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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Unlock()
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, "%s, curTerm %d < term %d",
			rf.fmtServerInfo(), rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.resetToFollower() // 一定要重置
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		candidateLog := logSlot{args.LastLogTerm, args.LastLogIndex}
		receiverLog := logSlot{rf.log[len(rf.log)-1].Term, len(rf.log) - 1}
		if !(receiverLog.newerThan(&candidateLog)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			debug.Debug(debug.DVote, "%s, voted to S%d Candidate", rf.fmtServerInfo(), args.CandidateId)
		} else {
			debug.Debug(debug.DVote, "%s, election restriction on S%d: candidate%s;receiver%s",
				rf.fmtServerInfo(), args.CandidateId, candidateLog, receiverLog)
		}
	}

	reply.Term = rf.currentTerm
	rf.Unlock()

	rf.onRPCChan <- 1
}

// this doesn't own lock!
func (rf *Raft) resetToFollower() {
	// if rf.state != followerState {
	// 	debug.Debug(debug.DTerm, "%s, convert to follower", rf.fmtServerInfo())
	// }
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

func (rf *Raft) sendRequestVoteToAll(elected chan<- int, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	grantedVotes := 1
	replyChan := make(chan *WrappedRequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.Lock()
		if rf.state == candidateState {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			go rf.callRequestVoteChan(i, &args, replyChan)
		} else if rf.state == followerState {
			rf.Unlock()
			return
		}
		rf.Unlock()
	}

	for !rf.killed() {
		rf.Lock()
		if rf.state != candidateState {
			rf.Unlock()
			break
		}
		rf.Unlock()
		var reply *WrappedRequestVoteReply
		select {
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
				if reply.VoteGranted {
					grantedVotes++
					debug.Debug(debug.DVote, "%s, received vote from S%d, %d(expected >%d)",
						rf.fmtServerInfo(), reply.from, grantedVotes, len(rf.peers)/2)
					if grantedVotes > len(rf.peers)/2 && rf.state == candidateState {
						rf.Unlock()
						elected <- 1
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
		case <-timer.C:
			// 避免 goroutine 泄露
			return
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

	// fast backup
	XTerm  int // Follower 中与 Leader 冲突的 Log 对应的任期号，Leader 会在 prevLogTerm 中带上本地 Log 记录中，前一条Log的任期号。如果Follower在对应位置的任期号不匹配，它会拒绝Leader的AppendEntries消息，并将自己的任期号放在XTerm中。如果Follower在对应位置没有Log，那么这里会返回 -1。
	XIndex int // Follower 中，对应任期号为 XTerm 的第一条 Log 条目的槽位号。
	XLen   int // 如果 Follower 在对应位置没有 Log，那么 XTerm 会返回 -1，XLen 表示空白的 Log 槽位数。
}

// 作为一个 follower 收到：心跳或日志
// 作为一个 condidate 收到：如果是一个更新的 leader，转换成 follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()

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

	reply.Term = rf.currentTerm

	// If a server receives a request with a stale term
	// number, it rejects the request. (§5.1)
	if args.Term < rf.currentTerm {
		rf.Unlock()
		reply.Success = false
		return
	}

	// empty for heartbeat
	if args.Entries == nil || len(args.Entries) == 0 {
		debug.Debug(debug.DLog, "%s, received heartbeat from S%d Leader(term %d)",
			rf.fmtServerInfo(), args.LeaderId, args.Term)
	} else {
		debug.Debug(debug.DLog2, "%s, received AppendEntries from S%d Leader(term %d), prev(%d,%d), entries:%s",
			rf.fmtServerInfo(), args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, fmtLogs(args.Entries, args.PrevLogIndex+1))
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, "%s, curTerm %d < term %d",
			rf.fmtServerInfo(), rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		if rf.state != followerState {
			debug.Debug(debug.DTerm, "%s, convert to follower", rf.fmtServerInfo())
		}
		rf.resetToFollower()
	}

	// If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower
	// state. If the term in the RPC is smaller than the candidate’s
	// current term, then the candidate rejects the RPC and continues
	// in candidate state. (§5.2)
	if rf.state == candidateState && args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		debug.Debug(debug.DTerm, "%s, convert to follower", rf.fmtServerInfo())
		rf.resetToFollower()
	}

	// 2.	Reply false if log doesn’t contain an entry at prevLogIndex
	// 		whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		debug.Debug(debug.DTrace, "%s, in AE, none log matches (prevLogIndex,PrevLogTerm)=(%d,%d)",
			rf.fmtServerInfo(), args.PrevLogIndex, args.PrevLogTerm)
		if args.PrevLogIndex >= len(rf.log) {
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex - len(rf.log) + 1
		} else {
			XTerm := rf.log[args.PrevLogIndex].Term
			reply.XTerm = XTerm
			l, r := 0, len(rf.log)-1
			for l < r {
				mid := l + (r-l)/2
				if rf.log[mid].Term >= XTerm {
					r = mid
				} else {
					l = mid + 1
				}
			}
			if rf.log[l].Term != XTerm {
				log.Fatalf("AppendEntries: bin search error!\n")
			}
			reply.XIndex = l
		}
		debug.Debug(debug.DLog2, "%s, fast backup: XTerm=%d, XIndex=%d, XLen=%d",
			rf.fmtServerInfo(), reply.XTerm, reply.XIndex, reply.XLen)
		rf.Unlock()
		reply.Success = false

		rf.onRPCChan <- 1
		return
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		debug.Debug(debug.DTrace, "%s, in AE, prevLogIndex%d,PrevLogTerm%d matched",
			rf.fmtServerInfo(), args.PrevLogIndex, args.PrevLogTerm)
	}

	// 3.	If an existing entry conflicts with a new one (same index
	// 		but different terms), delete the existing entry and all that
	// 		follow it (§5.3)
	// 现在可以确定 rf.log[prevLogIdx] == args.prevLogTerm
	i := 0
	j := args.PrevLogIndex + 1
	for i < len(args.Entries) && j < len(rf.log) {
		if rf.log[j].Term != args.Entries[i].Term {
			rf.log = rf.log[0:j]
			debug.Debug(debug.DDrop, "%s, dropped rf.log[%d:]", rf.fmtServerInfo(), j)
			break
		}
		i++
		j++
	}

	// 4.	Append any new entries not already in the log
	if i < len(args.Entries) {
		st := len(rf.log)
		rf.log = append(rf.log, args.Entries[i:]...)
		debug.Debug(debug.DLog2, "%s appended, where new entries start at %d",
			rf.fmtServerInfo(), st)
	}

	// 5.	If leaderCommit > commitIndex, set commitIndex =
	// 		min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newEnt := len(rf.log) - 1
		if args.LeaderCommit < newEnt {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = newEnt
		}
		debug.Debug(debug.DLog2, "%s, set commitIdx = %d", rf.fmtServerInfo(), rf.commitIndex)
		// If commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine (§5.3) (Fig. 2)

		rf.updateLastApplied()
	}

	rf.Unlock()

	reply.Success = true

	rf.onRPCChan <- 1
}

type WrappedAppendEntriesReply struct {
	ok    bool
	from  int
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (rf *Raft) callAppendEntriesChan(target int, args *AppendEntriesArgs, ch chan *WrappedAppendEntriesReply) {
	reply := AppendEntriesReply{}
	ok := rf.peers[target].Call("Raft.AppendEntries", args, &reply)
	select {
	case ch <- &WrappedAppendEntriesReply{
		ok:    ok,
		from:  target,
		args:  args,
		reply: &reply}:
	default:
		// do not block
	}
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) CallAppendEntries(target int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[target].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatToAll() {
	timer := time.NewTimer(heartbeatInterval)
	replyChan := make(chan *WrappedAppendEntriesReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 注意：leader 可能在发送心跳期间转变为 follower，若如此，它无权再发送心跳。
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.Lock()
		if rf.state == leaderState {
			prevLogIndex := len(rf.log) - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      nil,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			go rf.callAppendEntriesChan(i, &args, replyChan)
		} else if rf.state == followerState {
			rf.Unlock()
			return
		}
		rf.Unlock()
	}

	for !rf.killed() {
		rf.Lock()
		if rf.state != leaderState {
			rf.Unlock()
			break
		}
		rf.Unlock()
		var wrReply *WrappedAppendEntriesReply
		select {
		case wrReply = <-replyChan:
			reply := wrReply.reply
			if wrReply.ok {
				rf.Lock()
				// 任何时候 term > rf.currentTerm 都要这么做
				if reply.Term > rf.currentTerm {
					debug.Debug(debug.DLeader, "%s, reply term(%d) > curTerm, convert to follower",
						rf.fmtServerInfo(), reply.Term)
					rf.currentTerm = reply.Term
					rf.resetToFollower()
					rf.Unlock()
					return
				}

				// 说明 prevLogIndex,prevLogTerm 不匹配
				// 这里可以选择发送 AE
				if !reply.Success && rf.state == leaderState {
					// if rf.nextIndex[wrReply.from] > 1 {
					// 	debug.Debug(debug.DLog2, "%s, nextIndex[%d]--", rf.fmtServerInfo(), wrReply.from)
					// 	rf.nextIndex[wrReply.from]--
					// }
					rf.updateNextIndex(wrReply)
					go rf.callAppendEntriesChan(wrReply.from, rf.getAppendEntriesArgs(wrReply.from), replyChan)
				}
				rf.Unlock()
			}
		case <-timer.C:
			return
		}
	}
}

// this doesn't hold lock!
func (rf *Raft) getAppendEntriesArgs(target int) *AppendEntriesArgs {
	// nextIndex：要发给这个服务器的下一条 log
	// prevLogIndex：紧接在新条目之前的 log
	// 例子：s2 要给 s1 发 4
	// 所以 nextIdx=4
	//         v prevLogIndex
	//           v nextIdx
	//     1 2 3 4
	// s1: 1 1 1
	// s2: 1 1 1 2
	//           ^ new log
	//          ^ become leader
	prevLogIndex := rf.nextIndex[target] - 1 // 要发送（新的） log 的前一个
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := make([]LogEntry, len(rf.log)-rf.nextIndex[target])
	copy(entries, rf.log[rf.nextIndex[target]:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}

	return &args
}

func (rf *Raft) sendAppendEntriesToall() {
	// TODO 用 heartbeat 周期作为超时时间合理吗
	timer := time.NewTimer(heartbeatInterval) // 防止无限的阻塞
	replyChan := make(chan *WrappedAppendEntriesReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 注意：leader 可能在此期间转变为 follower，若如此，它无权再发送 AE。
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.Lock()
		if rf.state == leaderState {
			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex (Fig. 2)
			if len(rf.log)-1 >= rf.nextIndex[i] {
				// debug.Debug(debug.DLog, "%s, last log index(%d) >= nextIndex(%d) for S%d",
				// 	rf.fmtServerInfo(), len(rf.log)-1, rf.nextIndex[i], i)
				go rf.callAppendEntriesChan(i, rf.getAppendEntriesArgs(i), replyChan)
			} else {
				debug.Debug(debug.DLog, "%s, last log index(%d) < nextIndex(%d) for S%d",
					rf.fmtServerInfo(), len(rf.log)-1, rf.nextIndex[i], i)
			}
		} else if rf.state == followerState {
			rf.Unlock()
			return
		}
		rf.Unlock()
	}

	replyCnt := 0
	for !rf.killed() {
		rf.Lock()
		// 可能 leader 已经转变为 follower，或者已经收到所有回复
		if (rf.state != leaderState) || (replyCnt >= len(rf.peers)-1) {
			rf.Unlock()
			break
		}
		rf.Unlock()
		var wrReply *WrappedAppendEntriesReply
		select {
		case wrReply = <-replyChan:
			if wrReply.ok {
				reply := wrReply.reply
				rf.Lock()
				// 任何时候 term > rf.currentTerm 都要这么做
				if reply.Term > rf.currentTerm {
					debug.Debug(debug.DLeader, "%s, reply term(%d) > curTerm, convert to follower",
						rf.fmtServerInfo(), reply.Term)
					rf.currentTerm = reply.Term
					rf.resetToFollower()
					rf.Unlock()
					return
				}
				// 只有 leader 才有以下行为
				if rf.state == leaderState {
					if reply.Success {
						replyCnt++ // TODO rpc 成功还是 reply.success ？
						debug.Debug(debug.DInfo, "%s, AE reply.Success=true", rf.fmtServerInfo())
						// If successful: update nextIndex and matchIndex for
						// follower (§5.3) (Fig. 2)
						target := wrReply.from
						rf.nextIndex[target] = wrReply.args.PrevLogIndex + len(wrReply.args.Entries) + 1
						rf.matchIndex[target] = wrReply.args.PrevLogIndex + len(wrReply.args.Entries)
						// If there exists an N such that N > commitIndex, a majority
						// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
						// set commitIndex = N (§5.3, §5.4) (Fig. 2)
						for N := rf.commitIndex + 1; N < len(rf.log); N++ {
							if rf.log[N].Term == rf.currentTerm {
								counter := 0
								for i := 0; i < len(rf.peers); i++ {
									if rf.matchIndex[i] >= N {
										counter++
									}
								}
								if counter > len(rf.peers)/2 {
									rf.commitIndex = N
									debug.Debug(debug.DLog2, "%s, set commitIdx = %d", rf.fmtServerInfo(), N)
									// If commitIndex > lastApplied: increment lastApplied, apply
									// log[lastApplied] to state machine (§5.3) (Fig. 2)
									rf.updateLastApplied()
								}
							}
						}
					} else {
						// If AppendEntries fails because of log inconsistency:
						// decrement nextIndex and retry (§5.3)
						// 注意首个 log index 为 1
						// if rf.nextIndex[wrReply.from] > 1 {
						// 	debug.Debug(debug.DLog2, "%s, nextIndex[%d]--", rf.fmtServerInfo(), wrReply.from)
						// 	rf.nextIndex[wrReply.from]--
						// }
						rf.updateNextIndex(wrReply)
						go rf.callAppendEntriesChan(wrReply.from, rf.getAppendEntriesArgs(wrReply.from), replyChan)
					}
				}
				rf.Unlock()
			} else {
				// rpc 失败，重试
				rf.Lock()
				// 时刻注意 leader 可能已转为 follower
				if rf.state == leaderState {
					// wrReply.args or rf.getAppendEntriesArgs ?
					go rf.callAppendEntriesChan(wrReply.from, rf.getAppendEntriesArgs(wrReply.from), replyChan)
				}
				rf.Unlock()
			}
		case <-timer.C:
			if !timer.Reset(heartbeatInterval) {
				timer = time.NewTimer(heartbeatInterval)
			}
		}
	}
}

// this function doesn't hold lock!
func (rf *Raft) updateNextIndex(wrReply *WrappedAppendEntriesReply) {
	oldIdx := rf.nextIndex[wrReply.from]
	reply := wrReply.reply
	if reply.XTerm == -1 {
		rf.nextIndex[wrReply.from] -= reply.XLen
		if rf.nextIndex[wrReply.from] < 1 {
			rf.nextIndex[wrReply.from] = 1
		}
	} else {
		if rf.log[reply.XIndex].Term != reply.XTerm {
			rf.nextIndex[wrReply.from] = reply.XIndex
		} else {
			rf.nextIndex[wrReply.from] = reply.XIndex + 1
		}
	}
	newIdx := rf.nextIndex[wrReply.from]
	debug.Debug(debug.DLog2, "S%d, nextIndex[%d]: %d -> %d",
		rf.me, wrReply.from, oldIdx, newIdx)
}

// this doesn't hold lock!
func (rf *Raft) updateLastApplied() {
	if rf.commitIndex > rf.lastApplied {
		// 上次提交的下一个 log 到 commitIdx
		applyLogs := rf.log[rf.lastApplied+1 : rf.commitIndex+1]
		for i, ent := range applyLogs {
			appMsg := ApplyMsg{
				CommandValid: true,
				Command:      ent.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
			rf.appMsgBuffer = append(rf.appMsgBuffer, &appMsg)
		}
		debug.Debug(debug.DTrace, "%s, appMsg appended", rf.fmtServerInfo())
		rf.lastApplied = rf.commitIndex
		rf.appMsgCond.Signal()
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
	rf.Lock()
	isLeader = rf.state == leaderState
	term = rf.currentTerm
	if isLeader {
		// Leaders:
		// - If command received from client: append entry to local log,
		//   respond after entry applied to state machine (§5.3)
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.nextIndex[rf.me] = len(rf.log)
		debug.Debug(debug.DLog2, "%s, received command, sending AE to all", rf.fmtServerInfo())
		go rf.sendAppendEntriesToall()
	}
	// 例如原来 leader 的 log 槽位：0 1 2 3 (len 4)
	// 增加后变为：0 1 2 3 4 (len 5)
	index = len(rf.log) - 1
	rf.Unlock()

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
				if !timer.Reset(electionTimeout) {
					timer = time.NewTimer(electionTimeout)
				}
				rf.Lock()
				debug.Debug(debug.DTimer, "%s, reset election timeout(%d ms)",
					rf.fmtServerInfo(), electionTimeout/time.Millisecond)
				rf.Unlock()
			case <-timer.C:
				rf.Lock()
				debug.Debug(debug.DTimer, "%s, election timeout!", rf.fmtServerInfo())
				debug.Debug(debug.DInfo, "%s, converting to candidate", rf.fmtServerInfo())
				rf.Unlock()
				break followerReceivingHeartbeat
			}
		}
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate(Fig. 2)

	candidateElection:
		for !rf.killed() {
			rf.Lock()
			rf.state = candidateState
			rf.currentTerm++
			rf.votedFor = rf.me
			debug.Debug(debug.DInfo, "%s, start election", rf.fmtServerInfo())
			rf.Unlock()
			electionTimeout = randomElectionTimeout()
			if !timer.Reset(electionTimeout) {
				timer = time.NewTimer(electionTimeout)
			}
			// Send RequestVote RPCs to all other servers
			elect := make(chan int)
			go rf.sendRequestVoteToAll(elect, electionTimeout)
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
					if rf.state == followerState {
						rf.Unlock()
						continue followerLoop
					}
					rf.Unlock()
					continue candidateElection
				}
			}
		}

		rf.Lock()
		// leader 只能由 candidate 转变而来
		if rf.state != candidateState {
			rf.Unlock()
			continue followerLoop
		}
		rf.state = leaderState
		debug.Debug(debug.DLeader, "S%d become leader in term %d", rf.me, rf.currentTerm)
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.Unlock()
		timer = time.NewTimer(0) // 立刻发送心跳
	leaderHeartbeatLoop:
		for !rf.killed() {
			select {
			case <-rf.onRPCChan:
				rf.Lock()
				if rf.state == followerState {
					timer.Stop()
					debug.Debug(debug.DLeader, "%s, convert from leader, exiting", rf.fmtServerInfo())
					rf.Unlock()
					break leaderHeartbeatLoop
				}
				rf.Unlock()
			case <-timer.C:
				rf.Lock()
				if rf.state == leaderState {
					debug.Debug(debug.DLog, "%s, sending heartbeat to all", rf.fmtServerInfo())
					go rf.sendHeartbeatToAll()
				}
				rf.Unlock()
				if !timer.Reset(heartbeatInterval) {
					timer = time.NewTimer(heartbeatInterval)
				}
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
	rf.appMsgBuffer = make([]*ApplyMsg, 0)
	rf.appMsgCond = sync.NewCond(&rf.Mutex)
	go func() {
		for !rf.killed() {
			rf.Lock()
			for len(rf.appMsgBuffer) == 0 {
				rf.appMsgCond.Wait()
			}
			// non-empty
			msg := rf.appMsgBuffer[0]
			rf.appMsgBuffer = rf.appMsgBuffer[1:]
			debug.Debug(debug.DTrace, "%s, wrote applyCh", rf.fmtServerInfo())
			rf.Unlock()
			applyCh <- *msg
		}
	}()

	rf.currentTerm = 0
	rf.votedFor = -1 // null

	rf.log = make([]LogEntry, 1) // first index is 1
	rf.log[0] = LogEntry{
		Term:    0,
		Command: nil,
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = nil
	rf.matchIndex = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
