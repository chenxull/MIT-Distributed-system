package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(peers, me, persister, applyCh)
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
	"math/rand"
	"sync"
	"time"

	"github.com/chenxull/MIT/6.824/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry is log entry
type LogEntry struct {
	Index   int
	Command interface{}
}

// 使用数字来代表服务器的身份
const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             int           // follower, candidate or leader
	resetTimer        chan struct{} //for reset election timer
	electionTimer     *time.Timer   //选举超时计时器
	electionTimeout   time.Duration //选举超时时间周期
	heartbeatInterval time.Duration //心跳发送间隔
	// Persistent state on all server ,update on stable storage before responding to RPGs
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	applyCh    chan ApplyMsg // outgoing channel to service
	shutdownCh chan struct{} // 关闭通道
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true

	}
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateID  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//AppendEntryArgs Log Replication and HeartBeat
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//AppendEntryReply is
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// args是候选人投票请求参数，rf 是接收到投票请求的服务器实例
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.state = Follower
			rf.VoteFor = -1
			rf.CurrentTerm = args.Term
		}

		//TODO figure2中的2还没有实现
		if rf.VoteFor == -1 {
			//TODO 没有判断candidate 是否as up-to-data as receiver's
			rf.resetTimer <- struct{}{}
			rf.VoteFor = args.CandidateID
			rf.state = Follower
			reply.VoteGranted = true
			DPrintf("[%d]:peer %d vote to peer %d", rf.me, rf.me, args.CandidateID)
		}
	}

}

//AppendEntries is
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
	}
	if rf.state == Leader {
		rf.turnToFollower()
	}

	//TODO 2A
	reply.Success = true
	reply.Term = args.Term
	rf.resetTimer <- struct{}{}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.shutdownCh)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.VoteFor = -1
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{
		Index:   1,
		Command: nil,
	}

	rf.nextIndex = make([]int, 1)
	rf.matchIndex = make([]int, 1)

	rf.electionTimeout = time.Millisecond * time.Duration(150+rand.Intn(100)*2) // 在150到300ms 之间
	rf.electionTimer = time.NewTimer(rf.electionTimeout)                        //创建定时器
	rf.resetTimer = make(chan struct{})                                         //用来接收重置定时器请求
	rf.shutdownCh = make(chan struct{})                                         //用来关闭此服务器
	rf.heartbeatInterval = time.Millisecond * 40
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionDaemon()

	return rf
}

//监听定时器的变化，执行不同的操作
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdownCh:
			return
		case <-rf.resetTimer: //重置定时器 接收器
			if !rf.electionTimer.Stop() { //每次调用 stop 需要判断返回值，如果返回 false 表示：stop 失败，即 timer 以及在 stop 前到期。在 raft 表示服务器获得选举权
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout) //重置定时器
		case <-rf.electionTimer.C:
			DPrintf("[%d]: peer %d election timeout, issue election @ term %d\n", rf.me, rf.me, rf.CurrentTerm)
			//TODO :candidate 拉取选票,创建一个协程来执行这个逻辑
			go rf.canvassVotes()
			rf.electionTimer.Reset(rf.electionTimeout)
		}
	}
}

// candidate 拉取选票
func (rf *Raft) canvassVotes() {
	//构造需要发送的数据
	var voteArgs RequestVoteArgs
	rf.buildVoteArgs(&voteArgs)
	peers := len(rf.peers)
	vote := 1
	//定义 reply 匿名处理函数：1.如果有新的 leader， 自身转变为 follower，定时器重置 2.获得票数大于等于大多数，成为 leader，发送心跳
	replyHandler := func(reply *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			if reply.Term > voteArgs.Term { // 收到来 leader 的回复,自身变为 follower
				rf.CurrentTerm = reply.Term
				rf.turnToFollower()
				rf.resetTimer <- struct{}{} //发送信息给定时器，重置
				return
			}
			if reply.VoteGranted {
				if vote == peers/2 { //票数大于等于一半，成为 leader
					rf.state = Leader
					go rf.heartbeatDaemon() //向所有 follower 发送心跳信息
					DPrintf("[%d]: peer %d become leader , issue election @ term %d\n", rf.me, rf.me, rf.CurrentTerm)

				}
				vote++
			}
		}
	}
	// 发送 RequestVote给每一个 peers
	for i := 0; i < peers; i++ {
		if i != rf.me {
			go func(n int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(n, &voteArgs, &reply) {
					replyHandler(&reply)
				}
			}(i)
		}
	}
}

// 构造投票请求参数
func (rf *Raft) buildVoteArgs(args *RequestVoteArgs) {
	// 防止在修改的过程中，其他 goroutine 访问其中的数据
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 身份转变为 candidate
	rf.state = Candidate
	rf.VoteFor = rf.me
	rf.CurrentTerm = rf.CurrentTerm + 1

	// 构造投票参数
	args.CandidateID = rf.me
	args.Term = rf.CurrentTerm
	//TODO LastLogIndex，LastLogTerm 如何构建

}

// 身份转变为 follower
func (rf *Raft) turnToFollower() {
	rf.state = Follower
	rf.VoteFor = -1
}

// 发送心跳,只有 leader 才可以使用
func (rf *Raft) heartbeatDaemon() {
	for {
		if _, isleader := rf.GetState(); !isleader {
			return
		}
		rf.resetTimer <- struct{}{}
		select {
		case <-rf.shutdownCh:
			return
		default:
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					//发送 heatbeatDaemon
					go rf.consensusCheck(i)
				}
			}
		}
	}
}

// 一致性检查，并发送AppendEntries
func (rf *Raft) consensusCheck(n int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 构造  AppendEntries(RPCs with no log entries)
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: -1, //暂时为空
		PrevLogTerm:  -1,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	//将参数发送给follower 服务器,并处理返回的结果
	go func() {
		var reply AppendEntriesReply
		if rf.sendAppendEntries(n, &args, &reply) {
			rf.consensusCheckReplyHandle(n, &reply)
			//fmt.Println("DEBUG")
		}
	}()
}

// 一致性检查 reply
func (rf *Raft) consensusCheckReplyHandle(n int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	if reply.Success {
		//TODO一致性检查成功,日志相关处理工作

	} else { //收到的回复失败,leader变成 follower,重置定时器
		if rf.state == Leader && reply.Term > rf.CurrentTerm {
			rf.turnToFollower()
			rf.resetTimer <- struct{}{}
			DPrintf("[%d]: leader %d found new term (heartbeat resp from peer %d), turn to follower.",
				rf.me, rf.me, n)
			return
		}
	}
}
