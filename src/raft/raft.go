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
	"distributed/labgob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "distributed/labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower	int = 0
	Candidate		= 1
	Leader			= 2
)

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

	applyCh   chan  ApplyMsg
	applyCond *sync.Cond

	//current term
	currentTerm      int

	leaderId         int

	//candidate id votes for
	voteFor          int

	//log entries containing command for state machine
	Log   []LogEntry
	nextIndex    []int
	matchIndex   []int

	//index of the highest log entry known to be committed
	commitIndex      int
	//index of the highest log entry known to be applied to state machine
	lastApplied      int

	//timeout to trigger an election or heartbeat
	electionTimeout   int
	heartbeatTimeout  int
	lastElectionTime  int64
	lastHeartbeatTime int64

	electionTimeoutChan   chan bool
	//state of current peer
	state           int

}

//generate a new timeout for election
func (rf *Raft) generateElectionTimeout(){
	rand.Seed(time.Now().UnixNano()+rand.Int63())
	rf.electionTimeout = 150 + rand.Intn(150)
	rf.lastElectionTime = time.Now().UnixNano()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.state==Leader
	return term, isLeader
}

func (rf *Raft)switchStateTo(state int)  {
	rf.state = state
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
	e.Encode(rf.voteFor)
	e.Encode(rf.Log)

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
	var currentTerm  int
	var voteFor      int
	var Log          []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor)!=nil ||
		d.Decode(&Log) !=nil {
		DPrintf("Failed to read persisted data\n")
	}else{
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.Log = Log
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//candidate's term
	Term      int

	//candidate's id
	CandidateId    int

	//index of candidate's last log entry
	LastLogIndex   int
	//term of candidate's last log entry
	LastLogTerm    int

	LastCommit     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Success     bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm < args.Term{
		DPrintf("Candidate %d 's term is greater than current peer %d,switch the peer to follower\n",args.CandidateId,rf.me)
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.switchStateTo(Follower)
		rf.persist()
	}

	if rf.voteFor == -1 || rf.voteFor==args.CandidateId{
		lastLogIndex := len(rf.Log) - 1
		//we can't satisfy rf.Log[lastLogIndex].Term <= args.LastLogTerm || args.LastLogIndex >= lastLogIndex at the same time ,see TestRejoin2B

		//and the candidate's commit index must be greater than the peer's,
		//so that the candidate includes all the committed logs peer owns,
		//testBackup2B introduces a condition where the peer with larger lastLogIndex has smaller commitIndex,if the peer won the election
		//it begins sending AppendEntries,result in duplicating commits
		if (rf.Log[lastLogIndex].Term <= args.LastLogTerm || args.LastLogIndex >= lastLogIndex) && args.LastCommit >= rf.commitIndex{
			rf.voteFor = args.CandidateId
			rf.generateElectionTimeout()
			rf.currentTerm = args.Term
			rf.switchStateTo(Follower)
			rf.persist()
			reply.Success = true
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		}
	}

	reply.Success = false
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	return
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
		if rf.state!=Leader{
			isLeader = false
		}else{
			logEntry := LogEntry{Command: command,Term: rf.currentTerm}
			rf.Log = append(rf.Log,logEntry)
			index = len(rf.Log) - 1
			term = rf.currentTerm
			isLeader = true
			go rf.broadcastEntries()
		}
	rf.mu.Unlock()
	return index, term, isLeader
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
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.leaderId = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.heartbeatTimeout = 120
	rf.Log = make([]LogEntry,0)
	//the real log index starts form 1
	rf.Log = append(rf.Log, LogEntry{Term:0})
	size := len(rf.peers)
	rf.nextIndex = make([]int,size)
	rf.matchIndex = make([]int,size)
	rf.electionTimeoutChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.generateElectionTimeout()
	go rf.triggerTimeoutEvent()
	go rf.electionTimeoutTik()
	go rf.heartbeatTimeoutTik()
	go rf.applyEntries()

	return rf
}

/**
 * fire evens from the channel
 */
func (rf *Raft) triggerTimeoutEvent() {
	for{
		select{
			case <- rf.electionTimeoutChan:
				//send the election requests synchronize
				//wait for the election to be finished to start a new one
				rf.startElection()
		}
	}
}

/**
 * Timer for election
 */
func (rf *Raft) electionTimeoutTik() {
	for{
		rf.mu.Lock()
			if _,isLeader :=rf.GetState();!isLeader{
				elapseTime := time.Now().UnixNano() - rf.lastElectionTime
				if int(elapseTime / int64(time.Millisecond)) >= rf.electionTimeout{
					rf.electionTimeoutChan <- true
				}
			}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond*10)
	}
}

func (rf *Raft) heartbeatTimeoutTik() {
	for{
		time.Sleep(time.Duration(rf.heartbeatTimeout)*time.Millisecond)
		go rf.sendHeartbeat()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if _,isLeader := rf.GetState();isLeader{
		DPrintf("leader %d is not allowed to elect\n",rf.me)
		rf.mu.Unlock()
		return
	}
	//vote for itself
	rf.voteFor = rf.me

	//reset the election timeout
	rf.generateElectionTimeout()
	//switch to candidate state
	rf.switchStateTo(Candidate)
	rf.persist()
	rf.currentTerm += 1
	DPrintf("Starting election,candidate id: %d,candidate term: %d\n",rf.me,rf.currentTerm)
	rf.mu.Unlock()

	votes := 1
	thresholdToWin := len(rf.peers)/2 +1

	rf.mu.Lock()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.Log) - 1
	args.LastLogTerm = rf.Log[args.LastLogIndex].Term
	args.CandidateId = rf.me
	args.LastCommit = rf.commitIndex
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for i :=0;i<len(rf.peers);i++{
		wg.Add(1)
	}

	//send request vote RPC to all the peers
	for i,_ := range rf.peers {
		if i == rf.me {
			wg.Done()
			continue
		}
		go func(i int) {
			//the threshold to win an election
			var reply RequestVoteReply
			//send request to all the peers
			ok := rf.sendRequestVote(i, &args, &reply)
			//the RPC failure
			if !ok {
				DPrintf("Failed to send request to peer %d,current sender: %d\n", i, rf.me)
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != args.Term{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success {
				//the request vote succeed,vote granted
				rf.mu.Lock()
				//check if the candidate is already a leader
				if rf.state == Leader{
					rf.mu.Unlock()
					return
				}
				votes += 1
				DPrintf("Vote granted to candidate,current candidate: %d,current candidate term: %d,current votes: %d,threshold to win %d,voter's term: %d,current role %d\n", rf.me, rf.currentTerm, votes,thresholdToWin, reply.Term,rf.state)
				if votes >= thresholdToWin{
					if rf.state == Candidate{
						DPrintf("Candidate %d won the election with %d/%d votes\n",rf.me,votes,len(rf.peers))
						rf.switchStateTo(Leader)
						rf.persist()
						rf.leaderId = rf.me
						for i,_ := range rf.peers{
							rf.nextIndex[i] = len(rf.Log)
							rf.matchIndex[i] = 0
						}
						go rf.sendHeartbeat()
					}
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("Failed to elect,the candidate id: %d,the candidate's term is less than peer's\n", rf.me)
					//the candidate's term is less than the peer's,the peer rejected it
					rf.switchStateTo(Follower)
					rf.persist()
					rf.voteFor = -1
					rf.currentTerm = reply.Term
					rf.generateElectionTimeout()
				}
				rf.mu.Unlock()
			}
		}(i)
		wg.Done()
	}
	wg.Wait()
}

type AppendEntriesArgs struct {
	//leader's term
	Term   int
	//leader's id
	LeaderId   int
	//preLogIndex
	PrevLogIndex  int
	PrevLogTerm   int
	Entries    []LogEntry
	LeaderCommit   int
}

type AppendEntriesApply struct {
	Success   bool
	Term      int
}

//called by the peers to append entries
func (rf *Raft)AppendEntries(args AppendEntriesArgs,reply *AppendEntriesApply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm{
		DPrintf("Reject the vote request from candidate %d,candidate's term is %d,current peer's term is %d\n",rf.me,args.Term,rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	entries := make([]LogEntry,len(args.Entries))
	copy(entries,args.Entries)

	rf.mu.Lock()
	if args.Term > rf.currentTerm{
		rf.switchStateTo(Follower)
		rf.persist()
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.generateElectionTimeout()
	}

	if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.Log = append(rf.Log[:args.PrevLogIndex+1],entries...)
		DPrintf("Appending entries,current leader id :%d,current term: %d\n",args.LeaderId,args.Term)
		rf.switchStateTo(Follower)
		rf.persist()
		rf.generateElectionTimeout()
		rf.leaderId = args.LeaderId

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.Log) - 1{
				rf.commitIndex = len(rf.Log) - 1
			}else{
				rf.commitIndex = args.LeaderCommit
			}
			DPrintf("Peer %d commit index %d\n",rf.me,rf.commitIndex)
			rf.applyCond.Broadcast()
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	DPrintf("Consistency check failed,length of peer's: %d,prevLogIndex of leader's: %d\n",len(rf.Log),args.PrevLogTerm)
	reply.Success = false
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) SendAppendEntriesRequest(server int, args AppendEntriesArgs, reply *AppendEntriesApply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.BroadcastAppendEntries(true)
}

func (rf *Raft) broadcastEntries() {
	rf.BroadcastAppendEntries(false)
}

//append entries called by the leader
func (rf *Raft) BroadcastAppendEntries(isHeartbeat bool) {
	rf.mu.Lock()
	if _,isLeader := rf.GetState();!isLeader{
		rf.mu.Unlock()
		return
	}

	//majority := len(rf.Log) /2 +1
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	rf.mu.Unlock()

	committed := false
	nReplicated := 1

	var wg sync.WaitGroup
	for i :=0;i<len(rf.peers);i++{
		wg.Add(1)
	}

	for i,_ := range rf.peers{
		if i==rf.me{
			wg.Done()
			continue
		}
		go func(i int,committed *bool,nReplicated *int) {
		retry:
			rf.mu.Lock()
			if isHeartbeat{
				DPrintf("Sending heartbeat to peer %d,current leader %d,current term %d\n",i,rf.me,rf.currentTerm)
			}else{
				DPrintf("Sending append entries to peer %d,current leader %d,current term %d\n",i,rf.me,rf.currentTerm)
			}
			args.PrevLogIndex = rf.nextIndex[i]	- 1
			args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			args.Entries = make([]LogEntry,0)
			for i := rf.nextIndex[i]; i < len(rf.Log); i++ {
				args.Entries = append(args.Entries, rf.Log[i])
			}
			rf.mu.Unlock()

			var reply AppendEntriesApply
			ok := rf.SendAppendEntriesRequest(i,args,&reply)
			if !ok{
				DPrintf("Failed to send RPC request to peer %d,current sender: %d\n",i,rf.me)
				return
			}

			rf.mu.Lock()
			if args.Term != rf.currentTerm{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success{
				if isHeartbeat{
					DPrintf("Sent heartbeat to peer %d successfully,current leader %d\n",i,rf.me)
				}else{
					DPrintf("Send append entries to peer %d successfully,current leader %d\n",i,rf.me)
				}
				rf.mu.Lock()
				if !isHeartbeat && rf.state == Leader{
					*nReplicated += 1
					//rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
					rf.nextIndex[i] = len(rf.Log)
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					if !*committed && *nReplicated >= len(rf.peers)/ 2 +1{
						index := len(rf.Log) - 1
						if index> rf.commitIndex && rf.currentTerm == rf.Log[index].Term{
							rf.commitIndex = index
							DPrintf("Leader %d commit index %d\n",rf.me,rf.commitIndex)
							go rf.broadcastEntries()
							rf.applyCond.Broadcast()
						}
					}
				}
				rf.mu.Unlock()
			}else{
				rf.mu.Lock()
				if rf.state !=Leader{
					rf.mu.Unlock()
					return
				}
				if isHeartbeat{
					DPrintf("Sending heartbeat failed,leader's term: %d,reply's term: %d\n",rf.currentTerm,reply.Term)
				}else {
					DPrintf("Appending entries failed,leader's term: %d,reply's term: %d\n", rf.currentTerm, reply.Term)
				}
				if rf.currentTerm < reply.Term{
					DPrintf("Peer's term greater than leader's,switch to follower.peer's term: %d,leader's term: %d\n",reply.Term,rf.currentTerm)
					rf.switchStateTo(Follower)
					rf.persist()
					rf.voteFor = -1
					rf.generateElectionTimeout()
				}else{
					DPrintf("Peer's log is conflict with leaders,decrease the nextIndex to %d and try again\n",rf.nextIndex[i]-1)
					rf.nextIndex[i] -= 1
					rf.matchIndex[i] -= 1
					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			}
		}(i,&committed,&nReplicated)
		wg.Done()
	}
	wg.Wait()
}

/**
 * loop event for applying entries
 */
func (rf *Raft) applyEntries() {
	for{
		rf.mu.Lock()
		if rf.lastApplied==rf.commitIndex{
			rf.applyCond.Wait()
		}else{
			DPrintf("Starting apply message to log\n")
			for i := rf.lastApplied + 1;i <= rf.commitIndex;i++{
				applyMsg := ApplyMsg{CommandValid: true,Command: rf.Log[i].Command,CommandIndex: i}
				rf.lastApplied = i
				rf.applyCh <- applyMsg
			}
		}
		rf.mu.Unlock()
	}
}
