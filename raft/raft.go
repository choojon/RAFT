package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	currentTerm   int
	votedFor      int
	commitIndex   int
	lastLogIndex  int
	lastLogTerm   int
	lastApplied   int
	commitedamt   int
	nextIndex     []int
	matchIndex    []int
	position      string
	lastHeartbeat time.Time
	timeout       int
	votes         int
	replies       int
	logs          []LogEntry
	applyChannel  chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	// Your code here (4A).
	if rf.position == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//rf.mu.Lock()
	e.Encode(rf.logs)
	//e.Encode(rf.currentTerm)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.votedFor)
	e.Encode(rf.nextIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.commitedamt)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//rf.mu.Unlock()
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
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
	var logs []LogEntry
	var currentTerm int
	var lastLogIndex int
	var votedFor int
	var nextIndex []int
	var commitIndex int
	var commitedamt int
	rf.mu.Lock()
	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil ||
		d.Decode(&lastLogIndex) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&nextIndex) != nil || d.Decode(&commitIndex) != nil || d.Decode(&commitedamt) != nil {
		//fmt.Println("SDS")
	} else {
		rf.commitIndex = commitIndex
		rf.logs = logs
		//rf.currentTerm = currentTerm
		//rf.lastLogIndex = len(logs) - 1
		rf.votedFor = votedFor
		rf.lastLogIndex = len(logs) - 1
		//rf.nextIndex = nextIndex
		//rf.commitedamt = commitedamt
		//rf.lastHeartbeat = time.Now()
	}
	fmt.Println()
	fmt.Printf("Recovered peer %v logs %v", rf.me, rf.logs)
	rf.mu.Unlock()
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	VoteTerm     int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}
type LogEntry struct {
	Term    int
	Command interface{}
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	VoteTerm   int
	VoteStatus bool
}
type AppendEntriesArg struct {
	LeaderID     int
	Term         int
	PrevLogTerm  int
	Entries      []LogEntry
	PrevLogIndex int
	LeaderCommit int
	Typeof       string
}
type AppendEntriesReply struct {
	Result       bool
	Term         int
	LastLogIndex int
	LastLogTerm  int
	LogLen       int
}

// Append Entry RPC Handler
// For 2A) Should reset the heartbeat time on that raft server
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Println()
	//fmt.Printf("Peer %v appendentry received", rf.me)
	reply.LastLogTerm = -1
	reply.LastLogIndex = -1
	reply.LogLen = len(rf.logs)
	reply.Result = false
	reply.Term = rf.currentTerm
	//fmt.Println(rf.logs)
	/*fmt.Printf("Peer %v", rf.me)
	fmt.Println()
	fmt.Println(args.Entries)
	fmt.Println(rf.logs)*/
	/*fmt.Println()
	fmt.Printf("Append Received %v", args.Entries)
	fmt.Println()
	fmt.Printf("Current logs %v", rf.logs)
	fmt.Println()
	fmt.Printf("Current Term:%v, Args term:%v", rf.currentTerm, args.Term)*/
	if args.Typeof == "leaderSelect" && len(rf.logs) > 1 {
		//rf.commitIndex += 1
	}
	if args.Typeof == "leaderSelect" {
		xdx := LogEntry{}
		xdx.Term = args.Term
		if len(rf.logs) == 0 {
			rf.logs = append(rf.logs, xdx)
		}
		//rf.logs = append(rf.logs, xdx)
		//rf.commitedamt += 1
		//rf.commitIndex += 1
		//rf.commitIndex += 1
	}

	if rf.position == "candidate" || rf.position == "leader" {
		//Upon receiving empty RPC convert to follower
		rf.position = "follower"
		rf.lastHeartbeat = time.Now()
		rf.mu.Unlock()
		wg.Done()
		//fmt.Println("ehraas")
		return
	}
	if len(args.Entries) == 0 && args.Typeof == "Heartbeat" {
		//Reset heartbeat
		rf.lastHeartbeat = time.Now()
		rf.mu.Unlock()
		wg.Done()
		//fmt.Println("heres")
		return
	}
	/*if len(rf.logs) == 0 {
		rf.logs = append(rf.logs, args.Entries[0])
		wg.Done()
	}*/
	//fmt.Println(args.PrevLogIndex)
	reply.LastLogTerm = -1
	reply.LastLogIndex = -1
	reply.LogLen = len(rf.logs)
	reply.Result = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		fmt.Println("here1")
		fmt.Printf("Currentterm:%v", reply.Term)
		wg.Done()
		return
	}
	if len(rf.logs) < args.PrevLogIndex+1 {
		/*fmt.Printf("len %v", len(rf.logs))
		fmt.Println("here3")
		fmt.Printf("args %v", args.PrevLogIndex)*/
		fmt.Println("here3")
		rf.mu.Unlock()
		wg.Done()
		return
	} else {
		wg.Done()
	}
	wg.Wait()
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.LastLogTerm = rf.logs[args.PrevLogIndex].Term
		for i := range rf.logs {
			if rf.logs[i].Term == reply.LastLogTerm {
				reply.LastLogIndex = i
				break
			}
		}
		//fmt.Println("here2")
		rf.mu.Unlock()
		return
	}
	//posi
	posi := 0
	for posi = 0; posi < len(args.Entries); posi++ {
		currentIndex := args.PrevLogIndex + 1 + posi
		//finalposi += 1
		if currentIndex > len(rf.logs)-1 {
			break
		}
		if rf.logs[currentIndex].Term != args.Entries[posi].Term {
			rf.logs = rf.logs[:currentIndex]
			rf.lastLogIndex = len(rf.logs) - 1
			//fmt.Println("Edit")
			rf.persist()
			break
		}
		//fmt.Println("didhere")
	}
	if len(args.Entries) > 0 {
		fmt.Println()
		fmt.Printf("Temp = %v", posi)
		for temp := posi; temp < len(args.Entries); temp++ {
			rf.logs = append(rf.logs, args.Entries[temp])
			fmt.Println()
			fmt.Printf("Peer %v appended %v to the log", rf.me, args.Entries[temp])
		}
		rf.lastLogIndex = len(rf.logs) - 1
		//fmt.Println("edit")

	}
	//fmt.Println()
	/*fmt.Printf("Append Received %v", args.Entries)
	fmt.Println()
	fmt.Printf("Current logs %v", rf.logs)
	fmt.Println()
	fmt.Printf("Current Term:%v, Args term:%v", rf.currentTerm, args.Term)*/
	//fmt.Printf("LeaderCommit: %v", args.LeaderCommit)
	//fmt.Printf("CommitIndex %v", rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		xd := 0
		if args.LeaderCommit > rf.lastLogIndex {
			xd = rf.lastLogIndex
		} else {
			xd = args.LeaderCommit
		}
		for i := rf.commitIndex + 1; i <= xd; i++ {
			rf.commitIndex = i
			newcommit := ApplyMsg{}
			newcommit.Command = rf.logs[i].Command
			newcommit.CommandIndex = rf.commitIndex
			newcommit.CommandValid = true
			fmt.Println()
			fmt.Printf("Peer %v commiting %v at index %v", rf.me, newcommit.Command, newcommit.CommandIndex)
			fmt.Println()
			fmt.Printf("Peer %v logs: %v", rf.me, rf.logs)
			fmt.Println()
			fmt.Printf("Peer %v CommitIndex %v, Leader Commit:%v", rf.me, rf.commitIndex, args.LeaderCommit)
			if newcommit.CommandIndex > rf.commitedamt && newcommit.Command != nil {
				rf.applyChannel <- newcommit
				//rf.commitedamt += 1
			}
			/*fmt.Println()
			fmt.Printf("Peer %v commiting %v at index %v", rf.me, newcommit.Command, newcommit.CommandIndex)
			fmt.Println()
			fmt.Printf("Peer %v logs: %v", rf.me, rf.logs)
			fmt.Println()
			fmt.Printf("Peer %v CommitIndex %v, Leader Commit:%v", rf.me, rf.commitIndex, args.LeaderCommit)
			*/ //rf.applyChannel <- newcommit
			//rf.commitedamt += 1
			//fmt.Println(newcommit)
			//fmt.Println("Commited")
		}
	}
	rf.lastHeartbeat = time.Now()
	//fmt.Println(rf.logs)
	reply.Result = true
	rf.persist()
	rf.mu.Unlock()

}
func (rf *Raft) sendAppendEntry(server int, entry *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", entry, reply)
	return ok
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.VoteTerm == rf.currentTerm {
		if rf.votedFor == args.CandidateID || rf.votedFor == -1 {
			//valid vote or revote in same term
			//fmt.Printf("%d voted %d", rf.me, args.CandidateID)
			//fmt.Println()
			rf.position = "follower"
			reply.VoteStatus = true
			rf.votedFor = args.CandidateID
			rf.lastHeartbeat = time.Now()
			return
		}
	}
	if args.VoteTerm < rf.currentTerm {
		reply.VoteStatus = false
		//rf.position = "follower"
		return
	}
	if args.VoteTerm > rf.currentTerm {
		rf.currentTerm = args.VoteTerm
		rf.votedFor = args.CandidateID
		reply.VoteStatus = true
		//rf.lastHeartbeat = time.Now()
		//fmt.Printf("Voted for %d", rf.votedFor)
		rf.position = "follower"
	}
	if rf.lastLogIndex-1 > 0 {
		lastLogTerm := rf.logs[rf.lastLogIndex-1].Term
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteStatus = false
			return
		}
	}
	rf.position = "follower"
	reply.VoteStatus = true
	rf.lastHeartbeat = time.Now()
	rf.persist()
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	rf.mu.Lock()
	if rf.position != "leader" {
		rf.mu.Unlock()
		return index, term, false
	}
	newlog := LogEntry{}
	newlog.Term = rf.currentTerm
	newlog.Command = command
	rf.logs = append(rf.logs, newlog)
	rf.lastLogIndex = rf.lastLogIndex + 1
	index = rf.lastLogIndex
	//index = rf.commitedamt
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	fmt.Printf("new entry started: %v, command :%v", index, command)
	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//var wg sync.WaitGroup
	for rf.killed() == false {
		rf.mu.Lock()
		position := rf.position
		rf.mu.Unlock()
		if position == "leader" {
			rf.mu.Lock()
			peers := rf.peers
			logs := rf.logs
			lastLogIndex := rf.lastLogIndex
			currentTerm := rf.currentTerm
			commitIndex := rf.commitIndex
			nextIndex := rf.nextIndex
			matchIndex := rf.matchIndex
			self := rf.me
			nextIndex[self] = lastLogIndex + 1
			matchIndex[self] = lastLogIndex
			rf.mu.Unlock()
			// For the leader raft, send out heartbeat every 10s
			for index := range rf.peers {
				if index != rf.me {
					ee := AppendEntriesArg{}
					de := AppendEntriesReply{}
					//ee.Entries = []
					ee.Entries = []LogEntry{}
					ee.Typeof = "Heartbeat"
					rf.sendAppendEntry(index, &ee, &de)
				}
			}
			/*fmt.Printf("lastLogIndex:%v CommitIndex:%v", lastLogIndex, commitIndex)
			fmt.Printf("Logs:%v", logs)
			fmt.Printf("matchIndex:%v", matchIndex)*/
			if lastLogIndex > commitIndex {
				// There is a log to be commited
				votesNeeded := (len(peers) / 2) + 1
				for i := commitIndex + 1; i <= lastLogIndex; i++ {
					//wg.Add(1)
					votes := 0
					//total:=len()
					//fmt.Printf("matchIndex:%v", matchIndex)
					fmt.Println("Starting commit vote")
					fmt.Printf("matchindex:%v", matchIndex)
					for j := range peers {
						//fmt.Println("Voting")
						//fmt.Println(matchIndex[j])
						//fmt.Println(logs[i].Term)
						rf.mu.Lock()
						if matchIndex[j] >= i && logs[i].Term == currentTerm {
							votes++
						}
						rf.mu.Unlock()
					}
					if votes >= votesNeeded {
						//Commit to leader and send applymsg
						rf.mu.Lock()
						rf.commitIndex += 1
						newApply := ApplyMsg{}
						//rf.commitedamt += 1
						newApply.Command = logs[i].Command
						//newApply.CommandIndex = i
						newApply.CommandValid = true
						if newApply.Command != nil {
							//rf.commitedamt += 1
							fmt.Println()
							fmt.Printf("Leader commited %v at %v", newApply, rf.commitedamt)
						}
						newApply.CommandIndex = rf.commitIndex
						rf.applyChannel <- newApply
						//fmt.Println("Commited to leader")
						//wg.Done()
						rf.mu.Unlock()
					} else {
						//wg.Done()
					}
					//fmt.Println()
					/*fmt.Println()
					fmt.Printf("Leader %v Sending RPC to %v", rf.me)
					fmt.Println()
					fmt.Printf("Leaders Logs: %v", rf.logs)*/
				}
				for index := range peers {
					if index == self {
						continue
					}
					fmt.Println()

					appendArgs := AppendEntriesArg{}
					appendReply := AppendEntriesReply{}
					rf.mu.Lock()
					fmt.Printf("Nextindex:%v", nextIndex)
					//fmt.Println(rf.nextIndex)
					//fmt.Printf("Nextindex")
					PrevLogIndex := nextIndex[index] - 1
					if PrevLogIndex == -1 {
						PrevLogIndex = 0
					}
					appendArgs.PrevLogTerm = rf.logs[PrevLogIndex].Term
					appendArgs.PrevLogIndex = PrevLogIndex
					//rf.mu.Lock()
					appendArgs.LeaderCommit = rf.commitIndex
					appendArgs.LeaderID = self
					appendArgs.Term = rf.currentTerm
					if nextIndex[index] <= rf.lastLogIndex {
						//xd := nextIndex[index] - 1
						appendArgs.Entries = logs[nextIndex[index] : rf.lastLogIndex+1]
						//fmt.Printf("Sending %v", appendArgs.Entries)
					}
					//fmt.Println(rf.logs)
					fmt.Println()
					fmt.Printf("Leader %v Sending RPC to %v", rf.me, index)
					fmt.Println()
					fmt.Printf("Leaders Logs: %v", rf.logs)
					fmt.Println()
					fmt.Printf("Logs sent: %v", appendArgs.Entries)
					rf.mu.Unlock()
					//fmt.println(rf.logs)
					go func(index int) {

						//Send commit attempt to all peers
						rf.sendAppendEntry(index, &appendArgs, &appendReply)
						rf.mu.Lock()
						//fmt.Println(rf.logs)
						peerlastlog := rf.nextIndex[index] - 1
						if appendReply.Result == true {
							//Successfully appended latest commit
							dxd := rf.nextIndex[index] + len(appendArgs.Entries)
							if (rf.lastLogIndex + 1) < dxd {
								dxd = rf.lastLogIndex + 1
							}
							rf.nextIndex[index] = dxd
							//fmt.Println("returned true")
							rf.matchIndex[index] = peerlastlog + len(appendArgs.Entries)
						} else {
							if appendReply.Term > rf.currentTerm {
								rf.position = "follower"
								rf.mu.Unlock()
								return
							}
							if appendReply.LastLogIndex == -1 {
								//fmt.Println("Here")
								rf.nextIndex[index] = appendReply.LogLen
								rf.mu.Unlock()
								return
							}
							newindex := -1
							for i, v := range rf.logs {
								if v.Term == appendReply.LogLen {
									index = i
								}
							}
							if newindex == -1 {
								rf.nextIndex[index] = appendReply.LastLogIndex
								//fmt.Println("Here")
							} else {
								rf.nextIndex[index] = newindex
							}
						}
						rf.mu.Unlock()
					}(index)

				}

			}
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
		} else if position == "follower" {
			// If follower, check if timeout met else do nothing
			max := 1050
			min := 450
			rf.mu.Lock()
			lastHeartbeat := rf.lastHeartbeat
			rf.mu.Unlock()
			timeout := (rand.Intn(max-min) + min)
			// Follower timeout = become candidate
			if time.Now().Sub(lastHeartbeat).Milliseconds() > int64(timeout) {
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.votedFor = -1
				rf.position = "candidate"
				fmt.Println()
				fmt.Printf("peer %v started vote", rf.me)
				rf.persist()
				rf.mu.Unlock()
			}
			//rf.persist()

		} else if rf.position == "candidate" {
			var wg sync.WaitGroup
			// If candidate, start an election and become either leader or back to follower
			max := 950
			min := 500
			rf.mu.Lock()
			//lastHeartbeat := rf.lastHeartbeat
			votesNeeded := (len(rf.peers) / 2) + 1
			total := len(rf.peers)
			//rf.votes = 0
			//rf.replies = 0
			rf.mu.Unlock()
			timeout1 := (rand.Intn(max-min) + min)
			//timeout2 := (rand.Intn(max-200) + 200)

			//electionStart:=time.Now()
			votes := 0
			replies := 0
			//if time.Now().Sub(lastHeartbeat).Milliseconds() > int64(timeout1) {
			//Election timeout reached -> Start election
			//votes := 0
			//fmt.Println(votes)
			//fmt.Println(timeout)
			//fmt.Printf("Election started %d", rf.me)
			//fmt.Println()
			/*rf.mu.Lock()
			rf.votedFor = -1
			rf.currentTerm += 1
			rf.mu.Unlock()*/
			//fmt.Printf("Electionterm %d started by %d", rf.currentTerm, rf.me)
			//fmt.Println()
			electionStart := time.Now()
			wg.Add(1)
			for index := range rf.peers {
				//args := RequestVoteArgs{}
				//reply := RequestVoteReply{}

				if index == rf.me {
					//Vote for self
					////fmt.Printf("Voted for self")
					rf.mu.Lock()
					votes += 1
					replies += 1
					rf.votedFor = rf.me
					rf.mu.Unlock()
				} else {
					go func(index int) {

						args := RequestVoteArgs{}
						reply := RequestVoteReply{}
						rf.mu.Lock()
						args.VoteTerm = rf.currentTerm
						args.CandidateID = rf.me
						rf.mu.Unlock()
						rf.sendRequestVote(index, &args, &reply)
						if reply.VoteStatus == true {
							rf.mu.Lock()
							votes += 1
							replies += 1
							rf.persist()
							rf.mu.Unlock()
						} else {

							rf.mu.Lock()
							if rf.currentTerm < reply.VoteTerm {
								rf.position = "follower"
							}
							replies += 1
							rf.persist()
							rf.mu.Unlock()
						}
					}(index)
				}
			}
			for {
				rf.mu.Lock()
				if replies == total || votes >= votesNeeded || time.Now().Sub(electionStart).Milliseconds() > int64(timeout1) {
					//fmt.Printf("Node %d votes %d Term %d", rf.me, votes, rf.currentTerm)
					wg.Done()
					//fmt.Printf(rf.position)
					//fmt.Println()
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				// Wait for election to finish
				time.Sleep(100 * time.Millisecond)
			}
			wg.Wait()
			//fmt.Printf("Node %d votes %d Term %d Post %s", rf.me, votes, rf.currentTerm, rf.position)
			rf.mu.Lock()
			//posi := rf.position
			//rf.mu.Unlock()
			if votes >= votesNeeded && rf.position == "candidate" {
				//Won majority
				//fmt.Println(rf.peers)
				//fmt.Printf(" %dBeceame leader", rf.me)
				//fmt.Println(rf.me)
				//rf.mu.Lock()
				rf.position = "leader"
				fmt.Println()
				fmt.Printf("Peer %v became leader", rf.me)
				for index := range rf.peers {
					rf.nextIndex[index] = rf.lastLogIndex + 1
					//rf.matchIndex[index] += 1
					if index != rf.me {
						ee := AppendEntriesArg{}
						de := AppendEntriesReply{}
						ee.Entries = []LogEntry{}
						ee.Typeof = "leaderSelect"
						ee.Term = rf.currentTerm
						//ee.Entries = []
						rf.sendAppendEntry(index, &ee, &de)
					}
				}
				xdx := LogEntry{}
				xdx.Term = rf.currentTerm
				if len(rf.logs) == 0 {
					rf.logs = append(rf.logs, xdx)
				}
				//rf.logs = append(rf.logs, xdx)
				if len(rf.logs) > 2 {
					//rf.commitIndex += 1
					//rf.lastLogIndex += 1
				}
				//fmt.Println(rf.nextIndex)
				//rf.mu.Unlock()
			} else if time.Now().Sub(electionStart).Milliseconds() > int64(timeout1) {

				//fmt.Printf("Node %d Hit timeout time", rf.me)
				//fmt.Println()
				//f.mu.Lock()
				rf.position = "follower"
				rf.lastHeartbeat = time.Now()
				//rf.mu.Unlock()
				//rf.mu.Unlock()
			} else if replies == total {
				//Did not get majority
				//fmt.Println("HIt reply=total")
				//rf.mu.Lock()
				rf.position = "follower"
				rf.lastHeartbeat = time.Now()
				//time.sleep()
				//rf.mu.Unlock()
			}
			rf.persist()
			rf.mu.Unlock()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(100 * time.Millisecond)
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//rf.mu.Lock()

	max := 650
	min := 250
	rf.timeout = (rand.Intn(max-min) + min)
	rf.position = "follower"
	//rf.lastLogIndex = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votes = 0
	rf.replies = 0
	rf.commitedamt = 0
	//rf.currentTerm = 0
	rf.logs = []LogEntry{}
	rf.applyChannel = applyCh

	// Your initialization code here (4A, 4B).

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
