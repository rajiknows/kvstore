package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const DebugCM =1

// let's implement raft from scratch here
// its gonna be intreresting lets see
//
//
// not gonna sleep unless i implement it

type CmState int
const (
	Follower CmState = iota
	Candidate
	Leader
	Dead
)

type LogEntry struct{
	Command any
	Term int
}

type ConsensusModule struct{
	mu sync.Mutex
	// id is the serverId of the node
	id int

	// server is the server containing this cm and it is used for rpc comm
	server *Server

	// peerIds list the ids of the nodes
	peerIds []int

	// Persistant raft state
	currentTerm int
	votedFor int
	log []LogEntry


	// volatile state
	state CmState
	electionResetEvent time.Time
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

func (cm *ConsensusModule) electionTimeout() time.Duration{
	return time.Duration(150 + rand.Intn(150))*time.Millisecond
}

func (cm *ConsensusModule) dlog(format string, args ...any){
	if DebugCM>0{
		fmt.Println(fmt.Sprintf("[%d] ", cm.id)+format , args)
	}
}

func (cm *ConsensusModule) runElectionTimer(){
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer start (%v), term = %d", timeoutDuration, termStarted)


	ticker  := time.NewTicker(10* time.Millisecond)
	for{
		<-ticker.C

		cm.mu.Lock()
		if cm.state!= Candidate && cm.state!= Follower{
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted!= cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out ",termStarted, cm.currentTerm )
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed<timeoutDuration{
			cm.startElection()
			cm.mu.Unlock()
			return
		}

		cm.mu.Unlock()
	}
}

type RequestVoteArgs struct{
	Term int
	CandidateId int
}

// type RequestVoteReply{

// }

func (cm *ConsensusModule) startElection(){
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent =time.Now()
	// vote yourself
	cm.votedFor = cm.id
	cm.dlog("becomes candidate (currentTerm = %d); log = %v", savedCurrentTerm, cm.log)


	votesReceived := 1

	for _,peerId := range cm.peerIds{
		go func(){
			args:= RequestVoteArgs{
				Term: savedCurrentTerm,
				CandidateId: cm.id,
			}

			var reply RequestVoteReply
			cm.dlog("sending request vote to peer = %d , args = %+v", peerId, args)
			if err:= cm.server.Call(peerId, "REQUESTVOTE", args, &reply); err==nil{
				cm.mu.Lock()
				defer cm.mu.Unlock()

				cm.dlog("recieved reply from peer = %d , reply = %+v", peerId, reply)

				if cm.state != Candidate{
					cm.dlog("while waiting for reply state changed to %d", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm{
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				}else if reply.Term == savedCurrentTerm{
					if reply.VoteGranted {
						votesReceived+=1
						if votesReceived*2 > len(cm.peerIds) + 1{
							// won the election
							cm.dlog("won the election with %d votes", votesReceived)
							cm.StartLeader()
							return
						}
					}
				}
			}
		}()
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) StartLeader(){
	cm.state= Leader
	cm.dlog("becomes leader term = %d , log = %+v", cm.currentTerm, cm.log)


	go func(){
		// send periodic heartbeats
		ticker := time.NewTicker(50 *time.Millisecond)
		defer ticker.Stop()

		for{
			cm.leaderSendHeartBeats()
			<-ticker.C


			cm.mu.Lock()
			if cm.state!=Leader{
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartBeats(){
	cm.mu.Lock()
	if cm.state != Leader{
		cm.mu.Unlock()
		return
	}

	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()


	for _, peerId := range cm.peerIds{
		args := AppendEntriesArgs{
			Term: savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(){
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "APPENDENTRIES", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}()

	}

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}


func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}
