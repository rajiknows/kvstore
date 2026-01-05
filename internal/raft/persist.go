package raft

import (
	"encoding/gob"
	"fmt"
	"os"
)

type RaftState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

func (cm *ConsensusModule) Persist() {
	state := RaftState{
		CurrentTerm: cm.currentTerm,
		VotedFor:    cm.votedFor,
		Log:         cm.log,
	}
	file, err := os.Create(fmt.Sprintf("raft_state-%d.bin", cm.id))
	if err != nil {
		cm.dlog("error in creating persist file: %v", err)
		return
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	if err := enc.Encode(state); err != nil {
		cm.dlog("error in encoding persist data: %v", err)
	}
}

func (cm *ConsensusModule) Load() {
	file, err := os.Open(fmt.Sprintf("raft_state-%d.bin", cm.id))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		cm.dlog("error in opening persist file: %v", err)
		return
	}
	defer file.Close()

	var state RaftState
	dec := gob.NewDecoder(file)
	if err := dec.Decode(&state); err != nil {
		cm.dlog("error in decoding persist data: %v", err)
		return
	}

	cm.currentTerm = state.CurrentTerm
	cm.votedFor = state.VotedFor
	cm.log = state.Log
}
