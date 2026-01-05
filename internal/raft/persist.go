package raft

import (
	"encoding/gob"
	"fmt"
	"os"
)

func (cm *ConsensusModule) Persist() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// persist the voting state in a file
	file, err := os.Create(fmt.Sprintf("raft_state-%d.bin", cm.id))
	if err != nil {
		return
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	data := struct {
		CurrentTime int
		VotedFor    int
		log         []LogEntry
	}{cm.currentTerm, cm.votedFor, cm.log}
	err = enc.Encode(data)
	if err != nil {
		cm.dlog("error in encoding persist data")
		return
	}
	err = file.Sync()
	if err != nil {
		cm.dlog("error in encoding persist data")
		return
	}
}

func (cm *ConsensusModule) Load() {
}
