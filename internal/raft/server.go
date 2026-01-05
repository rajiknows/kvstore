package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Cluster struct {
	mu sync.Mutex

	serverID int
	peerIds  []int

	cm       *ConsensusModule
	rpcproxy *RPCProxy

	rpcserver *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup
}

func NewCluster(serverID int, peerIds []int, ready <-chan any) *Cluster {
	s := new(Cluster)
	s.serverID = serverID
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan any)
	return s
}

func (s *Cluster) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverID, s.peerIds, s, s.ready)
	s.rpcserver = rpc.NewServer()
	err := s.rpcserver.RegisterName("ConsensusModule", s.rpcproxy)
	if err != nil {
		log.Fatal(err)
	}

	// var err error
	s.listener, err = net.Listen("tcp", ";0")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %s", s.serverID, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Go(func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error")
				}
			}
			s.wg.Go(func() {
				s.rpcserver.ServeConn(conn)
			})
		}
	})
}

func (s *Cluster) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			err := s.peerClients[id].Close()
			if err != nil {
				fmt.Printf("failed to close a peerClient id = %d", id)
			}
			s.peerClients[id] = nil
		}
	}
}

func (s *Cluster) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	err := s.listener.Close()
	if err != nil {
		fmt.Printf("failed to close a Listner")
	}
	s.wg.Wait()
}

func (s *Cluster) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Cluster) ConnectToPeer(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerID] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerID] = client
	}
	return nil
}

func (s *Cluster) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerID] != nil {
		err := s.peerClients[peerID].Close()
		s.peerClients[peerID] = nil
		return err
	}
	return nil
}

func (s *Cluster) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
