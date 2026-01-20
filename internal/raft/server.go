package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"kvstore/internal"
)

type Server struct {
	mu sync.Mutex

	serverID int
	peerIds  []int

	cm *ConsensusModule

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup
}

func NewServer(serverID int, peerIds []int, ready <-chan any, commitChan chan<- internal.Log) *Server {
	s := new(Server)
	s.serverID = serverID
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan any)
	s.cm = NewConsensusModule(s.serverID, s.peerIds, s, s.ready, commitChan)
	return s
}

func (s *Server) GetLeaderID() int {
	return s.cm.GetLeaderId()
}

func (s *Server) Serve(addr string) {
	s.mu.Lock()

	s.rpcServer = rpc.NewServer()
	err := s.rpcServer.Register(s.cm)
	if err != nil {
		log.Fatal(err)
	}

	s.listener, err = net.Listen("tcp", addr)
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
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Go(func() {
				s.rpcServer.ServeConn(conn)
			})
		}
	})
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			err := s.peerClients[id].Close()
			if err != nil {
				log.Fatal(err)
			}
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	err := s.listener.Close()
	if err != nil {
		log.Fatal(err)
	}
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		if err != nil {
			log.Fatal(err)
		}
		s.peerClients[peerId] = nil
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	}
	return peer.Call(serviceMethod, args, reply)
}

func (s *Server) Submit(command internal.Log) (bool, error) {
	return s.cm.Submit(command)
}
