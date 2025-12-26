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
type Server struct{

	mu sync.Mutex


	serverId int
	peerIds []int

	cm *ConsensusModule
	rpcproxy *RPCProxy

	rpcserver *rpc.Server
	listener net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan any
	quit chan any
	wg sync.WaitGroup
}



func newServer(serverId int , peerIds []int , ready <-chan any) *Server{
	s:= new(Server)
	s.serverId = serverId
	s.peerIds =peerIds
	s.peerClients= make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan any)
	return s
}

func (s *Server) Serve(){
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready)
	s.rpcserver = rpc.NewServer()
	s.rpcserver.RegisterName("ConsensusModule",s.rpcproxy )

	var err error
	s.listener , err = net.Listen("tcp", ";0")
	if err!=nil{
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()


	s.wg.Add(1)
	go func(){
		defer s.wg.Done()


		for{
			conn, err := s.listener.Accept()
			if err!=nil{
				select{
					case <- s.quit:
					return
					default:
					log.Fatal("accept error")
				}
			}
			s.wg.Add(1)
			go func(){
				s.rpcserver.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error{
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerId] == nil{
		client , err  := rpc.Dial(addr.Network(),addr.String())
		if err!=nil{
			return err
		}
		s.peerClients[peerId]= client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
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
		if dice == 9 {
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
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
		if dice == 9 {
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
