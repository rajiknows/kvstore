package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	testTimeout     = 30 * time.Second
	testclusterSize = 3
)

type Node struct{}

type TestCluster struct {
	nodes   []*exec.Cmd
	ports   []struct{ http, raft int }
	dataDir []string
	mu      sync.Mutex
}

func (tc *TestCluster) start(t *testing.T, ctx context.Context) {
	tc.nodes = make([]*exec.Cmd, testclusterSize)
	tc.dataDir = make([]string, testclusterSize)
	tc.ports = make([]struct{ http, raft int }, testclusterSize)

	for i:=range testclusterSize{
		httpPort := 8080 + i
		raftPort := 5000 +i
		tc.ports[i] = struct{http, raft int}{httpPort, raftPort}

		dir := filepath.Join(t.TempDir(),fmt.Sprintf("node-%d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create data dir: %v", err)
		}
		tc.dataDir[i] = dir

		clusterStr := ""
		clusterHttpStr := ""

		for j:=range testclusterSize{
			if i==j{
				continue
			}

			clusterStr+= fmt.Sprintf("%d=localhost:%d,", j, 5000+j)
			clusterHttpStr += fmt.Sprintf("%d=localhost:%d,", j, 8080+j)
		}

		clusterStr = strings.TrimSuffix(clusterStr, ",")
		clusterHttpStr = strings.TrimSuffix(clusterHttpStr, ",")


		cmd := exec.CommandContext(ctx, "go", "run", "./cmd/kvstore/main.go",
			fmt.Sprintf("-id=%d", i),
			fmt.Sprintf("-http-addr=localhost:%d", httpPort),
			fmt.Sprintf("-raft-addr=localhost:%d", raftPort),
			fmt.Sprintf("-cluster=%s", clusterStr),
			fmt.Sprintf("-cluster-http=%s", clusterHttpStr),
		)
		cmd.Stdout= os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = "."


		tc.nodes[i] = cmd
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start node %d: %v", i, err)
		}

		time.Sleep(400 * time.Millisecond)


	}
}


func (tc *TestCluster) ShutDown(){
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for _,cmd := range tc.nodes{
		if cmd.Process!=nil && cmd!=nil{
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	for _, cmd := range tc.nodes {
			if cmd != nil {
				_ = cmd.Wait()
			}
		}
}

func (tc *TestCluster) httpClient() *http.Client{
	return &http.Client{Timeout: 5*time.Second}
}
func (c *TestCluster) put(t *testing.T, nodeIdx int, key, value string) {
	url := fmt.Sprintf("http://localhost:%d/put/%s", c.ports[nodeIdx].http, key)
	body := map[string]string{"value": value}
	data, _ := json.Marshal(body)

	req, _ := http.NewRequest("PUT", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusTemporaryRedirect {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected PUT status %d: %s", resp.StatusCode, body)
	}
}

func (c *TestCluster) get(t *testing.T, nodeIdx int, key string) string {
	url := fmt.Sprintf("http://localhost:%d/get/%s", c.ports[nodeIdx].http, key)
	resp, err := c.httpClient().Get(url)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTemporaryRedirect {
		// follow redirect once
		loc := resp.Header.Get("Location")
		if loc == "" {
			t.Fatal("redirect without Location")
		}
		resp2, err := c.httpClient().Get(loc)
		if err != nil {
			t.Fatalf("redirect GET failed: %v", err)
		}
		defer resp2.Body.Close()
		resp = resp2
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected GET status %d: %s", resp.StatusCode, body)
	}

	body, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body))
}

func TestCluster_BasicWriteRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	var cluster TestCluster
	cluster.start(t, ctx)
	defer cluster.ShutDown()

	// write via node 0
	cluster.put(t, 0, "testkey", "testvalue")

	// read from all nodes
	for i := 0; i < testclusterSize; i++ {
		val := cluster.get(t, i, "testkey")
		if val != "testvalue" {
			t.Errorf("node %d read wrong value: got %q, want %q", i, val, "testvalue")
		}
	}
}

func TestCluster_Failover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	var cluster TestCluster
	cluster.start(t, ctx)
	defer cluster.ShutDown()

	cluster.put(t, 0, "failkey", "before")

	// kill presumed initial leader (node 0)
	if cluster.nodes[0] != nil && cluster.nodes[0].Process != nil {
		_ = cluster.nodes[0].Process.Kill()
		cluster.nodes[0] = nil // mark as dead
	}

	time.Sleep(5 * time.Second) // allow election

	// write after failover
	cluster.put(t, 1, "failkey", "after") // try via node 1

	// read from surviving nodes
	for i := 1; i < testclusterSize; i++ {
		val := cluster.get(t, i, "failkey")
		if val != "after" {
			t.Errorf("node %d has stale/incorrect value after failover: %q", i, val)
		}
	}
}
