package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type RaftEnv struct {
	dir      string
	conf     *raft.Config
	fsm      *raft.MockFSM
	store    *raft.InmemStore
	snapshot *raft.FileSnapshotStore
	trans    *raft.NetworkTransport
	raft     *raft.Raft
}

var (
	logger  *zap.Logger
	raftEnv *RaftEnv
)

func init() {
	l, err := zap.NewProduction()
	if err != nil {
		panic("init logger error")
	}
	logger = l
}

func WaitFuture(f raft.Future) error {
	timer := time.AfterFunc(200*time.Millisecond, func() {
		panic(fmt.Errorf("timeout waiting for future %v", f))
	})
	defer timer.Stop()
	return f.Error()
}

func NoErr(err error) {
	if err != nil {
		logger.Panic("", zap.Error(err))
	}
}

func waitFor(env *RaftEnv, state raft.RaftState) error {
	limit := time.Now().Add(50000 * time.Millisecond)
	for env.raft.State() != state {
		if time.Now().Before(limit) {
			time.Sleep(10 * time.Millisecond)
		} else {
			return fmt.Errorf("failed to transition to state %v", state)
		}
	}
	return nil
}

func WaitForAny(state raft.RaftState, envs []*RaftEnv) (*RaftEnv, error) {
	limit := time.Now().Add(200 * time.Millisecond)
CHECK:
	for _, env := range envs {
		if env.raft.State() == state {
			return env, nil
		}
	}

	if time.Now().Before(limit) {
		goto WAIT
	}
	return nil, fmt.Errorf("failed to find node in %v state", state)
WAIT:
	time.Sleep(10 * time.Millisecond)
	goto CHECK
}

func leaderAction(conf *raft.Config, serverID string, port string) {
	//create a single leader node
	conf.LocalID = raft.ServerID(serverID)
	raftEnv = makeRaft(conf, true, port)
	NoErr(waitFor(raftEnv, raft.Leader))
	/*
	   for i := 0; i<2; i++ {
	     var serverID, port string
	     //add one voter
	     fmt.Print("Input Voter addr [serverID port]:")
	     fmt.Scanln(&serverID,&port)
	     addr := raft.ServerAddress("127.0.0.1:"+port)
	     NoErr(WaitFuture(env1.raft.AddVoter(raft.ServerID(serverID),addr,0,0)))
	   }
	*/
}

func voterAction(conf *raft.Config, serverID string, port string) {
	conf.LocalID = raft.ServerID(serverID)
	raftEnv = makeRaft(conf, false, port)
}

func httpStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "rafStatus:"+raftEnv.raft.State().String()+"\n")
}

func httpPostHandler(w http.ResponseWriter, r *http.Request) {
	//only leader can response POST action: addnode or addlog
	if raftEnv.raft.State() != raft.Leader {
		http.Error(w, "I am not Leader!!!", http.StatusNotFound)
		return
	}
	if strings.Contains(r.URL.Path, "addnode") {
		httpAddNode(w, r)
	} else if strings.Contains(r.URL.Path, "addlog") {
		httpAddLog(w, r)
	}
}

// return a log entry that's at least sz long that has the prefix 'test i '
func logBytes(i, sz int) []byte {
	var logBuffer bytes.Buffer
	fmt.Fprintf(&logBuffer, "test %d ", i)
	for logBuffer.Len() < sz {
		logBuffer.WriteByte('x')
	}
	return logBuffer.Bytes()
}

//add random log to raft
func httpAddLog(w http.ResponseWriter, r *http.Request) {
	var future raft.ApplyFuture
	future = raftEnv.raft.Apply(logBytes(1, 8), 0)
	NoErr(WaitFuture(future))
}

func httpAddNode(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		fmt.Fprintf(w, "ParseForm() err: %v", err)
		return
	}

	serverID := r.Form.Get("serverID")
	port := r.Form.Get("port")
	logger.Info("Receive adding server request\n")
	logger.Info("serverID:" + serverID + "  port:" + port + "\n")
	if port == "" || serverID == "" {
		fmt.Fprintf(w, "port or serverID is nil\n")
		return
	}
	addr := raft.ServerAddress("127.0.0.1:" + port)
	NoErr(WaitFuture(raftEnv.raft.AddVoter(raft.ServerID(serverID), addr, 0, 0)))
	fmt.Fprintf(w, "adding server success\n")
}

func httpHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		httpStatus(w, r)
	} else if r.Method == "POST" {
		httpPostHandler(w, r)
	} else {
		http.Error(w, "Method is not support.", http.StatusNotFound)
	}
}

/***********
parameter:
usage:  traft [l|v] [ServerID] [raft_port] [http_port]
Des:
   this program use hashicorp raft package to build a raft cluster. All processes need to run on same machine.
   para:
       [l|v] leader or voter. l means the process will start as leader. voter means process start as voter, so it will wait untill leader add it to cluster.
       [ServerID] Id of the server. ID should be different for each server in the cluster.
       [raft_port] raft listen port of the server. (each port has to be different than others)
       [http_port] http port for getting information of raft and input commands for raft

  procedure:
    1. start leader: traft l fist 8000 9000
    2. start one voter: traft v second 8001 9001
    3. join the voter to cluster (on first port): curl -X POST -d "serverID=second&port=8001" http://127.0.0.1:9000/addnode
    4. start another voter node: traft v third 8002 9002
    5. join the voter to cluster (on first port): curl -X POST -d "serverID=third&port=8002" http://127.0.0.1:9000/addnode
for leader:
  Add Node: curl -X POST -d "serverID=[serverID]&port=[port]" http://127.0.0.1:9000/addnode
  Add random log: curl -X POST http://127.0.0.1:9000/addlog
*/
func main() {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID("first")
	conf.HeartbeatTimeout = 3000 * time.Millisecond
	conf.ElectionTimeout = 3000 * time.Millisecond
	conf.LeaderLeaseTimeout = 3000 * time.Millisecond
	conf.CommitTimeout = 500 * time.Millisecond
	conf.SnapshotThreshold = 100
	conf.TrailingLogs = 10

	if len(os.Args) != 5 {
		logger.Info("Usage: traft [l|v] [ServerID] [raft_port] [http_port]")
		logger.Panic("args Error")
	}

	logger.Info("program starting...")

	if os.Args[1] == "l" {
		leaderAction(conf, os.Args[2], os.Args[3])
	} else {
		voterAction(conf, os.Args[2], os.Args[3])
	}

	logger.Info("starting http server...")
	http.HandleFunc("/", httpHandler)
	NoErr(http.ListenAndServe(":"+os.Args[4], nil))
}

func makeRaft(conf *raft.Config, bootstrap bool, port string) *RaftEnv {
	stable := raft.NewInmemStore()
	if _, err := os.Stat("snapshot"); os.IsNotExist(err) {
		err := os.Mkdir("snapshot", 0755)
		NoErr(err)
	}

	dir, err := ioutil.TempDir("snapshot", "raft")
	if err != nil {
		logger.Panic("creating temp file", zap.Error(err))
	}
	snap, err := raft.NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		panic("creating snapstore fail")
	}

	env := &RaftEnv{
		conf:     conf,
		dir:      dir,
		store:    stable,
		snapshot: snap,
		fsm:      &raft.MockFSM{},
	}

	trans, err := raft.NewTCPTransport("127.0.0.1:"+port, nil, 2, time.Second, nil)
	if err != nil {
		logger.Panic("newtcptransport", zap.Error(err))
	}
	env.trans = trans

	if bootstrap {
		var configuration raft.Configuration
		logger.Info("bootstaping raft cluster")
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       conf.LocalID,
			Address:  trans.LocalAddr(),
		})

		err = raft.BootstrapCluster(conf, stable, stable, snap, trans, configuration)
		if err != nil {
			panic("bootstarpcluster fail")
		}
	}

	logger.Info("starting node, ", zap.String("addr:", string(trans.LocalAddr())))

	//we dont need a logger for conf, since NewRaft will assign hclogger for it if there is no logger
	raft, err := raft.NewRaft(conf, env.fsm, stable, stable, snap, trans)
	if err != nil {
		panic("staring a new raft fail")
	}
	env.raft = raft

	return env
}
