package main
import (
  "time"
  "io/ioutil"
  "os"
  "go.uber.org/zap"
  "github.com/hashicorp/raft"
  "fmt"
)

type RaftEnv struct {
  dir string
  conf *raft.Config
  fsm *raft.MockFSM
  store *raft.InmemStore
  snapshot *raft.FileSnapshotStore
  trans *raft.NetworkTransport
  raft *raft.Raft
}

var (
  logger *zap.Logger
)

func init() {
  l, err := zap.NewProduction()
  if err != nil {
    panic("init logger error")
  }
  logger = l
}


func NoErr(err error) {
  if err != nil {
    logger.Panic("",zap.Error(err))
  }
}

func waitFor(env *RaftEnv, state raft.RaftState) error {
  limit := time.Now().Add(200 * time.Millisecond)
  for env.raft.State() != state {
    if time.Now().Before(limit) {
      time.Sleep(10*time.Millisecond)
    } else {
      return fmt.Errorf("failed to transition to state %v",state)
    }
  }
  return nil
}
      
  
func main() {
  logger.Info("program starting...")
  conf := raft.DefaultConfig()
  conf.LocalID = raft.ServerID("first")
  conf.HeartbeatTimeout = 50* time.Millisecond
  conf.ElectionTimeout = 50* time.Millisecond
  conf.LeaderLeaseTimeout = 50* time.Millisecond
  conf.CommitTimeout = 5* time.Millisecond
  conf.SnapshotThreshold = 100
  conf.TrailingLogs = 10

  //create a single node
  env1 := makeRaft(conf, true)
  NoErr(waitFor(env1,raft.Leader))

  //join a few nodes
  var envs []*RaftEnv
  for i := 0; i<2; i++ {
    conf.LocalID = ServerID(fmt.Sprintf("next-batch-%d",i))
    env := makeRaft(conf,false)
    addr := env.trans.LocalAddr()
    NoErr(
  }
    
  
}

func makeRaft(conf *raft.Config, bootstrap bool) *RaftEnv {
  stable := raft.NewInmemStore()
  if _, err := os.Stat("snapshot"); os.IsNotExist(err) {
    err := os.Mkdir("snapshot",0755)
    NoErr(err)
  }
  
  dir,err := ioutil.TempDir("snapshot","raft") 
  if err != nil {
    logger.Panic("creating temp file",zap.Error(err))
  }
  snap, err := raft.NewFileSnapshotStore(dir,3,nil)
  if err != nil {
    panic("creating snapstore fail")
  }

  env := &RaftEnv{
    conf: conf,
    dir: dir,
    store: stable,
    snapshot: snap,
    fsm: &raft.MockFSM{},
  }

  trans, err := raft.NewTCPTransport("127.0.0.1:0",nil,2,time.Second,nil)
  if err != nil {
    logger.Panic("newtcptransport",zap.Error(err))
  }
  env.trans = trans

  if bootstrap {
    var configuration raft.Configuration
    logger.Info("bootstaping raft cluster")
    configuration.Servers = append(configuration.Servers, raft.Server{
      Suffrage: raft.Voter,
      ID: conf.LocalID,
      Address: trans.LocalAddr(),
    })
  
    err = raft.BootstrapCluster(conf, stable, stable,snap, trans, configuration)
    if err != nil {
     panic("bootstarpcluster fail")
    } 
  }

  logger.Info("starting node, ", zap.String("addr:",string(trans.LocalAddr())))

  //we dont need a logger for conf, since NewRaft will assign hclogger for it if there is no logger
  raft, err := raft.NewRaft(conf, env.fsm, stable, stable, snap, trans)
  if err != nil {
    panic("staring a new raft fail")
  }
  env.raft = raft

  return env
} 
