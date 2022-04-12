package weed_server

import (
	"net/http"
	"time"

	ui "github.com/bary321/seaweedfs-1/weed/server/master_ui"
	"github.com/bary321/seaweedfs-1/weed/stats"
	"github.com/bary321/seaweedfs-1/weed/util"
	"github.com/chrislusf/raft"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	args := struct {
		Version    string
		Topology   interface{}
		RaftServer raft.Server
		Stats      map[string]interface{}
		Counters   *stats.ServerStats
	}{
		util.Version(),
		ms.Topo.ToMap(),
		ms.Topo.RaftServer,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
