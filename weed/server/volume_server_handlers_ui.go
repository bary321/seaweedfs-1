package weed_server

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/bary321/seaweedfs-1/weed/pb/volume_server_pb"
	ui "github.com/bary321/seaweedfs-1/weed/server/volume_server_ui"
	"github.com/bary321/seaweedfs-1/weed/stats"
	"github.com/bary321/seaweedfs-1/weed/storage"
	"github.com/bary321/seaweedfs-1/weed/util"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	volumeInfos := vs.store.VolumeInfos()
	var normalVolumeInfos, remoteVolumeInfos []*storage.VolumeInfo
	for _, vinfo := range volumeInfos {
		if vinfo.IsRemote() {
			remoteVolumeInfos = append(remoteVolumeInfos, vinfo)
		} else {
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
		}
	}
	args := struct {
		Version       string
		Masters       []string
		Volumes       interface{}
		EcVolumes     interface{}
		RemoteVolumes interface{}
		DiskStatuses  interface{}
		Stats         interface{}
		Counters      *stats.ServerStats
	}{
		util.Version(),
		vs.SeedMasterNodes,
		normalVolumeInfos,
		vs.store.EcVolumes(),
		remoteVolumeInfos,
		ds,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
