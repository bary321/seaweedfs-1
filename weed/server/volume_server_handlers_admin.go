package weed_server

import (
	"net/http"
	"path/filepath"

	"github.com/bary321/seaweedfs-1/weed/pb/volume_server_pb"
	"github.com/bary321/seaweedfs-1/weed/stats"
	"github.com/bary321/seaweedfs-1/weed/util"
)

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.Version()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatuses"] = ds
	m["Volumes"] = vs.store.VolumeInfos()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.Version()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}
