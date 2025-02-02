package topology

import (
	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/storage/needle"
	"github.com/chrislusf/raft"
)

type MaxVolumeIdCommand struct {
	MaxVolumeId needle.VolumeId `json:"maxVolumeId"`
}

func NewMaxVolumeIdCommand(value needle.VolumeId) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
	}
}

func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

func (c *MaxVolumeIdCommand) Apply(server raft.Server) (interface{}, error) {
	topo := server.Context().(*Topology)
	before := topo.GetMaxVolumeId()
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)

	glog.V(1).Infoln("max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}
