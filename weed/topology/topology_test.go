package topology

import (
	"github.com/bary321/seaweedfs-1/weed/pb/master_pb"
	"github.com/bary321/seaweedfs-1/weed/sequence"
	"github.com/bary321/seaweedfs-1/weed/storage"
	"github.com/bary321/seaweedfs-1/weed/storage/needle"
	"github.com/bary321/seaweedfs-1/weed/storage/super_block"

	"testing"
)

func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.GetActiveVolumeCount() != 15 {
		t.Fail()
	}
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.GetActiveVolumeCount() != 12 {
		t.Fail()
	}
}

func TestHandlingVolumeServerHeartbeat(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, "127.0.0.1", 25)

	{
		volumeCount := 7
		var volumeMessages []*master_pb.VolumeInformationMessage
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(25432),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 34524,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.CurrentVersion),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		topo.SyncDataNodeRegistration(volumeMessages, dn)

		assert(t, "activeVolumeCount1", int(topo.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(topo.volumeCount), volumeCount)
	}

	{
		volumeCount := 7 - 1
		var volumeMessages []*master_pb.VolumeInformationMessage
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(254320),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 345240,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.CurrentVersion),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}
		topo.SyncDataNodeRegistration(volumeMessages, dn)

		//rp, _ := storage.NewReplicaPlacementFromString("000")
		//layout := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL)
		//assert(t, "writables", len(layout.writables), volumeCount)

		assert(t, "activeVolumeCount1", int(topo.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(topo.volumeCount), volumeCount)
	}

	{
		volumeCount := 6
		newVolumeShortMessage := &master_pb.VolumeShortInformationMessage{
			Id:               uint32(3),
			Collection:       "",
			ReplicaPlacement: uint32(0),
			Version:          uint32(needle.CurrentVersion),
			Ttl:              0,
		}
		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)
		rp, _ := super_block.NewReplicaPlacementFromString("000")
		layout := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL)
		assert(t, "writables after repeated add", len(layout.writables), volumeCount)

		assert(t, "activeVolumeCount1", int(topo.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(topo.volumeCount), volumeCount)

		topo.IncrementalSyncDataNodeRegistration(
			nil,
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			dn)
		assert(t, "writables after deletion", len(layout.writables), volumeCount-1)
		assert(t, "activeVolumeCount1", int(topo.activeVolumeCount), volumeCount-1)
		assert(t, "volumeCount", int(topo.volumeCount), volumeCount-1)

		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)

		for vid, _ := range layout.vid2location {
			println("after add volume id", vid)
		}
		for _, vid := range layout.writables {
			println("after add writable volume id", vid)
		}

		assert(t, "writables after add back", len(layout.writables), volumeCount)

	}

	topo.UnRegisterDataNode(dn)

	assert(t, "activeVolumeCount2", int(topo.activeVolumeCount), 0)

}

func assert(t *testing.T, message string, actual, expected int) {
	if actual != expected {
		t.Fatalf("unexpected %s: %d, expected: %d", message, actual, expected)
	}
}

func TestAddRemoveVolume(t *testing.T) {

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, "127.0.0.1", 25)

	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             100,
		Collection:       "xcollection",
		FileCount:        123,
		DeleteCount:      23,
		DeletedByteCount: 45,
		ReadOnly:         false,
		Version:          needle.CurrentVersion,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}

	dn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn)
	topo.RegisterVolumeLayout(v, dn)

	if _, hasCollection := topo.FindCollection(v.Collection); !hasCollection {
		t.Errorf("collection %v should exist", v.Collection)
	}

	topo.UnRegisterVolumeLayout(v, dn)

	if _, hasCollection := topo.FindCollection(v.Collection); hasCollection {
		t.Errorf("collection %v should not exist", v.Collection)
	}

}
