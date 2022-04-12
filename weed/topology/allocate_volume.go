package topology

import (
	"context"

	"github.com/bary321/seaweedfs-1/weed/operation"
	"github.com/bary321/seaweedfs-1/weed/pb/volume_server_pb"
	"github.com/bary321/seaweedfs-1/weed/storage/needle"
	"google.golang.org/grpc"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(dn.Url(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		_, deleteErr := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
			VolumeId:           uint32(vid),
			Collection:         option.Collection,
			Replication:        option.ReplicaPlacement.String(),
			Ttl:                option.Ttl.String(),
			Preallocate:        option.Prealloacte,
			MemoryMapMaxSizeMb: option.MemoryMapMaxSizeMb,
		})
		return deleteErr
	})

}
