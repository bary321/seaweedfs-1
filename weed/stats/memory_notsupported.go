// +build !linux

package stats

import "github.com/bary321/seaweedfs-1/weed/pb/volume_server_pb"

func fillInMemStatus(status *volume_server_pb.MemStatus) {
	return
}
