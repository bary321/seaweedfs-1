package storage

import (
	"testing"

	"github.com/bary321/seaweedfs-1/weed/storage/needle"
)

func TestSortVolumeInfos(t *testing.T) {
	vis := []*VolumeInfo{
		&VolumeInfo{
			Id: 2,
		},
		&VolumeInfo{
			Id: 1,
		},
		&VolumeInfo{
			Id: 3,
		},
	}
	sortVolumeInfos(vis)
	for i := 0; i < len(vis); i++ {
		if vis[i].Id != needle.VolumeId(i+1) {
			t.Fatal()
		}
	}
}
