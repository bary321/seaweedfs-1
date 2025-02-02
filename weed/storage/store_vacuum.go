package storage

import (
	"fmt"
	"github.com/bary321/seaweedfs-1/weed/stats"

	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/storage/needle"
)

func (s *Store) CheckCompactVolume(volumeId needle.VolumeId) (float64, error) {
	if v := s.findVolume(volumeId); v != nil {
		glog.V(3).Infof("volumd %d garbage level: %f", volumeId, v.garbageLevel())
		return v.garbageLevel(), nil
	}
	return 0, fmt.Errorf("volume id %d is not found during check compact", volumeId)
}
func (s *Store) CompactVolume(vid needle.VolumeId, preallocate int64, compactionBytePerSecond int64) error {
	if v := s.findVolume(vid); v != nil {
		s := stats.NewDiskStatus(v.dir)
		if int64(s.Free) < preallocate {
			return fmt.Errorf("free space: %d bytes, not enough for %d bytes", s.Free, preallocate)
		}
		return v.Compact2(preallocate, compactionBytePerSecond)
	}
	return fmt.Errorf("volume id %d is not found during compact", vid)
}
func (s *Store) CommitCompactVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.CommitCompact()
	}
	return fmt.Errorf("volume id %d is not found during commit compact", vid)
}
func (s *Store) CommitCleanupVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up", vid)
}
