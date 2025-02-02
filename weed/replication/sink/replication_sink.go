package sink

import (
	"github.com/bary321/seaweedfs-1/weed/pb/filer_pb"
	"github.com/bary321/seaweedfs-1/weed/replication/source"
	"github.com/bary321/seaweedfs-1/weed/util"
)

type ReplicationSink interface {
	GetName() string
	Initialize(configuration util.Configuration, prefix string) error
	DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error
	CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error
	UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error)
	GetSinkToDirectory() string
	SetSourceFiler(s *source.FilerSource)
}

var (
	Sinks []ReplicationSink
)
