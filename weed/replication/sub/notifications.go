package sub

import (
	"github.com/bary321/seaweedfs-1/weed/pb/filer_pb"
	"github.com/bary321/seaweedfs-1/weed/util"
)

type NotificationInput interface {
	// GetName gets the name to locate the configuration in sync.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	ReceiveMessage() (key string, message *filer_pb.EventNotification, err error)
}

var (
	NotificationInputs []NotificationInput
)
