package meta_cache

import (
	"context"
	"fmt"

	"github.com/bary321/seaweedfs-1/weed/filer"
	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/pb/filer_pb"
	"github.com/bary321/seaweedfs-1/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) {

	mc.visitedBoundary.EnsureVisited(dirPath, func(path util.FullPath) (childDirectories []string, err error) {

		glog.V(4).Infof("ReadDirAllEntries %s ...", path)

		err = filer_pb.ReadDirAllEntries(client, dirPath, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
			entry := filer.FromPbEntry(string(dirPath), pbEntry)
			if err := mc.doInsertEntry(context.Background(), entry); err != nil {
				glog.V(0).Infof("read %s: %v", entry.FullPath, err)
				return err
			}
			if entry.IsDirectory() {
				childDirectories = append(childDirectories, entry.Name())
			}
			return nil
		})
		if err != nil {
			err = fmt.Errorf("list %s: %v", dirPath, err)
		}
		return
	})
}
