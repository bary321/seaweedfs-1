package filesys

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/bary321/seaweedfs-1/weed/filer"
	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

var _ = fs.NodeSymlinker(&Dir{})
var _ = fs.NodeReadlinker(&File{})

func (dir *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {

	glog.V(4).Infof("Symlink: %v/%v to %v", dir.FullPath(), req.NewName, req.Target)

	request := &filer_pb.CreateEntryRequest{
		Directory: dir.FullPath(),
		Entry: &filer_pb.Entry{
			Name:        req.NewName,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:         time.Now().Unix(),
				Crtime:        time.Now().Unix(),
				FileMode:      uint32((os.FileMode(0777) | os.ModeSymlink) &^ dir.wfs.option.Umask),
				Uid:           req.Uid,
				Gid:           req.Gid,
				SymlinkTarget: req.Target,
			},
		},
		Signatures: []int32{dir.wfs.signature},
	}

	err := dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer dir.wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("symlink %s/%s: %v", dir.FullPath(), req.NewName, err)
			return fuse.EIO
		}

		dir.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	symlink := dir.newFile(req.NewName, request.Entry)

	return symlink, err

}

func (file *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {

	if err := file.maybeLoadEntry(ctx); err != nil {
		return "", err
	}

	if os.FileMode(file.entry.Attributes.FileMode)&os.ModeSymlink == 0 {
		return "", fuse.Errno(syscall.EINVAL)
	}

	glog.V(4).Infof("Readlink: %v/%v => %v", file.dir.FullPath(), file.Name, file.entry.Attributes.SymlinkTarget)

	return file.entry.Attributes.SymlinkTarget, nil

}
