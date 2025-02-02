package filer

import (
	"context"
	"fmt"

	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/pb/filer_pb"
	"github.com/bary321/seaweedfs-1/weed/pb/master_pb"
	"github.com/bary321/seaweedfs-1/weed/util"
)

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32) (err error) {
	if p == "/" {
		return nil
	}

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}

	isCollection := f.isBucket(entry)

	var chunks []*filer_pb.FileChunk
	chunks = append(chunks, entry.Chunks...)
	if entry.IsDirectory() {
		// delete the folder children, not including the folder itself
		var dirChunks []*filer_pb.FileChunk
		dirChunks, err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !isCollection, isFromOtherCluster, signatures)
		if err != nil {
			glog.V(0).Infof("delete directory %s: %v", p, err)
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
		chunks = append(chunks, dirChunks...)
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks, isFromOtherCluster, signatures)
	if err != nil {
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if shouldDeleteChunks && !isCollection {
		go f.DeleteChunks(chunks)
	}
	if isCollection {
		collectionName := entry.Name()
		f.doDeleteCollection(collectionName)
		f.deleteBucket(collectionName)
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32) (chunks []*filer_pb.FileChunk, err error) {

	lastFileName := ""
	includeLastFile := false
	for {
		entries, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "")
		if err != nil {
			glog.Errorf("list folder %s: %v", entry.FullPath, err)
			return nil, fmt.Errorf("list folder %s: %v", entry.FullPath, err)
		}
		if lastFileName == "" && !isRecursive && len(entries) > 0 {
			// only for first iteration in the loop
			glog.Errorf("deleting a folder %s has children: %+v ...", entry.FullPath, entries[0].Name())
			return nil, fmt.Errorf("fail to delete non-empty folder: %s", entry.FullPath)
		}

		for _, sub := range entries {
			lastFileName = sub.Name()
			var dirChunks []*filer_pb.FileChunk
			if sub.IsDirectory() {
				dirChunks, err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks, false, nil)
				chunks = append(chunks, dirChunks...)
			} else {
				f.NotifyUpdateEvent(ctx, sub, nil, shouldDeleteChunks, isFromOtherCluster, nil)
				chunks = append(chunks, sub.Chunks...)
			}
			if err != nil && !ignoreRecursiveError {
				return nil, err
			}
		}

		if len(entries) < PaginationSize {
			break
		}
	}

	glog.V(3).Infof("deleting directory %v delete %d chunks: %v", entry.FullPath, len(chunks), shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
		return nil, fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}

	f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)

	return chunks, nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).Infof("deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteEntry(ctx, entry.FullPath); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}
	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

func (f *Filer) doDeleteCollection(collectionName string) (err error) {

	return f.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.Infof("delete collection %s: %v", collectionName, err)
		}
		return err
	})

}
