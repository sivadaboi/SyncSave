package syncengine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SyncCoordinator checks differences between local and remote file states
type SyncCoordinator struct {
	LocalIndex *SafeFileInfo
	RootPath   string
	NodeID     string
}

// SyncDiff contains the actions that need to be performed on the local node
type SyncDiff struct {
	PullFiles       []FileInfo
	DeleteLocally   []FileInfo
	Conflicts       []FileInfo
}

func NewSyncCoordinator(rootPath string, localIndex *SafeFileInfo, nodeID string) *SyncCoordinator {
	return &SyncCoordinator{
		LocalIndex: localIndex,
		RootPath:   rootPath,
		NodeID:     nodeID,
	}
}

// Diff compares local index against a remote index and returns lists of files to pull, delete, or conflict
func (sc *SyncCoordinator) Diff(remoteFiles map[string]FileInfo) SyncDiff {
	diff := SyncDiff{}

	// Create set of all unique file names
	allFiles := make(map[string]bool)
	sc.LocalIndex.Range(func(name string, fi FileInfo) bool {
		allFiles[name] = true
		return true
	})
	for name := range remoteFiles {
		allFiles[name] = true
	}

	for name := range allFiles {
		localFi, localExists := sc.LocalIndex.Load(name)
		remoteFi, remoteExists := remoteFiles[name]

		if !remoteExists {
			// File does not exist on remote. Local is newer or untracked.
			// Syncthing behavior: if remote doesn't know about it, we keep local
			// and let the remote pull it when it receives our index.
			continue
		}

		if !localExists {
			// File exists on remote but not locally.
			if !remoteFi.IsDeleted {
				diff.PullFiles = append(diff.PullFiles, remoteFi)
			}
			continue
		}

		// Both exist. Compare version vectors.
		comparison := localFi.VersionVector.Compare(remoteFi.VersionVector)

		switch comparison {
		case Older:
			// Remote is newer.
			if remoteFi.IsDeleted {
				if !localFi.IsDeleted {
					diff.DeleteLocally = append(diff.DeleteLocally, remoteFi)
				}
			} else {
				diff.PullFiles = append(diff.PullFiles, remoteFi)
			}
		case Concurrent:
			// Concurrent edits. If hashes differ, we have a conflict!
			if !remoteFi.IsDeleted && !localFi.IsDeleted {
				if filesHaveChanged(localFi, remoteFi) {
					diff.Conflicts = append(diff.Conflicts, remoteFi)
				}
			} else if remoteFi.IsDeleted && !localFi.IsDeleted {
				// One deleted, one modified. Conflict!
				diff.Conflicts = append(diff.Conflicts, remoteFi)
			} else if !remoteFi.IsDeleted && localFi.IsDeleted {
				// Remote recreated it, local deleted it. Pull remote.
				diff.PullFiles = append(diff.PullFiles, remoteFi)
			}
		case Newer, Equal:
			// Local is newer or identical. Do nothing.
		}
	}

	// Sort directories to pull/delete to avoid nested dependencies issues
	// Sort PullFiles ascending by directory structure depth (so folders are created before files)
	sort.Slice(diff.PullFiles, func(i, j int) bool {
		return len(filepath.SplitList(diff.PullFiles[i].Name)) < len(filepath.SplitList(diff.PullFiles[j].Name))
	})
	// Sort DeleteLocally descending (so nested files are deleted before parent folders)
	sort.Slice(diff.DeleteLocally, func(i, j int) bool {
		return len(filepath.SplitList(diff.DeleteLocally[i].Name)) > len(filepath.SplitList(diff.DeleteLocally[j].Name))
	})

	return diff
}

// ResolveConflict renames the local conflicting file to conflict format and updates indexes
func (sc *SyncCoordinator) ResolveConflict(remoteFi FileInfo, peerID string) (string, error) {
	localFi, exists := sc.LocalIndex.Load(remoteFi.Name)
	if !exists || localFi.IsDeleted {
		return "", nil // No local conflict to rename
	}

	// Calculate conflict path: filename.sync-conflict-[YYYYMMDD]-[HHMMSS]-[peerID].ext
	dir := filepath.Dir(remoteFi.Name)
	ext := filepath.Ext(remoteFi.Name)
	base := filepath.Base(remoteFi.Name)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	timestamp := time.Now().Format("20060102-150405")
	conflictRelName := filepath.Join(dir, fmt.Sprintf("%s.sync-conflict-%s-%s%s", nameWithoutExt, timestamp, peerID, ext))
	conflictRelName = filepath.ToSlash(conflictRelName)

	fullLocalPath := filepath.Join(sc.RootPath, localFi.Name)
	fullConflictPath := filepath.Join(sc.RootPath, conflictRelName)

	// Rename local file
	if err := os.Rename(fullLocalPath, fullConflictPath); err != nil {
		return "", err
	}

	// Index the conflict copy as a new local file with its own version vector
	conflictVV := localFi.VersionVector.Copy()
	conflictVV.Increment(sc.NodeID)

	// Fetch file stat
	stat, err := os.Stat(fullConflictPath)
	if err == nil {
		confFi, err := HashFile(fullConflictPath, conflictRelName, stat)
		if err == nil {
			confFi.VersionVector = conflictVV
			sc.LocalIndex.Store(confFi)
		}
	}

	return conflictRelName, nil
}

// AssembleFile atomically puts blocks together from local reuse and remote fetches
func (sc *SyncCoordinator) AssembleFile(remoteFi FileInfo, fetchBlockFunc func(relPath string, block BlockInfo) ([]byte, error)) error {
	fullTargetPath := filepath.Join(sc.RootPath, remoteFi.Name)
	dirPath := filepath.Dir(fullTargetPath)

	// Ensure parent directories exist
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	// Handle directory replication
	if remoteFi.IsDirectory {
		return os.MkdirAll(fullTargetPath, 0755)
	}

	// Create temp file
	tempFileName := filepath.Join(dirPath, fmt.Sprintf(".syncsave.%s.tmp", filepath.Base(remoteFi.Name)))
	tempFile, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		tempFile.Close()
		_ = os.Remove(tempFileName) // Clean up temp file on failure
	}()

	// Try opening the current local file to reuse unchanged blocks
	var localFile *os.File
	if _, err := os.Stat(fullTargetPath); err == nil {
		localFile, _ = os.Open(fullTargetPath)
	}
	if localFile != nil {
		defer localFile.Close()
	}

	localFi, hasLocal := sc.LocalIndex.Load(remoteFi.Name)

	// Process each block
	for _, block := range remoteFi.Blocks {
		var blockData []byte
		reused := false

		// Check if we can reuse this block from our local version of the file
		if hasLocal && !localFi.IsDeleted && localFile != nil {
			for _, localBlock := range localFi.Blocks {
				if localBlock.Hash == block.Hash && localBlock.Size == block.Size {
					// Identical block! Read directly from local file
					data := make([]byte, block.Size)
					_, err := localFile.ReadAt(data, localBlock.Offset)
					if err == nil {
						blockData = data
						reused = true
						break
					}
				}
			}
		}

		// Block not present locally; pull it from the peer
		if !reused {
			data, err := fetchBlockFunc(remoteFi.Name, block)
			if err != nil {
				return fmt.Errorf("failed to fetch block %s offset %d: %w", block.Hash, block.Offset, err)
			}
			blockData = data
		}

		// Write block to temp file
		if _, err := tempFile.WriteAt(blockData, block.Offset); err != nil {
			return err
		}
	}

	// Close temp file so we can rename it
	if err := tempFile.Close(); err != nil {
		return err
	}

	// Verify complete assembly matches remote file size
	tempStat, err := os.Stat(tempFileName)
	if err != nil {
		return err
	}
	if tempStat.Size() != remoteFi.Size {
		return fmt.Errorf("size mismatch on file assembly: got %d, expected %d", tempStat.Size(), remoteFi.Size)
	}

	// Atomic rename
	if err := os.Rename(tempFileName, fullTargetPath); err != nil {
		return err
	}

	// Update local index
	sc.LocalIndex.Store(remoteFi)

	return nil
}

// Helper: compares blocks or hashes
func filesHaveChanged(fi1, fi2 FileInfo) bool {
	if fi1.Size != fi2.Size {
		return true
	}
	if len(fi1.Blocks) != len(fi2.Blocks) {
		return true
	}
	for i := range fi1.Blocks {
		if fi1.Blocks[i].Hash != fi2.Blocks[i].Hash {
			return true
		}
	}
	return false
}
