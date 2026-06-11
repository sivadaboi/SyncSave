package syncengine

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Dynamic Block Size Boundaries
const (
	BlockSize128K = 128 * 1024
	BlockSize256K = 256 * 1024
	BlockSize512K = 512 * 1024
	BlockSize1M   = 1024 * 1024
)

// GetBlockSize determines the block size to use based on the file size
func GetBlockSize(fileSize int64) int64 {
	if fileSize < 1*1024*1024 { // < 1 MiB
		return BlockSize128K
	}
	if fileSize < 10*1024*1024 { // < 10 MiB
		return BlockSize256K
	}
	if fileSize < 50*1024*1024 { // < 50 MiB
		return BlockSize512K
	}
	return BlockSize1M
}

// HashFile hashes a file on disk into a list of BlockInfo chunks
func HashFile(fullPath string, relPath string, info os.FileInfo) (FileInfo, error) {
	file, err := os.Open(fullPath)
	if err != nil {
		return FileInfo{}, err
	}
	defer file.Close()

	fileSize := info.Size()
	blockSize := GetBlockSize(fileSize)
	blocks := []BlockInfo{}

	buffer := make([]byte, blockSize)
	var offset int64 = 0

	for {
		bytesRead, err := file.Read(buffer)
		if bytesRead > 0 {
			hasher := sha256.New()
			hasher.Write(buffer[:bytesRead])
			hashStr := hex.EncodeToString(hasher.Sum(nil))

			blocks = append(blocks, BlockInfo{
				Offset: offset,
				Size:   int64(bytesRead),
				Hash:   hashStr,
			})
			offset += int64(bytesRead)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return FileInfo{}, err
		}
	}

	return FileInfo{
		Name:          relPath,
		Size:          fileSize,
		ModifiedTime:  info.ModTime().UnixNano() / int64(time.Millisecond),
		IsDirectory:   false,
		IsDeleted:     false,
		Blocks:        blocks,
		VersionVector: make(VersionVector),
	}, nil
}

// ScanDirectory walks a directory, updates the local manifest index, and increments version vectors
func ScanDirectory(rootPath string, localIndex *SafeFileInfo, nodeID string) error {
	cleanRoot := filepath.Clean(rootPath)
	seenFiles := make(map[string]bool)

	err := filepath.Walk(cleanRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == cleanRoot {
			return nil
		}

		relPath, err := filepath.Rel(cleanRoot, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath) // Ensure cross-platform forward slashes

		seenFiles[relPath] = true

		// Handle directory scan
		if info.IsDir() {
			existing, exists := localIndex.Load(relPath)
			if !exists || !existing.IsDirectory || existing.IsDeleted {
				vv := make(VersionVector)
				if exists {
					vv = existing.VersionVector.Copy()
				}
				vv.Increment(nodeID)

				localIndex.Store(FileInfo{
					Name:          relPath,
					Size:          0,
					ModifiedTime:  info.ModTime().UnixNano() / int64(time.Millisecond),
					IsDirectory:   true,
					IsDeleted:     false,
					VersionVector: vv,
				})
			}
			return nil
		}

		// Handle file scan
		existing, exists := localIndex.Load(relPath)
		fileMtimeMs := info.ModTime().UnixNano() / int64(time.Millisecond)

		// Check if file is new or modified
		isModified := false
		if !exists || existing.IsDeleted || existing.IsDirectory {
			isModified = true
		} else if existing.Size != info.Size() || existing.ModifiedTime != fileMtimeMs {
			isModified = true
		}

		if isModified {
			// Compute block hashes
			fi, err := HashFile(path, relPath, info)
			if err != nil {
				return err
			}

			// Keep existing version vector and increment our node
			vv := make(VersionVector)
			if exists {
				vv = existing.VersionVector.Copy()
			}
			vv.Increment(nodeID)
			fi.VersionVector = vv

			localIndex.Store(fi)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Detect local deletions: files present in local index but missing on disk
	localIndex.Range(func(name string, fi FileInfo) bool {
		if !seenFiles[name] && !fi.IsDeleted {
			// File/directory was deleted locally
			vv := fi.VersionVector.Copy()
			vv.Increment(nodeID)

			fi.IsDeleted = true
			fi.Size = 0
			fi.Blocks = nil
			fi.ModifiedTime = time.Now().UnixNano() / int64(time.Millisecond)
			fi.VersionVector = vv

			localIndex.Store(fi)
		}
		return true
	})

	return nil
}

// PeriodicScanner runs a background tick to reconcile disk state as a fallback
type PeriodicScanner struct {
	ticker     *time.Ticker
	stop       chan struct{}
	rootPath   string
	localIndex *SafeFileInfo
	nodeID     string
	mu         sync.Mutex
}

func NewPeriodicScanner(rootPath string, localIndex *SafeFileInfo, nodeID string) *PeriodicScanner {
	return &PeriodicScanner{
		rootPath:   rootPath,
		localIndex: localIndex,
		nodeID:     nodeID,
		stop:       make(chan struct{}),
	}
}

func (ps *PeriodicScanner) Start(interval time.Duration) {
	ps.ticker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ps.ticker.C:
				ps.mu.Lock()
				_ = ScanDirectory(ps.rootPath, ps.localIndex, ps.nodeID)
				ps.mu.Unlock()
			case <-ps.stop:
				return
			}
		}
	}()
}

func (ps *PeriodicScanner) Stop() {
	if ps.ticker != nil {
		ps.ticker.Stop()
	}
	close(ps.stop)
}
