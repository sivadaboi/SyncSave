package syncengine

import (
	"encoding/json"
	"sync"
)

// BlockInfo represents a chunk of a file for differential transfers
type BlockInfo struct {
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	Hash   string `json:"hash"` // Hex-encoded SHA-256 hash
}

// VersionVector maps a Node ID to its generation counter
type VersionVector map[string]uint64

// Copy returns a deep copy of the VersionVector
func (vv VersionVector) Copy() VersionVector {
	cp := make(VersionVector, len(vv))
	for k, v := range vv {
		cp[k] = v
	}
	return cp
}

// Increment increments the counter for a given node
func (vv VersionVector) Increment(nodeID string) {
	vv[nodeID]++
}

// Merge combines two version vectors, taking the maximum of each counter
func (vv VersionVector) Merge(other VersionVector) {
	for node, count := range other {
		if cur, exists := vv[node]; !exists || count > cur {
			vv[node] = count
		}
	}
}

// VectorComparison represents the relationship between two version vectors
type VectorComparison int

const (
	Equal VectorComparison = iota
	Newer
	Older
	Concurrent
)

// Compare returns the relation of vv relative to other (e.g. vv is Newer, Older, Equal, or Concurrent)
func (vv VersionVector) Compare(other VersionVector) VectorComparison {
	vvNewer := false
	otherNewer := false

	// Compare nodes in both vectors
	allNodes := make(map[string]bool)
	for n := range vv {
		allNodes[n] = true
	}
	for n := range other {
		allNodes[n] = true
	}

	for node := range allNodes {
		valSelf := vv[node]
		valOther := other[node]

		if valSelf > valOther {
			vvNewer = true
		} else if valSelf < valOther {
			otherNewer = true
		}
	}

	if vvNewer && otherNewer {
		return Concurrent
	}
	if vvNewer {
		return Newer
	}
	if otherNewer {
		return Older
	}
	return Equal
}

// FileInfo represents the complete state of a tracked file or directory
type FileInfo struct {
	Name          string        `json:"name"` // Relative path, e.g. "player.dat"
	Size          int64         `json:"size"`
	ModifiedTime  int64         `json:"modifiedTime"` // Unix timestamp in milliseconds
	IsDirectory   bool          `json:"isDirectory"`
	IsDeleted     bool          `json:"isDeleted"`
	Blocks        []BlockInfo   `json:"blocks"`
	VersionVector VersionVector `json:"versionVector"`
}

// SafeFileInfo is a thread-safe map of FileInfo metadata
type SafeFileInfo struct {
	mu    sync.RWMutex
	files map[string]*FileInfo
}

func NewSafeFileInfo() *SafeFileInfo {
	return &SafeFileInfo{
		files: make(map[string]*FileInfo),
	}
}

func (sf *SafeFileInfo) Load(name string) (FileInfo, bool) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	fi, exists := sf.files[name]
	if !exists {
		return FileInfo{}, false
	}
	return *fi, true
}

func (sf *SafeFileInfo) Store(fi FileInfo) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.files[fi.Name] = &fi
}

func (sf *SafeFileInfo) Delete(name string) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	delete(sf.files, name)
}

func (sf *SafeFileInfo) Range(f func(name string, fi FileInfo) bool) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	for name, fi := range sf.files {
		if !f(name, *fi) {
			break
		}
	}
}

func (sf *SafeFileInfo) ToMap() map[string]FileInfo {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	m := make(map[string]FileInfo, len(sf.files))
	for name, fi := range sf.files {
		m[name] = *fi
	}
	return m
}

func (sf *SafeFileInfo) LoadFromMap(m map[string]FileInfo) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.files = make(map[string]*FileInfo, len(m))
	for name, fi := range m {
		val := fi
		sf.files[name] = &val
	}
}

func (sf *SafeFileInfo) MarshalJSON() ([]byte, error) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return json.Marshal(sf.files)
}

func (sf *SafeFileInfo) UnmarshalJSON(b []byte) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	var raw map[string]*FileInfo
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	sf.files = raw
	return nil
}
