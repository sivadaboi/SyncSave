package watcher

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher manages recursive filesystem event watching with channel-based debouncing
type Watcher struct {
	fswatcher *fsnotify.Watcher
	mu        sync.Mutex
	paths     map[string]bool          // Tracked directories
	timers    map[string]*time.Timer   // Debounce timers keyed by root watched path
	roots     map[string]string        // Maps subdirectory -> root watched directory
	Events    chan string              // Emits the root watched path when events settle
	stop      chan struct{}
	wg        sync.WaitGroup
}

// NewWatcher initializes a filesystem watcher
func NewWatcher() (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		fswatcher: fsw,
		paths:     make(map[string]bool),
		timers:    make(map[string]*time.Timer),
		roots:     make(map[string]string),
		Events:    make(chan string, 100),
		stop:      make(chan struct{}),
	}, nil
}

// Watch recursively monitors a directory and associates events with its root
func (w *Watcher) Watch(dirPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dirPath = filepath.Clean(dirPath)

	// Recursively walk and watch all directories
	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			cleanPath := filepath.Clean(path)
			if !w.paths[cleanPath] {
				if err := w.fswatcher.Add(cleanPath); err != nil {
					return err
				}
				w.paths[cleanPath] = true
				w.roots[cleanPath] = dirPath
			}
		}
		return nil
	})

	return err
}

// Unwatch stops watching a directory and all its subfolders
func (w *Watcher) Unwatch(dirPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dirPath = filepath.Clean(dirPath)

	for p := range w.paths {
		if p == dirPath || isSubdirectory(dirPath, p) {
			_ = w.fswatcher.Remove(p)
			delete(w.paths, p)
			delete(w.roots, p)
		}
	}
	return nil
}

// Start kicks off the background fsnotify listener event loop
func (w *Watcher) Start(debounceDuration time.Duration) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case event, ok := <-w.fswatcher.Events:
				if !ok {
					return
				}

				cleanPath := filepath.Clean(event.Name)
				
				w.mu.Lock()
				// Dynamic subfolder tracking
				if event.Has(fsnotify.Create) {
					info, err := os.Stat(cleanPath)
					if err == nil && info.IsDir() {
						// Map new folder to same root
						parentDir := filepath.Dir(cleanPath)
						if root, exists := w.roots[parentDir]; exists {
							_ = w.fswatcher.Add(cleanPath)
							w.paths[cleanPath] = true
							w.roots[cleanPath] = root
						}
					}
				}

				// Find root watched directory for the modified path
				var rootPath string
				for p, root := range w.roots {
					if cleanPath == p || isSubdirectory(p, cleanPath) {
						rootPath = root
						break
					}
				}
				w.mu.Unlock()

				if rootPath == "" {
					continue // Ignore untracked paths
				}

				// Debounce event
				w.mu.Lock()
				if timer, ok := w.timers[rootPath]; ok {
					timer.Stop()
				}

				w.timers[rootPath] = time.AfterFunc(debounceDuration, func() {
					w.mu.Lock()
					delete(w.timers, rootPath)
					w.mu.Unlock()
					
					// Send settled event root path
					w.Events <- rootPath
				})
				w.mu.Unlock()

			case err, ok := <-w.fswatcher.Errors:
				if !ok {
					return
				}
				fmt.Fprintf(os.Stderr, "[Watcher] fsnotify error: %v\n", err)

			case <-w.stop:
				return
			}
		}
	}()
}

// Close gracefully terminates the watcher
func (w *Watcher) Close() {
	close(w.stop)
	_ = w.fswatcher.Close()
	w.wg.Wait()
	w.mu.Lock()
	for _, timer := range w.timers {
		timer.Stop()
	}
	w.mu.Unlock()
}

// Helper: check if sub is under parent
func isSubdirectory(parent, sub string) bool {
	rel, err := filepath.Rel(parent, sub)
	if err != nil {
		return false
	}
	return rel != ".." && !filepath.HasPrefix(rel, ".."+string(filepath.Separator))
}
