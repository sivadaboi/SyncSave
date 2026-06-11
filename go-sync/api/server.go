package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sivadaboi/syncsave-go/db"
	"github.com/sivadaboi/syncsave-go/syncengine"
	"github.com/sivadaboi/syncsave-go/watcher"
)

type Server struct {
	db          *db.Database
	watcher     *watcher.Watcher
	indexes     map[string]*syncengine.SafeFileInfo // In-memory version vectors per game
	indexesMu   sync.RWMutex
	nodeID      string
	port        int
	server      *http.Server
	WANSyncFunc func(gameID string, peer db.PeerSchema) error
	// Track handshake states
	pairingRequests     map[string]map[string]interface{}
	sentPairingRequests map[string]time.Time
	mu                  sync.Mutex
}

func NewServer(database *db.Database, w *watcher.Watcher) *Server {
	settings := database.GetSettings()
	return &Server{
		db:                  database,
		watcher:             w,
		indexes:             make(map[string]*syncengine.SafeFileInfo),
		nodeID:              settings.NodeID,
		port:                settings.Port,
		pairingRequests:     make(map[string]map[string]interface{}),
		sentPairingRequests: make(map[string]time.Time),
	}
}

func (s *Server) Start() error {
	// Initialize manifests from disk for all tracked games
	for gameID := range s.db.GetGames() {
		s.GetOrLoadIndex(gameID)
	}

	mux := http.NewServeMux()

	// REST API Routes
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/games", s.handleGames)
	mux.HandleFunc("/api/peers", s.handlePeers)
	mux.HandleFunc("/api/peers/pair", s.handlePeersPair)
	mux.HandleFunc("/api/peers/approve", s.handlePeersApprove)
	mux.HandleFunc("/api/peers/reject", s.handlePeersReject)
	mux.HandleFunc("/api/peers/unpair", s.handlePeersUnpair)
	mux.HandleFunc("/api/peers/probe", s.handlePeersProbe)
	mux.HandleFunc("/api/presets/scan", s.handlePresetsScan)
	mux.HandleFunc("/api/browse-directory", s.handleBrowseDirectory)
	mux.HandleFunc("/api/browse-file", s.handleBrowseFile)
	mux.HandleFunc("/api/relay/ips", s.handleRelayIPs)
	mux.HandleFunc("/api/relay/health", s.handleRelayHealth)
	mux.HandleFunc("/api/wan/status", s.handleWanStatus)

	// Game actions
	mux.HandleFunc("/api/games/", s.handleGameActions) // covers /launch, /snapshot, /rollback, /sync, /branch

	// P2P Routes (called by remote daemons)
	mux.HandleFunc("/api/p2p/ping", s.handleP2PPing)
	mux.HandleFunc("/api/p2p/handshake", s.handleP2PHandshake)
	mux.HandleFunc("/api/p2p/approve-confirm", s.handleP2PApproveConfirm)
	mux.HandleFunc("/api/p2p/unpair", s.handleP2PUnpair)
	mux.HandleFunc("/api/p2p/manifest/", s.handleP2PManifest)
	mux.HandleFunc("/api/p2p/blocks/", s.handleP2PBlocks)
	mux.HandleFunc("/api/p2p/delete-file/", s.handleP2PDeleteFile)

	// Mock window endpoints for Electron UI window control
	mux.HandleFunc("/api/window/minimize", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/api/window/maximize", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/api/window/close", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	// Cloud backup endpoints
	mux.HandleFunc("/api/auth/start", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/api/auth/disconnect", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/api/cloud/snapshots/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]interface{}{"snapshots": []interface{}{}})
	})

	// Backup export/restore
	mux.HandleFunc("/api/backup/export", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]string{"success": "true", "backupPath": "mock-backup.sscb"})
	})
	mux.HandleFunc("/api/backup/restore", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]string{"success": "true"})
	})

	// Serve Static Files (Frontend UI)
	frontendDir := s.findFrontendDir()
	if frontendDir != "" {
		fileServer := http.FileServer(http.Dir(frontendDir))
		mux.Handle("/", fileServer)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			fmt.Fprint(w, "Frontend assets not found on disk. Place 'src/frontend/' relative to the daemon.")
		})
	}

	// CORS wrapper
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		mux.ServeHTTP(w, r)
	})

	s.server = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", s.port),
		Handler: handler,
	}

	fmt.Printf("[API] Server listening on http://localhost:%d\n", s.port)
	return s.server.ListenAndServe()
}

func (s *Server) Stop() {
	if s.server != nil {
		_ = s.server.Close()
	}
}

// ── REST Handlers ──────────────────────────────────────────────────────────

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	settings := s.db.GetSettings()
	jsonResponse(w, 200, map[string]interface{}{
		"status":     "ok",
		"version":    "1.1.4",
		"deviceName": settings.DeviceName,
		"deviceType": settings.DeviceType,
		"nodeId":     s.nodeID,
	})
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		jsonResponse(w, 200, s.db.GetSettings())
		return
	}
	if r.Method == "POST" {
		var newSettings db.SettingsSchema
		if err := json.NewDecoder(r.Body).Decode(&newSettings); err != nil {
			jsonError(w, 400, err.Error())
			return
		}
		s.db.UpdateSettings(newSettings)
		jsonResponse(w, 200, s.db.GetSettings())
		return
	}
}

func (s *Server) handleGames(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		jsonResponse(w, 200, s.db.GetGames())
		return
	}
	if r.Method == "POST" {
		var req struct {
			Name     string `json:"name"`
			SavePath string `json:"savePath"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, 400, err.Error())
			return
		}

		game, err := s.db.AddGame(req.Name, req.SavePath)
		if err != nil {
			jsonError(w, 400, err.Error())
			return
		}

		// Load manifest index
		localIndex := s.GetOrLoadIndex(game.ID)

		// Watch directory
		_ = s.watcher.Watch(game.SavePath)

		// Initial hash scan in background
		go func() {
			_ = syncengine.ScanDirectory(game.SavePath, localIndex, s.nodeID)
			s.SaveIndexToDisk(game.ID)
		}()

		jsonResponse(w, 201, game)
		return
	}
}

func (s *Server) handleGameActions(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	// Path layout: /api/games/:gameId/sync
	if len(parts) < 4 {
		jsonError(w, 404, "Not Found")
		return
	}

	gameID := parts[3]
	action := ""
	if len(parts) > 4 {
		action = parts[4]
	}

	games := s.db.GetGames()
	game, exists := games[gameID]
	if !exists {
		jsonError(w, 404, "Game not found")
		return
	}

	if r.Method == "DELETE" && action == "" {
		_ = s.watcher.Unwatch(game.SavePath)
		_ = s.db.RemoveGame(gameID)
		jsonResponse(w, 200, map[string]string{"success": "true"})
		return
	}

	if r.Method == "POST" {
		switch action {
		case "sync":
			// Trigger manual synchronization with peers
			go s.SyncGameWithPeers(gameID)
			jsonResponse(w, 200, map[string]string{"status": "sync_triggered"})
			return
		case "launch":
			// Mock game launch
			jsonResponse(w, 200, map[string]string{"status": "launched"})
			return
		case "snapshot":
			// Mock snapshot
			jsonResponse(w, 200, map[string]interface{}{"id": "snap_mock", "status": "created"})
			return
		case "rollback":
			jsonResponse(w, 200, map[string]string{"status": "rolled_back"})
			return
		}
	}

	jsonError(w, 404, "Action not found")
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, s.db.GetPeers())
}

func (s *Server) handlePeersPair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	// 1. Send pairing handshake HTTP call to other node
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://%s:%d/api/p2p/handshake", req.Address, req.Port)

	payload := map[string]interface{}{
		"peerId":     s.nodeID,
		"deviceName": s.db.GetSettings().DeviceName,
		"deviceType": s.db.GetSettings().DeviceType,
		"port":       s.port,
	}
	jsonBytes, _ := json.Marshal(payload)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		jsonError(w, 500, fmt.Sprintf("Failed to contact remote peer: %s", err.Error()))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		jsonError(w, 500, fmt.Sprintf("Peer rejected handshake: %s", string(body)))
		return
	}

	// Register in local pending pairing requests
	s.mu.Lock()
	s.sentPairingRequests[req.Address] = time.Now()
	s.mu.Unlock()

	jsonResponse(w, 200, map[string]string{"status": "pending", "message": "Handshake sent successfully."})
}

func (s *Server) handlePeersApprove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	s.mu.Lock()
	request, ok := s.pairingRequests[req.PeerID]
	s.mu.Unlock()

	if !ok {
		jsonError(w, 404, "Handshake request not found.")
		return
	}

	addr := request["address"].(string)
	port := int(request["port"].(float64))

	// Send approve-confirm back to peer
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://%s:%d/api/p2p/approve-confirm", addr, port)

	payload := map[string]interface{}{
		"peerId":     s.nodeID,
		"deviceName": s.db.GetSettings().DeviceName,
		"deviceType": s.db.GetSettings().DeviceType,
		"port":       s.port,
	}
	jsonBytes, _ := json.Marshal(payload)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		jsonError(w, 500, fmt.Sprintf("Failed to confirm approval to peer: %s", err.Error()))
		return
	}
	defer resp.Body.Close()

	// Add peer locally
	s.db.AddPeer(req.PeerID, request["deviceName"].(string), addr, port, request["deviceType"].(string))
	s.db.UpdatePeerStatus(req.PeerID, "online", addr)

	s.mu.Lock()
	delete(s.pairingRequests, req.PeerID)
	s.mu.Unlock()

	jsonResponse(w, 200, map[string]string{"success": "true"})
}

func (s *Server) handlePeersReject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	s.mu.Lock()
	delete(s.pairingRequests, req.PeerID)
	s.mu.Unlock()

	jsonResponse(w, 200, map[string]string{"success": "true"})
}

func (s *Server) handlePeersUnpair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	peer, exists := s.db.GetPeers()[req.PeerID]
	if exists {
		// Notify peer they are unpaired
		client := &http.Client{Timeout: 3 * time.Second}
		url := fmt.Sprintf("http://%s:%d/api/p2p/unpair", peer.Address, peer.Port)

		payload := map[string]interface{}{
			"peerId": s.nodeID,
		}
		jsonBytes, _ := json.Marshal(payload)
		_, _ = client.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
	}

	s.db.RemovePeer(req.PeerID)
	jsonResponse(w, 200, map[string]string{"success": "true"})
}

func (s *Server) handlePeersProbe(w http.ResponseWriter, r *http.Request) {
	// Simple LAN Ping probe
	var req struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	client := &http.Client{Timeout: 3 * time.Second}
	url := fmt.Sprintf("http://%s:%d/api/p2p/ping?from=%s", req.Address, req.Port, s.nodeID)

	resp, err := client.Get(url)
	if err != nil {
		jsonError(w, 500, "Device unreachable")
		return
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&data)
	jsonResponse(w, 200, data)
}

func (s *Server) handlePresetsScan(w http.ResponseWriter, r *http.Request) {
	// Preset empty scan response
	jsonResponse(w, 200, []interface{}{})
}

func (s *Server) handleBrowseDirectory(w http.ResponseWriter, r *http.Request) {
	// Call native dialog (mock or return path)
	jsonResponse(w, 200, map[string]string{"path": ""})
}

func (s *Server) handleBrowseFile(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, map[string]string{"path": ""})
}

func (s *Server) handleRelayIPs(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, []string{"127.0.0.1"})
}

func (s *Server) handleRelayHealth(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, map[string]string{"status": "healthy"})
}

func (s *Server) handleWanStatus(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, map[string]string{"connected": "false"})
}

// ── P2P Handlers (Daemon-to-Daemon REST calls) ─────────────────────────────

func (s *Server) handleP2PPing(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	paired := false
	if from != "" {
		_, paired = s.db.GetPeers()[from]
	}

	jsonResponse(w, 200, map[string]interface{}{
		"status":     "ok",
		"paired":     paired,
		"deviceName": s.db.GetSettings().DeviceName,
		"deviceType": s.db.GetSettings().DeviceType,
		"games":      s.db.GetGames(),
	})
}

func (s *Server) handleP2PHandshake(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID     string `json:"peerId"`
		DeviceName string `json:"deviceName"`
		DeviceType string `json:"deviceType"`
		Port       int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	clientIP, _, _ := net.SplitHostPort(r.RemoteAddr)
	if clientIP == "::1" || clientIP == "127.0.0.1" {
		clientIP = "127.0.0.1"
	}

	s.mu.Lock()
	s.pairingRequests[req.PeerID] = map[string]interface{}{
		"peerId":     req.PeerID,
		"deviceName": req.DeviceName,
		"deviceType": req.DeviceType,
		"address":    clientIP,
		"port":       float64(req.Port),
	}
	s.mu.Unlock()

	jsonResponse(w, 200, map[string]string{"status": "pending"})
}

func (s *Server) handleP2PApproveConfirm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID     string `json:"peerId"`
		DeviceName string `json:"deviceName"`
		DeviceType string `json:"deviceType"`
		Port       int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	clientIP, _, _ := net.SplitHostPort(r.RemoteAddr)
	if clientIP == "::1" || clientIP == "127.0.0.1" {
		clientIP = "127.0.0.1"
	}

	s.db.AddPeer(req.PeerID, req.DeviceName, clientIP, req.Port, req.DeviceType)
	s.db.UpdatePeerStatus(req.PeerID, "online", clientIP)

	jsonResponse(w, 200, map[string]string{"success": "true"})
}

func (s *Server) handleP2PUnpair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	s.db.RemovePeer(req.PeerID)
	jsonResponse(w, 200, map[string]string{"success": "true"})
}

func (s *Server) handleP2PManifest(w http.ResponseWriter, r *http.Request) {
	// Path layout: /api/p2p/manifest/:gameId
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		jsonError(w, 400, "Missing game ID")
		return
	}
	gameID := parts[4]

	localIndex := s.GetOrLoadIndex(gameID)
	jsonResponse(w, 200, map[string]interface{}{
		"gameId":   gameID,
		"manifest": localIndex.ToMap(),
	})
}

func (s *Server) handleP2PBlocks(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		jsonError(w, 400, "Missing game ID")
		return
	}
	gameID := parts[4]

	game, exists := s.db.GetGames()[gameID]
	if !exists {
		jsonError(w, 404, "Game not found")
		return
	}

	var req struct {
		RelPath      string `json:"relPath"`
		BlockIndices []int  `json:"blockIndices"`
		BlockSize    int64  `json:"blockSize"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	fullPath := filepath.Join(game.SavePath, filepath.FromSlash(req.RelPath))

	// Hashing safety checks (path traversal validation)
	if !IsSafePath(game.SavePath, fullPath) {
		jsonError(w, 403, "Access denied: path traversal attempt detected.")
		return
	}

	file, err := os.Open(fullPath)
	if err != nil {
		jsonError(w, 500, err.Error())
		return
	}
	defer file.Close()

	blocksPayload := []map[string]interface{}{}

	for _, idx := range req.BlockIndices {
		offset := int64(idx) * req.BlockSize
		data := make([]byte, req.BlockSize)
		bytesRead, err := file.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			jsonError(w, 500, err.Error())
			return
		}

		blocksPayload = append(blocksPayload, map[string]interface{}{
			"index": idx,
			"size":  bytesRead,
			"data":  data[:bytesRead],
		})
	}

	jsonResponse(w, 200, map[string]interface{}{
		"relPath": req.RelPath,
		"blocks":  blocksPayload,
	})
}

func (s *Server) handleP2PDeleteFile(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		jsonError(w, 400, "Missing game ID")
		return
	}
	gameID := parts[4]

	game, exists := s.db.GetGames()[gameID]
	if !exists {
		jsonError(w, 404, "Game not found")
		return
	}

	var req struct {
		RelPath string `json:"relPath"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}

	fullPath := filepath.Join(game.SavePath, filepath.FromSlash(req.RelPath))
	if !IsSafePath(game.SavePath, fullPath) {
		jsonError(w, 403, "Path traversal denied.")
		return
	}

	if _, err := os.Stat(fullPath); err == nil {
		_ = os.RemoveAll(fullPath)
	}

	jsonResponse(w, 200, map[string]string{"success": "true"})
}

// ── Helper Sync Logic ──────────────────────────────────────────────────────

func (s *Server) GetOrLoadIndex(gameID string) *syncengine.SafeFileInfo {
	s.indexesMu.Lock()
	defer s.indexesMu.Unlock()

	if idx, ok := s.indexes[gameID]; ok {
		return idx
	}

	idx := syncengine.NewSafeFileInfo()
	manifest, err := s.db.LoadGameManifest(gameID)
	if err == nil && len(manifest) > 0 {
		idx.LoadFromMap(manifest)
	}

	s.indexes[gameID] = idx
	return idx
}

func (s *Server) SaveIndexToDisk(gameID string) {
	s.indexesMu.RLock()
	idx, ok := s.indexes[gameID]
	s.indexesMu.RUnlock()

	if ok {
		_ = s.db.SaveGameManifest(gameID, idx.ToMap())
	}
}

func (s *Server) P2PHandshakeLocal(peerID string, deviceName string, deviceType string, port int, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairingRequests[peerID] = map[string]interface{}{
		"peerId":     peerID,
		"deviceName": deviceName,
		"deviceType": deviceType,
		"address":    address,
		"port":       float64(port),
	}
}

// SyncGameWithPeers runs the Syncthing BEP diffing and block-based exchange over network
func (s *Server) SyncGameWithPeers(gameID string) {
	games := s.db.GetGames()
	game, exists := games[gameID]
	if !exists {
		return
	}

	localIndex := s.GetOrLoadIndex(gameID)

	// Scan local directory first to get latest local hashes and increment local version vectors
	_ = syncengine.ScanDirectory(game.SavePath, localIndex, s.nodeID)
	s.SaveIndexToDisk(gameID)

	// Get paired peers
	peers := s.db.GetPeers()
	coordinator := syncengine.NewSyncCoordinator(game.SavePath, localIndex, s.nodeID)

	for _, peer := range peers {
		// Only sync with online peers
		if peer.Status != "online" {
			continue
		}

		if peer.Address == "relay" {
			if s.WANSyncFunc != nil {
				go func(p db.PeerSchema) {
					if err := s.WANSyncFunc(gameID, p); err != nil {
						fmt.Printf("[Sync] WAN sync error with peer %s: %v\n", p.Name, err)
					}
				}(peer)
			}
			continue
		}

		client := syncengine.NewLANExchangeClient(peer.Address, peer.Port)
		remoteManifest, err := client.GetFileIndex(gameID)
		if err != nil {
			fmt.Printf("[Sync] Failed to fetch index from peer %s: %v\n", peer.Name, err)
			continue
		}

		// Calculate synchronization diff
		diff := coordinator.Diff(remoteManifest)

		// 1. Apply local deletions
		for _, delFi := range diff.DeleteLocally {
			fullPath := filepath.Join(game.SavePath, filepath.FromSlash(delFi.Name))
			_ = os.RemoveAll(fullPath)
			localIndex.Store(delFi)
		}

		// 2. Resolve conflict files (rename local conflicting copy)
		for _, confFi := range diff.Conflicts {
			conflictName, err := coordinator.ResolveConflict(confFi, peer.ID)
			if err == nil && conflictName != "" {
				fmt.Printf("[Sync] Conflict detected! Saved local version as %s\n", conflictName)
			}
		}

		// 3. Replicate/Pull updated files (including concurrent ones which were renamed)
		pullList := append(diff.PullFiles, diff.Conflicts...)
		for _, pullFi := range pullList {
			err := coordinator.AssembleFile(pullFi, func(relPath string, block syncengine.BlockInfo) ([]byte, error) {
				return client.FetchBlock(gameID, relPath, block)
			})
			if err != nil {
				fmt.Printf("[Sync] Error assembling file %s: %v\n", pullFi.Name, err)
			}
		}

		// Save updated manifest
		s.SaveIndexToDisk(gameID)
		s.db.UpdatePeerLastSynced(peer.ID, time.Now())
	}
}

// ── Generic Helpers ────────────────────────────────────────────────────────

func IsSafePath(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func jsonResponse(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, code int, message string) {
	jsonResponse(w, code, map[string]string{"error": message})
}

func (s *Server) findFrontendDir() string {
	// Look for src/frontend relative to the working directory or parent directories
	paths := []string{
		"src/frontend",
		"../src/frontend",
		"../../src/frontend",
		"./frontend",
	}

	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err == nil {
			if stat, err := os.Stat(abs); err == nil && stat.IsDir() {
				return abs
			}
		}
	}

	// Try relative to executable
	exePath, err := os.Executable()
	if err == nil {
		p := filepath.Join(filepath.Dir(exePath), "src", "frontend")
		if stat, err := os.Stat(p); err == nil && stat.IsDir() {
			return p
		}
	}

	return ""
}

// TriggerLocalSyncAll triggers synchronization of all registered games
func (s *Server) TriggerLocalSyncAll() {
	for gameID := range s.db.GetGames() {
		go s.SyncGameWithPeers(gameID)
	}
}

func bytesValue(b []byte) []byte {
	return b
}
