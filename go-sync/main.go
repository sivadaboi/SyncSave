package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sivadaboi/syncsave-go/api"
	"github.com/sivadaboi/syncsave-go/db"
	"github.com/sivadaboi/syncsave-go/syncengine"
	"github.com/sivadaboi/syncsave-go/watcher"
)

type Daemon struct {
	db              *db.Database
	watcher         *watcher.Watcher
	server          *api.Server
	scanners        map[string]*syncengine.PeriodicScanner
	wanConn         *websocket.Conn
	wanConnMu       sync.Mutex
	wanPendingReqs  map[string]chan []byte
	wanReqsMu       sync.Mutex
	discoveredPeers map[string]db.PeerSchema
	peersMu         sync.Mutex
	stop            chan struct{}
	wg              sync.WaitGroup
}

func main() {
	fmt.Println("====================================================")
	fmt.Println("  SyncSave Go-Syncthing Daemon v1.1.4")
	fmt.Println("====================================================")

	d := &Daemon{
		db:              db.NewDatabase(),
		scanners:        make(map[string]*syncengine.PeriodicScanner),
		wanPendingReqs:  make(map[string]chan []byte),
		discoveredPeers: make(map[string]db.PeerSchema),
		stop:            make(chan struct{}),
	}

	w, err := watcher.NewWatcher()
	if err != nil {
		log.Fatalf("[Watcher] Failed to start: %v", err)
	}
	d.watcher = w

	s := api.NewServer(d.db, d.watcher)
	d.server = s
	s.WANSyncFunc = func(gameID string, peer db.PeerSchema) error {
		return d.syncGameOverWAN(gameID, peer)
	}

	// 1. Start Watcher
	d.watcher.Start(2 * time.Second) // 2-second debounce quiet period

	// 2. Watch existing tracked games
	for gameID, game := range d.db.GetGames() {
		fmt.Printf("[Watcher] Watching game save path: %s -> %s\n", game.Name, game.SavePath)
		_ = d.watcher.Watch(game.SavePath)

		// Start 30-minute fallback periodic scanner
		localIndex := s.GetOrLoadIndex(gameID)
		scanner := syncengine.NewPeriodicScanner(game.SavePath, localIndex, d.db.GetSettings().NodeID)
		scanner.Start(30 * time.Minute)
		d.scanners[gameID] = scanner
	}

	// 3. Start watching changes watcher channel
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case rootPath := <-d.watcher.Events:
				// Map root path back to game ID
				for gameID, game := range d.db.GetGames() {
					if filepath.Clean(game.SavePath) == filepath.Clean(rootPath) {
						fmt.Printf("[Watcher] Changes settled for \"%s\". Triggering scan & P2P sync.\n", game.Name)
						// Scan directory
						localIndex := s.GetOrLoadIndex(gameID)
						_ = syncengine.ScanDirectory(game.SavePath, localIndex, d.db.GetSettings().NodeID)
						s.SaveIndexToDisk(gameID)

						// Trigger P2P sync with online peers
						s.SyncGameWithPeers(gameID)
					}
				}
			case <-d.stop:
				return
			}
		}
	}()

	// 4. Start LAN UDP discovery
	d.startLANDiscovery()

	// 5. Start WAN Relay connection loop
	d.startWANConnectionLoop()

	// 6. Start REST HTTP server
	go func() {
		if err := d.server.Start(); err != nil {
			log.Printf("[API] Server exit: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n[Daemon] Shutting down gracefully...")
	close(d.stop)
	d.server.Stop()
	d.watcher.Close()
	for _, scanner := range d.scanners {
		scanner.Stop()
	}
	d.closeWANConnection()
	d.wg.Wait()
	fmt.Println("[Daemon] Shutdown complete.")
}

func (d *Daemon) syncGameOverWAN(gameID string, peer db.PeerSchema) error {
	// 1. Get WebSocket connection
	d.wanConnMu.Lock()
	conn := d.wanConn
	d.wanConnMu.Unlock()
	if conn == nil {
		return fmt.Errorf("WAN connection is offline")
	}

	// 2. Setup response channel mapping callbacks
	reg := func(msgID string, ch chan []byte) {
		d.wanReqsMu.Lock()
		d.wanPendingReqs[msgID] = ch
		d.wanReqsMu.Unlock()
	}
	dereg := func(msgID string) {
		d.wanReqsMu.Lock()
		delete(d.wanPendingReqs, msgID)
		d.wanReqsMu.Unlock()
	}

	// 3. Create WANExchangeClient
	client := syncengine.NewWANExchangeClient(conn, peer.ID, d.db.GetSettings().NodeID, reg, dereg)

	// 4. Fetch remote manifest
	remoteManifest, err := client.GetFileIndex(gameID)
	if err != nil {
		return fmt.Errorf("failed to fetch WAN index: %w", err)
	}

	// 5. Run diffing coordinator
	games := d.db.GetGames()
	game, exists := games[gameID]
	if !exists {
		return fmt.Errorf("game not found")
	}

	localIndex := d.server.GetOrLoadIndex(gameID)
	coordinator := syncengine.NewSyncCoordinator(game.SavePath, localIndex, d.db.GetSettings().NodeID)
	diff := coordinator.Diff(remoteManifest)

	// 6. Apply local deletions
	for _, delFi := range diff.DeleteLocally {
		fullPath := filepath.Join(game.SavePath, filepath.FromSlash(delFi.Name))
		_ = os.RemoveAll(fullPath)
		localIndex.Store(delFi)
	}

	// 7. Resolve conflict files
	for _, confFi := range diff.Conflicts {
		conflictName, err := coordinator.ResolveConflict(confFi, peer.ID)
		if err == nil && conflictName != "" {
			fmt.Printf("[Sync-WAN] Conflict detected! Saved local version as %s\n", conflictName)
		}
	}

	// 8. Replicate/Pull updated files
	pullList := append(diff.PullFiles, diff.Conflicts...)
	for _, pullFi := range pullList {
		err := coordinator.AssembleFile(pullFi, func(relPath string, block syncengine.BlockInfo) ([]byte, error) {
			return client.FetchBlock(gameID, relPath, block)
		})
		if err != nil {
			fmt.Printf("[Sync-WAN] Error assembling file %s: %v\n", pullFi.Name, err)
		}
	}

	// 9. Save updated manifest
	d.server.SaveIndexToDisk(gameID)
	d.db.UpdatePeerLastSynced(peer.ID, time.Now())

	return nil
}

// ── LAN UDP Discovery ──────────────────────────────────────────────────────

func (d *Daemon) startLANDiscovery() {
	settings := d.db.GetSettings()
	localPeerID := settings.NodeID

	// Start UDP Listener on port 8385
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 8385,
	})
	if err != nil {
		log.Printf("[LAN Discovery] Failed to bind to UDP port 8385: %v", err)
		return
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer conn.Close()
		buf := make([]byte, 1024)
		for {
			select {
			case <-d.stop:
				return
			default:
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				n, remoteAddr, err := conn.ReadFrom(buf)
				if err != nil {
					continue
				}

				msg := string(buf[:n])
				parts := strings.Split(msg, "|")
				// Format: syncsave-ping|deviceName|nodeId|port
				if len(parts) >= 4 && parts[0] == "syncsave-ping" {
					deviceName := parts[1]
					nodeID := parts[2]
					portVal, _ := strconv.Atoi(parts[3])

					if nodeID == localPeerID {
						continue // Ignore our own pings
					}

					ip := remoteAddr.(*net.UDPAddr).IP.String()

					d.peersMu.Lock()
					d.discoveredPeers[nodeID] = db.PeerSchema{
						ID:         nodeID,
						Name:       deviceName,
						Address:    ip,
						Port:       portVal,
						DeviceType: "desktop",
						Status:     "online",
					}
					d.peersMu.Unlock()

					// If they are in our paired peers database, mark them as online
					pairedPeers := d.db.GetPeers()
					if peer, exists := pairedPeers[nodeID]; exists {
						if peer.Status != "online" || peer.Address != ip {
							d.db.UpdatePeerStatus(nodeID, "online", ip)
							fmt.Printf("[LAN Discovery] Paired peer %s came online at %s:%d\n", deviceName, ip, portVal)
							d.server.TriggerLocalSyncAll()
						}
					}
				}
			}
		}
	}()

	// Start UDP Broadcast Sender Loop
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		raddr, err := net.ResolveUDPAddr("udp", "255.255.255.255:8385")
		if err != nil {
			return
		}

		// Dial socket for broadcast
		sconn, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			return
		}
		defer sconn.Close()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				msg := fmt.Sprintf("syncsave-ping|%s|%s|%d", settings.DeviceName, localPeerID, settings.Port)
				_, _ = sconn.Write([]byte(msg))
			case <-d.stop:
				return
			}
		}
	}()
}

// ── WAN WebSocket Relay Client ─────────────────────────────────────────────

func (d *Daemon) startWANConnectionLoop() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-d.stop:
				return
			default:
				settings := d.db.GetSettings()
				if settings.SyncCode == "" {
					time.Sleep(5 * time.Second)
					continue
				}

				d.connectToWANRelay(settings)
				time.Sleep(5 * time.Second) // Wait 5 seconds before trying to reconnect on drop
			}
		}
	}()
}

func (d *Daemon) connectToWANRelay(settings db.SettingsSchema) {
	u, err := url.Parse(settings.RelayURL)
	if err != nil {
		log.Printf("[WAN Client] Invalid relay URL: %v", err)
		return
	}

	wsURL := fmt.Sprintf("%s/?room=%s&device=%s", u.String(), settings.SyncCode, url.QueryEscape(settings.DeviceName))
	log.Printf("[WAN Client] Connecting to WAN Relay room: %s", settings.SyncCode)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("[WAN Client] Connection failed: %v", err)
		return
	}

	d.wanConnMu.Lock()
	d.wanConn = conn
	d.wanConnMu.Unlock()

	log.Printf("[WAN Client] Connected to WAN Relay successfully.")

	// 1. Broadcast hello presence
	d.sendWANRelayMessage(map[string]interface{}{
		"type":        "hello",
		"from":        settings.NodeID,
		"deviceName":  settings.DeviceName,
		"deviceType":  settings.DeviceType,
		"port":        settings.Port,
		"pairedPeers": getKeys(d.db.GetPeers()),
	})

	// 2. Start heartbeat ping loop
	pingStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.sendWANRelayMessage(map[string]interface{}{
					"type":       "ping",
					"from":       settings.NodeID,
					"deviceName": settings.DeviceName,
					"deviceType": settings.DeviceType,
					"port":       settings.Port,
				})
			case <-pingStop:
				return
			}
		}
	}()

	// 3. Receive messages loop
	defer close(pingStop)
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WAN Client] Connection closed: %v", err)
			break
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(message, &payload); err != nil {
			continue
		}

		// Route message
		msgType, _ := payload["type"].(string)
		to, _ := payload["to"].(string)

		if to != "" && to != settings.NodeID {
			continue // Message is not for us
		}

		from, _ := payload["from"].(string)
		if from == settings.NodeID {
			continue // Ignore our own broadcasts
		}

		// Mark peer online on WAN heartbeat/messages
		if from != "" {
			paired := d.db.GetPeers()
			if peer, exists := paired[from]; exists {
				if peer.Status != "online" || peer.Address != "relay" {
					d.db.UpdatePeerStatus(from, "online", "relay")
					fmt.Printf("[WAN Client] Paired peer %s came online over WAN\n", peer.Name)
					d.server.TriggerLocalSyncAll()
				}
			}
		}

		switch msgType {
		case "hello":
			// Reply hello-reply to announce presence
			d.sendWANRelayMessage(map[string]interface{}{
				"type":       "hello-reply",
				"to":         from,
				"from":       settings.NodeID,
				"deviceName": settings.DeviceName,
				"deviceType": settings.DeviceType,
				"port":       settings.Port,
			})
		case "request":
			// Process API requests from peer
			go d.handleWANRelayRequest(payload)
		case "response":
			// Route responses back to pending request channels
			msgID, _ := payload["msgId"].(string)
			d.wanReqsMu.Lock()
			ch, ok := d.wanPendingReqs[msgID]
			d.wanReqsMu.Unlock()
			if ok {
				ch <- message
			}
		}
	}

	d.wanConnMu.Lock()
	d.wanConn = nil
	d.wanConnMu.Unlock()
}

func (d *Daemon) closeWANConnection() {
	d.wanConnMu.Lock()
	if d.wanConn != nil {
		_ = d.wanConn.Close()
		d.wanConn = nil
	}
	d.wanConnMu.Unlock()
}

func (d *Daemon) sendWANRelayMessage(msg interface{}) {
	d.wanConnMu.Lock()
	defer d.wanConnMu.Unlock()
	if d.wanConn != nil {
		jsonBytes, err := json.Marshal(msg)
		if err == nil {
			_ = d.wanConn.WriteMessage(websocket.TextMessage, jsonBytes)
		}
	}
}

// ── WAN Client Tunnel Routing ──────────────────────────────────────────────

func (d *Daemon) handleWANRelayRequest(payload map[string]interface{}) {
	msgID, _ := payload["msgId"].(string)
	from, _ := payload["from"].(string)
	route, _ := payload["route"].(string)
	bodyMap, _ := payload["body"].(map[string]interface{})

	// Check pairing authorization
	pairedPeers := d.db.GetPeers()
	_, isPaired := pairedPeers[from]

	status := 200
	var resData interface{}

	requiresPairing := route != "/ping" && route != "/handshake"

	if requiresPairing && !isPaired {
		status = 401
		resData = map[string]string{"error": "Unauthorized: Requesting peer is not paired."}
	} else {
		// Route requests similarly to API REST handlers
		if route == "/ping" {
			resData = map[string]string{"status": "ok", "deviceName": d.db.GetSettings().DeviceName}
		} else if route == "/handshake" {
			// Save pending pairing requests
			d.server.GetOrLoadIndex("mock") // Ensure indexes initialized
			jsonBytes, _ := json.Marshal(bodyMap)
			var body struct {
				PeerID     string `json:"peerId"`
				DeviceName string `json:"deviceName"`
				DeviceType string `json:"deviceType"`
				Port       int    `json:"port"`
			}
			if err := json.Unmarshal(jsonBytes, &body); err == nil {
				// Handled in REST Server wrapper
				d.server.P2PHandshakeLocal(body.PeerID, body.DeviceName, body.DeviceType, body.Port, "relay")
			}
			resData = map[string]string{"status": "pending"}
		} else if strings.HasPrefix(route, "/manifest/") {
			gameID := strings.TrimPrefix(route, "/manifest/")
			localIndex := d.server.GetOrLoadIndex(gameID)
			resData = map[string]interface{}{
				"gameId":   gameID,
				"manifest": localIndex.ToMap(),
			}
		} else if strings.HasPrefix(route, "/blocks/") {
			gameID := strings.TrimPrefix(route, "/blocks/")
			jsonBytes, _ := json.Marshal(bodyMap)
			var body struct {
				RelPath      string `json:"relPath"`
				BlockIndices []int  `json:"blockIndices"`
				BlockSize    int64  `json:"blockSize"`
			}
			if err := json.Unmarshal(jsonBytes, &body); err == nil {
				game, exists := d.db.GetGames()[gameID]
				if !exists {
					status = 404
					resData = map[string]string{"error": "Game not found"}
				} else {
					fullPath := filepath.Join(game.SavePath, filepath.FromSlash(body.RelPath))
					if !api.IsSafePath(game.SavePath, fullPath) {
						status = 403
						resData = map[string]string{"error": "Path traversal denied."}
					} else {
						// Read file blocks
						file, err := os.Open(fullPath)
						if err != nil {
							status = 500
							resData = map[string]string{"error": err.Error()}
						} else {
							defer file.Close()
							blocksPayload := []map[string]interface{}{}
							for _, idx := range body.BlockIndices {
								offset := int64(idx) * body.BlockSize
								data := make([]byte, body.BlockSize)
								bytesRead, _ := file.ReadAt(data, offset)
								blocksPayload = append(blocksPayload, map[string]interface{}{
									"index": idx,
									"size":  bytesRead,
									"data":  data[:bytesRead],
								})
							}
							resData = map[string]interface{}{
								"relPath": body.RelPath,
								"blocks":  blocksPayload,
							}
						}
					}
				}
			}
		}
	}

	// Send response message back through WS relay
	d.sendWANRelayMessage(map[string]interface{}{
		"type":   "response",
		"to":     from,
		"from":   d.db.GetSettings().NodeID,
		"msgId":  msgID,
		"status": status,
		"data":   resData,
	})
}

// ── Server helper extensions (pairing handshakes bridging) ──────────────────

func getKeys(m map[string]db.PeerSchema) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

