package db

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sivadaboi/syncsave-go/syncengine"
)

type PathTranslation struct {
	RuleType string `json:"ruleType"` // "replace" or similar
	Match    string `json:"match"`
	Replace  string `json:"replace"`
}

type CloudSyncSettings struct {
	Enabled             bool              `json:"enabled"`
	Provider            string            `json:"provider"` // "local", "webdav", etc.
	URL                 string            `json:"url"`
	Username            string            `json:"username"`
	Password            string            `json:"password"`
	Headers             string            `json:"headers"`
	FolderID            string            `json:"folderId"`
	CustomClientIds     map[string]string `json:"customClientIds"`
	CustomClientSecrets map[string]string `json:"customClientSecrets"`
	Tokens              struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiryTime   int64  `json:"expiryTime"`
		UserEmail    string `json:"userEmail"`
	} `json:"tokens"`
}

type SettingsSchema struct {
	DeviceName        string            `json:"deviceName"`
	NodeID            string            `json:"nodeId"`
	DeviceType        string            `json:"deviceType"`
	Port              int               `json:"port"`
	SyncInterval      int               `json:"syncInterval"`
	SyncOnWatch       bool              `json:"syncOnWatch"`
	DataDir           string            `json:"dataDir"`
	BackupsDir        string            `json:"backupsDir"`
	SyncBackupsDir    string            `json:"syncBackupsDir"`
	AutoDeleteBackups bool              `json:"autoDeleteBackups"`
	AutoDeleteDays    int               `json:"autoDeleteDays"`
	AutoSyncOnTrack   bool              `json:"autoSyncOnTrack"`
	CustomScanPaths   []string          `json:"customScanPaths"`
	PathTranslations  []PathTranslation `json:"pathTranslations"`
	RelayURL          string            `json:"relayUrl"`
	SyncCode          string            `json:"syncCode"`
	HostRelay         bool              `json:"hostRelay"`
	RelayPort         int               `json:"relayPort"`
	StartOnBoot       bool              `json:"startOnBoot"`
	SpeedLimit        int               `json:"speedLimit"`
	URIMode           string            `json:"uiMode"`
	CloudSync         CloudSyncSettings `json:"cloudSync"`
}

type SnapshotSchema struct {
	ID        string   `json:"id"`
	Timestamp string   `json:"timestamp"`
	Files     []string `json:"files"`
	ZipPath   string   `json:"zipPath"`
	Size      int64    `json:"size"`
	Branch    string   `json:"branch"`
}

type BranchSchema struct {
	Name      string           `json:"name"`
	Snapshots []SnapshotSchema `json:"snapshots"`
}

type GameSchema struct {
	ID           string                  `json:"id"`
	Name         string                  `json:"name"`
	SavePath     string                  `json:"savePath"`
	ActiveBranch string                  `json:"activeBranch"`
	AutoSync     bool                    `json:"autoSync"`
	MaxSnapshots int                     `json:"maxSnapshots"`
	Branches     map[string]BranchSchema `json:"branches"`
	CreatedAt    string                  `json:"createdAt"`
}

type PeerSchema struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	DeviceType string  `json:"deviceType"`
	Address    string  `json:"address"`
	Port       int     `json:"port"`
	PairedAt   string  `json:"pairedAt"`
	LastSynced *string `json:"lastSynced"`
	Status     string  `json:"status"` // "online" or "offline"
}

type DatabaseSchema struct {
	Settings SettingsSchema        `json:"settings"`
	Games    map[string]GameSchema `json:"games"`
	Peers    map[string]PeerSchema `json:"peers"`
}

type Database struct {
	mu       sync.RWMutex
	filePath string
	Data     DatabaseSchema
}

// NewDatabase initializes a database helper targeting the user's home folder path
func NewDatabase(customFilePath ...string) *Database {
	var filePath string
	if len(customFilePath) > 0 && customFilePath[0] != "" {
		filePath = customFilePath[0]
	} else {
		homeDir, _ := os.UserHomeDir()
		filePath = filepath.Join(homeDir, ".syncsave", "syncsave-db.json")
	}

	db := &Database{
		filePath: filePath,
	}
	db.Load()
	return db
}

// Load reads the JSON configuration from disk
func (db *Database) Load() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ensure parent dir exists
	_ = os.MkdirAll(filepath.Dir(db.filePath), 0755)

	if _, err := os.Stat(db.filePath); os.IsNotExist(err) {
		db.Data = db.defaultState()
		db.saveLocked()
		return
	}

	dataBytes, err := os.ReadFile(db.filePath)
	if err != nil {
		db.Data = db.defaultState()
		return
	}

	if err := json.Unmarshal(dataBytes, &db.Data); err != nil {
		// Reset on corrupt json
		db.Data = db.defaultState()
		db.saveLocked()
		return
	}

	// Ensure sub-structures are initialized
	if db.Data.Games == nil {
		db.Data.Games = make(map[string]GameSchema)
	}
	if db.Data.Peers == nil {
		db.Data.Peers = make(map[string]PeerSchema)
	}
	if db.Data.Settings.NodeID == "" {
		db.Data.Settings.NodeID = generateNodeID()
	}
}

// Save writes database state back to the disk
func (db *Database) Save() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.saveLocked()
}

func (db *Database) saveLocked() {
	dataBytes, err := json.MarshalIndent(db.Data, "", "  ")
	if err == nil {
		_ = os.WriteFile(db.filePath, dataBytes, 0644)
	}
}

func (db *Database) GetSettings() SettingsSchema {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.Data.Settings
}

func (db *Database) UpdateSettings(s SettingsSchema) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Data.Settings = s
	db.saveLocked()
}

func (db *Database) GetGames() map[string]GameSchema {
	db.mu.RLock()
	defer db.mu.RUnlock()
	cp := make(map[string]GameSchema, len(db.Data.Games))
	for k, v := range db.Data.Games {
		cp[k] = v
	}
	return cp
}

func (db *Database) AddGame(name string, savePath string) (GameSchema, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Generate ID
	reg := regexp.MustCompile("[^a-z0-9]")
	id := strings.ToLower(name)
	id = reg.ReplaceAllString(id, "-")
	id = regexp.MustCompile("-+").ReplaceAllString(id, "-")
	id = strings.Trim(id, "-")

	if _, exists := db.Data.Games[id]; exists {
		return GameSchema{}, fmt.Errorf("game with name/id \"%s\" already exists", name)
	}

	absPath, _ := filepath.Abs(savePath)

	game := GameSchema{
		ID:           id,
		Name:         name,
		SavePath:     absPath,
		ActiveBranch: "main",
		AutoSync:     true,
		MaxSnapshots: 5,
		Branches: map[string]BranchSchema{
			"main": {
				Name:      "main",
				Snapshots: []SnapshotSchema{},
			},
		},
		CreatedAt: time.Now().Format(time.RFC3339),
	}

	db.Data.Games[id] = game
	db.saveLocked()

	return game, nil
}

func (db *Database) RemoveGame(id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Data.Games[id]; !exists {
		return errors.New("game not found")
	}

	delete(db.Data.Games, id)
	db.saveLocked()
	return nil
}

func (db *Database) GetPeers() map[string]PeerSchema {
	db.mu.RLock()
	defer db.mu.RUnlock()
	cp := make(map[string]PeerSchema, len(db.Data.Peers))
	for k, v := range db.Data.Peers {
		cp[k] = v
	}
	return cp
}

func (db *Database) AddPeer(peerID string, name string, address string, port int, deviceType string) PeerSchema {
	db.mu.Lock()
	defer db.mu.Unlock()

	peer := PeerSchema{
		ID:         peerID,
		Name:       name,
		DeviceType: deviceType,
		Address:    address,
		Port:       port,
		PairedAt:   time.Now().Format(time.RFC3339),
		Status:     "offline",
	}

	db.Data.Peers[peerID] = peer
	db.saveLocked()
	return peer
}

func (db *Database) RemovePeer(peerID string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Data.Peers, peerID)
	db.saveLocked()
}

func (db *Database) UpdatePeerStatus(peerID string, status string, address string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if p, exists := db.Data.Peers[peerID]; exists {
		p.Status = status
		if address != "" {
			p.Address = address
		}
		db.Data.Peers[peerID] = p
		db.saveLocked()
	}
}

func (db *Database) UpdatePeerLastSynced(peerID string, lastSynced time.Time) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if p, exists := db.Data.Peers[peerID]; exists {
		tStr := lastSynced.Format(time.RFC3339)
		p.LastSynced = &tStr
		db.Data.Peers[peerID] = p
		db.saveLocked()
	}
}


func (db *Database) defaultState() DatabaseSchema {
	homeDir, _ := os.UserHomeDir()
	syncsaveHome := filepath.Join(homeDir, ".syncsave")
	backupsDir := filepath.Join(syncsaveHome, "backups")

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "Desktop Device"
	}

	return DatabaseSchema{
		Settings: SettingsSchema{
			DeviceName:        hostname,
			NodeID:            generateNodeID(),
			DeviceType:        "desktop",
			Port:              8383,
			SyncInterval:      5000,
			SyncOnWatch:       true,
			DataDir:           syncsaveHome,
			BackupsDir:        backupsDir,
			SyncBackupsDir:    backupsDir,
			AutoDeleteBackups: false,
			AutoDeleteDays:    30,
			AutoSyncOnTrack:   true,
			CustomScanPaths:   []string{},
			PathTranslations:  []PathTranslation{},
			RelayURL:          "wss://syncsave-relay.onrender.com",
			SyncCode:          "",
			HostRelay:         false,
			RelayPort:         8386,
			StartOnBoot:       false,
			SpeedLimit:        0,
			URIMode:           "modern",
			CloudSync: CloudSyncSettings{
				Enabled:  false,
				Provider: "local",
				URL:      "",
				CustomClientIds: map[string]string{
					"google_drive": "",
					"onedrive":     "",
					"dropbox":      "",
				},
				CustomClientSecrets: map[string]string{
					"google_drive": "",
					"onedrive":     "",
					"dropbox":      "",
				},
			},
		},
		Games: make(map[string]GameSchema),
		Peers: make(map[string]PeerSchema),
	}
}

func generateNodeID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "node_" + hex.EncodeToString(b)
}

// GetManifestFilePath returns the path to the version vector manifest file for a game
func (db *Database) GetManifestFilePath(gameID string) string {
	return filepath.Join(db.Data.Settings.DataDir, "manifests", gameID+".json")
}

// LoadGameManifest reads a game's Syncthing-style version vector manifest index
func (db *Database) LoadGameManifest(gameID string) (map[string]syncengine.FileInfo, error) {
	manifestPath := db.GetManifestFilePath(gameID)
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return make(map[string]syncengine.FileInfo), nil
	}

	dataBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}

	var manifest map[string]syncengine.FileInfo
	if err := json.Unmarshal(dataBytes, &manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

// SaveGameManifest writes a game's Syncthing-style version vector manifest index
func (db *Database) SaveGameManifest(gameID string, manifest map[string]syncengine.FileInfo) error {
	manifestPath := db.GetManifestFilePath(gameID)
	_ = os.MkdirAll(filepath.Dir(manifestPath), 0755)

	dataBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(manifestPath, dataBytes, 0644)
}
