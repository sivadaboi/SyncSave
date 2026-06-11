package syncengine

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// BlockExchangeClient defines the transport layer for index and block transfers
type BlockExchangeClient interface {
	GetFileIndex(gameID string) (map[string]FileInfo, error)
	FetchBlock(gameID string, relPath string, block BlockInfo) ([]byte, error)
}

// LANExchangeClient communicates directly with the peer over HTTP
type LANExchangeClient struct {
	BaseURL    string // e.g. "http://192.168.1.100:8383"
	HTTPClient *http.Client
}

func NewLANExchangeClient(address string, port int) *LANExchangeClient {
	return &LANExchangeClient{
		BaseURL: fmt.Sprintf("http://%s:%d", address, port),
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *LANExchangeClient) GetFileIndex(gameID string) (map[string]FileInfo, error) {
	url := fmt.Sprintf("%s/api/p2p/manifest/%s", c.BaseURL, gameID)
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("peer returned status %d: %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Manifest map[string]FileInfo `json:"manifest"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return payload.Manifest, nil
}

func (c *LANExchangeClient) FetchBlock(gameID string, relPath string, block BlockInfo) ([]byte, error) {
	url := fmt.Sprintf("%s/api/p2p/blocks/%s", c.BaseURL, gameID)

	requestPayload := map[string]interface{}{
		"relPath":      relPath,
		"blockIndices": []int{int(block.Offset / block.Size)}, // index = offset / size
		"blockSize":    block.Size,
	}

	jsonBytes, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("peer returned status %d: %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Blocks []struct {
			Data []byte `json:"data"`
		} `json:"blocks"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	if len(payload.Blocks) == 0 {
		return nil, fmt.Errorf("no blocks returned by peer")
	}

	return payload.Blocks[0].Data, nil
}

// WANExchangeClient tunnels P2P requests through the WebSocket relay server
type WANExchangeClient struct {
	Conn           *websocket.Conn
	PeerID         string
	LocalNodeID    string
	RegisterChan   func(msgID string, ch chan []byte)
	DeregisterChan func(msgID string)
}

func NewWANExchangeClient(conn *websocket.Conn, peerID string, localNodeID string, reg func(msgID string, ch chan []byte), dereg func(msgID string)) *WANExchangeClient {
	return &WANExchangeClient{
		Conn:           conn,
		PeerID:         peerID,
		LocalNodeID:    localNodeID,
		RegisterChan:   reg,
		DeregisterChan: dereg,
	}
}

func (w *WANExchangeClient) GetFileIndex(gameID string) (map[string]FileInfo, error) {
	msgID := "req_" + generateRandomID()
	ch := make(chan []byte, 1)
	w.RegisterChan(msgID, ch)
	defer w.DeregisterChan(msgID)

	request := map[string]interface{}{
		"type":   "request",
		"msgId":  msgID,
		"to":     w.PeerID,
		"from":   w.LocalNodeID,
		"route":  "/manifest/" + gameID,
		"body":   map[string]interface{}{},
	}

	reqBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	if err := w.Conn.WriteMessage(websocket.TextMessage, reqBytes); err != nil {
		return nil, err
	}

	select {
	case respBytes := <-ch:
		var response struct {
			Status int `json:"status"`
			Data   struct {
				GameID   string              `json:"gameId"`
				Manifest map[string]FileInfo `json:"manifest"`
				Error    string              `json:"error"`
			} `json:"data"`
		}
		if err := json.Unmarshal(respBytes, &response); err != nil {
			return nil, err
		}
		if response.Status != 200 {
			return nil, fmt.Errorf("remote peer returned status %d", response.Status)
		}
		if response.Data.Error != "" {
			return nil, fmt.Errorf("remote peer error: %s", response.Data.Error)
		}
		return response.Data.Manifest, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("index transfer timeout")
	}
}

func (w *WANExchangeClient) FetchBlock(gameID string, relPath string, block BlockInfo) ([]byte, error) {
	msgID := "req_" + generateRandomID()
	ch := make(chan []byte, 1)
	w.RegisterChan(msgID, ch)
	defer w.DeregisterChan(msgID)

	request := map[string]interface{}{
		"type":   "request",
		"msgId":  msgID,
		"to":     w.PeerID,
		"from":   w.LocalNodeID,
		"route":  "/blocks/" + gameID,
		"body": map[string]interface{}{
			"relPath":      relPath,
			"blockIndices": []int{int(block.Offset / block.Size)},
			"blockSize":    block.Size,
		},
	}

	reqBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	if err := w.Conn.WriteMessage(websocket.TextMessage, reqBytes); err != nil {
		return nil, err
	}

	select {
	case respBytes := <-ch:
		var response struct {
			Status int `json:"status"`
			Data   struct {
				RelPath string `json:"relPath"`
				Blocks  []struct {
					Index int    `json:"index"`
					Size  int    `json:"size"`
					Data  []byte `json:"data"`
				} `json:"blocks"`
				Error string `json:"error"`
			} `json:"data"`
		}
		if err := json.Unmarshal(respBytes, &response); err != nil {
			return nil, err
		}
		if response.Status != 200 {
			return nil, fmt.Errorf("remote peer returned status %d", response.Status)
		}
		if response.Data.Error != "" {
			return nil, fmt.Errorf("remote peer error: %s", response.Data.Error)
		}
		if len(response.Data.Blocks) == 0 {
			return nil, fmt.Errorf("no blocks returned")
		}
		return response.Data.Blocks[0].Data, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("block transfer timeout")
	}
}

func generateRandomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
