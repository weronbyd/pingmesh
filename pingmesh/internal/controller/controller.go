package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html" // For escaping HTML content
	"log"
	"net/http"
	"sort" // For sorting data by timestamp
	"strconv" // For float to string conversion
	"sync"
	"time"

	"pingmesh/internal/api"
)

// ControllerConfig holds configuration for the RegionalController.
type ControllerConfig struct {
	ListenAddress             string
	MaxStoredResults          int
	ShutdownTimeout           time.Duration // Graceful shutdown timeout for the HTTP server
	GlobalAggregatorEndpoint  string        // Endpoint of the Global Aggregator
	DC_ID                     string        // Identifier for this controller's DC/region
}

// DefaultControllerConfig returns a default configuration for the controller.
func DefaultControllerConfig() ControllerConfig {
	return ControllerConfig{
		ListenAddress:             ":8080",
		MaxStoredResults:          1000,
		ShutdownTimeout:           5 * time.Second,
		GlobalAggregatorEndpoint:  "", // Default to empty, forwarding disabled
		DC_ID:                     "default-dc",
	}
}

// RegionalController manages the collection of probe data and forwards it.
type RegionalController struct {
	config       ControllerConfig
	server       *http.Server
	httpClient   *http.Client // For sending data to Global Aggregator
	dataStore    []api.ProbeDataPayload
	mu           sync.Mutex // Protects dataStore
	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

// NewRegionalController creates a new RegionalController.
func NewRegionalController(config ControllerConfig) *RegionalController {
	return &RegionalController{
		config:    config,
		dataStore: make([]api.ProbeDataPayload, 0, config.MaxStoredResults),
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Default timeout for HTTP requests
		},
		shutdownChan: make(chan struct{}),
	}
}

// Start initializes and starts the HTTP server for the controller.
func (rc *RegionalController) Start() error {
	log.Printf("RegionalController: Starting on %s...\n", rc.config.ListenAddress)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/submit-probe-data", rc.handleProbeDataSubmit)
	mux.HandleFunc("/status", rc.handleStatusPage) // New status page endpoint

	rc.server = &http.Server{
		Addr:    rc.config.ListenAddress,
		Handler: mux,
	}

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		log.Printf("RegionalController: HTTP server listening on %s\n", rc.config.ListenAddress)
		if err := rc.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("RegionalController: HTTP server ListenAndServe error: %v\n", err)
			// Normally, we might want to signal an error back to the main application
			// For now, we log it. Consider how to propagate this if main needs to know.
		}
		log.Println("RegionalController: HTTP server goroutine finished.")
	}()

	log.Println("RegionalController: Started successfully.")
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (rc *RegionalController) Stop() error {
	log.Println("RegionalController: Stopping...")
	close(rc.shutdownChan) // Signal any other internal loops if they existed

	if rc.server == nil {
		log.Println("RegionalController: Server not started or already stopped.")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), rc.config.ShutdownTimeout)
	defer cancel()

	if err := rc.server.Shutdown(ctx); err != nil {
		log.Printf("RegionalController: HTTP server shutdown error: %v\n", err)
		return err
	}

	rc.wg.Wait() // Wait for the ListenAndServe goroutine to exit
	log.Println("RegionalController: Stopped successfully.")
	return nil
}

// handleProbeDataSubmit is the HTTP handler for POST /api/v1/submit-probe-data.
func (rc *RegionalController) handleProbeDataSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload api.ProbeDataPayload
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&payload); err != nil {
		log.Printf("RegionalController: Error decoding JSON payload: %v\n", err)
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Log the received data
	// For high-volume scenarios, consider more structured logging or sampling.
	log.Printf("RegionalController: Received probe data: %+v\n", payload)

	// Store the data
	rc.mu.Lock()
	if len(rc.dataStore) >= rc.config.MaxStoredResults && rc.config.MaxStoredResults > 0 {
		// Simple ring buffer: remove the oldest element
		rc.dataStore = rc.dataStore[1:]
	}
	if rc.config.MaxStoredResults > 0 || len(rc.dataStore) < rc.config.MaxStoredResults {
		rc.dataStore = append(rc.dataStore, payload)
	}
	rc.mu.Unlock()

	// Forward data to Global Aggregator if configured
	if rc.config.GlobalAggregatorEndpoint != "" && rc.config.DC_ID != "" {
		// This is done synchronously. For high-throughput, consider a separate goroutine pool.
		rc.forwardToGlobalAggregator(payload)
	}

	// Respond to the agent
	w.WriteHeader(http.StatusAccepted) // 202 Accepted is good for async processing
	fmt.Fprintln(w, `{"status": "accepted"}`)
}

// forwardToGlobalAggregator sends the received probe data to the configured Global Aggregator.
func (rc *RegionalController) forwardToGlobalAggregator(probeData api.ProbeDataPayload) {
	regionalPayload := api.RegionalDataPayload{
		DC_ID:     rc.config.DC_ID,
		ProbeData: probeData,
	}

	jsonData, err := json.Marshal(regionalPayload)
	if err != nil {
		log.Printf("RegionalController: Error marshalling RegionalDataPayload to JSON: %v. Payload: %+v\n", err, regionalPayload)
		return
	}

	req, err := http.NewRequest(http.MethodPost, rc.config.GlobalAggregatorEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("RegionalController: Error creating HTTP request to Global Aggregator %s: %v\n", rc.config.GlobalAggregatorEndpoint, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rc.httpClient.Do(req)
	if err != nil {
		log.Printf("RegionalController: Error sending data to Global Aggregator %s: %v\n", rc.config.GlobalAggregatorEndpoint, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("RegionalController: Successfully forwarded data for DC '%s' to Global Aggregator %s, status: %s\n", rc.config.DC_ID, rc.config.GlobalAggregatorEndpoint, resp.Status)
	} else {
		log.Printf("RegionalController: Failed to forward data to Global Aggregator %s, status: %s\n", rc.config.GlobalAggregatorEndpoint, resp.Status)
	}
}

// GetData returns a copy of the currently stored probe data.
// This is a basic way to inspect data; a more robust solution might involve pagination or filtering.
func (rc *RegionalController) GetData() []api.ProbeDataPayload {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	// Return a copy to avoid external modification of the internal slice
	dataCopy := make([]api.ProbeDataPayload, len(rc.dataStore))
	copy(dataCopy, rc.dataStore)
	return dataCopy
}

// handleStatusPage serves a simple HTML page displaying the stored probe data.
func (rc *RegionalController) handleStatusPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	rc.mu.Lock()
	// Create a copy of the data to minimize lock holding time and for safe iteration
	// Sort data by timestamp, most recent first, for better display
	dataSnapshot := make([]api.ProbeDataPayload, len(rc.dataStore))
	copy(dataSnapshot, rc.dataStore)
	rc.mu.Unlock()

	sort.Slice(dataSnapshot, func(i, j int) bool {
		return dataSnapshot[j].Timestamp.Before(dataSnapshot[i].Timestamp) // Most recent first
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-f8")
	fmt.Fprintf(w, "<!DOCTYPE html>\n")
	fmt.Fprintf(w, "<html>\n<head>\n<title>Pingmesh Controller Status</title>\n")
	fmt.Fprintf(w, "<style>\n")
	fmt.Fprintf(w, "table { border-collapse: collapse; width: 90%%; margin: 20px auto; font-family: Arial, sans-serif; }\n")
	fmt.Fprintf(w, "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n")
	fmt.Fprintf(w, "th { background-color: #f2f2f2; }\n")
	fmt.Fprintf(w, "h1 { text-align: center; font-family: Arial, sans-serif; }\n")
	fmt.Fprintf(w, "</style>\n")
	fmt.Fprintf(w, "</head>\n<body>\n")
	fmt.Fprintf(w, "<h1>Pingmesh Probe Data</h1>\n")
	fmt.Fprintf(w, "<p style='text-align: center;'>Displaying last %d results (max %d). Newest first.</p>\n", len(dataSnapshot), rc.config.MaxStoredResults)

	if len(dataSnapshot) == 0 {
		fmt.Fprintf(w, "<p style='text-align: center;'>No data received yet.</p>\n")
	} else {
		fmt.Fprintf(w, "<table>\n")
		fmt.Fprintf(w, "<tr><th>Timestamp</th><th>Source IP</th><th>Destination IP</th><th>Latency (ms)</th><th>Jitter (ms)</th><th>Packet Loss (%%)</th><th>Source MAC</th><th>Dest MAC</th></tr>\n")

		for _, entry := range dataSnapshot {
			latencyMs := float64(entry.LatencyNS) / 1e6
			jitterMs := float64(entry.JitterNS) / 1e6
			fmt.Fprintf(w, "<tr>\n")
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.Timestamp.Format(time.RFC1123)))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.SourceIP))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.DestinationIP))
			fmt.Fprintf(w, "  <td>%.3f</td>\n", latencyMs)
			fmt.Fprintf(w, "  <td>%.3f</td>\n", jitterMs)
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(strconv.FormatFloat(float64(entry.PacketLoss), 'f', 2, 32)))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.SourceMac))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.DestinationMac))
			fmt.Fprintf(w, "</tr>\n")
		}
		fmt.Fprintf(w, "</table>\n")
	}

	fmt.Fprintf(w, "</body>\n</html>\n")
}
