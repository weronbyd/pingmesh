package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"html" // For escaping HTML content
	"log"
	"net/http"
	"sort" // For sorting data
	"strconv" // For float to string conversion
	"sync"
	"time"

	"pingmesh/internal/api"
)

// AggregatorConfig holds configuration for the GlobalAggregator.
type AggregatorConfig struct {
	ListenAddress         string
	MaxStoredRegionalData int           // Max total entries, or per DC if store is map[string][]api.RegionalDataPayload
	ShutdownTimeout       time.Duration // Graceful shutdown timeout
}

// DefaultAggregatorConfig returns a default configuration for the aggregator.
func DefaultAggregatorConfig() AggregatorConfig {
	return AggregatorConfig{
		ListenAddress:         ":9090",
		MaxStoredRegionalData: 10000, // Example: 10k total entries across all DCs
		ShutdownTimeout:       5 * time.Second,
	}
}

// GlobalAggregator manages the collection of data from regional controllers.
type GlobalAggregator struct {
	config    AggregatorConfig
	server    *http.Server
	dataStore []api.RegionalDataPayload // Simple slice for now, could be map[string][]api.RegionalDataPayload
	mu        sync.Mutex                // Protects dataStore
	wg        sync.WaitGroup
}

// NewGlobalAggregator creates a new GlobalAggregator.
func NewGlobalAggregator(config AggregatorConfig) *GlobalAggregator {
	return &GlobalAggregator{
		config:    config,
		dataStore: make([]api.RegionalDataPayload, 0, config.MaxStoredRegionalData),
	}
}

// Start initializes and starts the HTTP server for the aggregator.
func (ga *GlobalAggregator) Start() error {
	log.Printf("GlobalAggregator: Starting on %s...\n", ga.config.ListenAddress)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/submit-regional-data", ga.handleRegionalDataSubmit)
	mux.HandleFunc("/status", ga.handleAggregatorStatusPage) // New status page endpoint

	ga.server = &http.Server{
		Addr:    ga.config.ListenAddress,
		Handler: mux,
	}

	ga.wg.Add(1)
	go func() {
		defer ga.wg.Done()
		log.Printf("GlobalAggregator: HTTP server listening on %s\n", ga.config.ListenAddress)
		if err := ga.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("GlobalAggregator: HTTP server ListenAndServe error: %v\n", err)
		}
		log.Println("GlobalAggregator: HTTP server goroutine finished.")
	}()

	log.Println("GlobalAggregator: Started successfully.")
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (ga *GlobalAggregator) Stop() error {
	log.Println("GlobalAggregator: Stopping...")

	if ga.server == nil {
		log.Println("GlobalAggregator: Server not started or already stopped.")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), ga.config.ShutdownTimeout)
	defer cancel()

	if err := ga.server.Shutdown(ctx); err != nil {
		log.Printf("GlobalAggregator: HTTP server shutdown error: %v\n", err)
		return err
	}

	ga.wg.Wait() // Wait for the ListenAndServe goroutine to exit
	log.Println("GlobalAggregator: Stopped successfully.")
	return nil
}

// handleRegionalDataSubmit is the HTTP handler for POST /api/v1/submit-regional-data.
func (ga *GlobalAggregator) handleRegionalDataSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload api.RegionalDataPayload
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&payload); err != nil {
		log.Printf("GlobalAggregator: Error decoding JSON payload: %v\n", err)
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	log.Printf("GlobalAggregator: Received data from DC '%s': %+v\n", payload.DC_ID, payload.ProbeData)

	ga.mu.Lock()
	if len(ga.dataStore) >= ga.config.MaxStoredRegionalData && ga.config.MaxStoredRegionalData > 0 {
		// Simple ring buffer: remove the oldest element
		ga.dataStore = ga.dataStore[1:]
	}
	if ga.config.MaxStoredRegionalData > 0 || len(ga.dataStore) < ga.config.MaxStoredRegionalData {
		ga.dataStore = append(ga.dataStore, payload)
	}
	ga.mu.Unlock()

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, `{"status": "accepted"}`)
}

// GetData returns a copy of the currently stored regional probe data.
func (ga *GlobalAggregator) GetData() []api.RegionalDataPayload {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	dataCopy := make([]api.RegionalDataPayload, len(ga.dataStore))
	copy(dataCopy, ga.dataStore)
	return dataCopy
}

// handleAggregatorStatusPage serves a simple HTML page displaying the stored regional probe data.
func (ga *GlobalAggregator) handleAggregatorStatusPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	ga.mu.Lock()
	// Create a copy of the data to minimize lock holding time and for safe iteration
	dataSnapshot := make([]api.RegionalDataPayload, len(ga.dataStore))
	copy(dataSnapshot, ga.dataStore)
	ga.mu.Unlock()

	// Sort data by DC_ID, then by timestamp (most recent first)
	sort.Slice(dataSnapshot, func(i, j int) bool {
		if dataSnapshot[i].DC_ID != dataSnapshot[j].DC_ID {
			return dataSnapshot[i].DC_ID < dataSnapshot[j].DC_ID
		}
		return dataSnapshot[j].ProbeData.Timestamp.Before(dataSnapshot[i].ProbeData.Timestamp) // Most recent first
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-f8")
	fmt.Fprintf(w, "<!DOCTYPE html>\n")
	fmt.Fprintf(w, "<html>\n<head>\n<title>Global Aggregator Status</title>\n")
	fmt.Fprintf(w, "<style>\n")
	fmt.Fprintf(w, "table { border-collapse: collapse; width: 95%%; margin: 20px auto; font-family: Arial, sans-serif; }\n")
	fmt.Fprintf(w, "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n")
	fmt.Fprintf(w, "th { background-color: #f0f0f0; }\n") // Slightly different color for aggregator
	fmt.Fprintf(w, "h1 { text-align: center; font-family: Arial, sans-serif; }\n")
	fmt.Fprintf(w, "</style>\n")
	fmt.Fprintf(w, "</head>\n<body>\n")
	fmt.Fprintf(w, "<h1>Global Aggregator - Regional Probe Data</h1>\n")
	fmt.Fprintf(w, "<p style='text-align: center;'>Displaying last %d regional data submissions (max %d). Sorted by DC, then newest first.</p>\n", len(dataSnapshot), ga.config.MaxStoredRegionalData)

	if len(dataSnapshot) == 0 {
		fmt.Fprintf(w, "<p style='text-align: center;'>No data received yet from any Regional Controller.</p>\n")
	} else {
		fmt.Fprintf(w, "<table>\n")
		fmt.Fprintf(w, "<tr><th>DC ID</th><th>Timestamp</th><th>Source IP</th><th>Destination IP</th><th>Latency (ms)</th><th>Jitter (ms)</th><th>Packet Loss (%%)</th><th>Src MAC</th><th>Dst MAC</th></tr>\n")

		for _, entry := range dataSnapshot {
			pd := entry.ProbeData
			latencyMs := float64(pd.LatencyNS) / 1e6
			jitterMs := float64(pd.JitterNS) / 1e6
			fmt.Fprintf(w, "<tr>\n")
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(entry.DC_ID))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(pd.Timestamp.Format(time.RFC1123)))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(pd.SourceIP))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(pd.DestinationIP))
			fmt.Fprintf(w, "  <td>%.3f</td>\n", latencyMs)
			fmt.Fprintf(w, "  <td>%.3f</td>\n", jitterMs)
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(strconv.FormatFloat(float64(pd.PacketLoss), 'f', 2, 32)))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(pd.SourceMac))
			fmt.Fprintf(w, "  <td>%s</td>\n", html.EscapeString(pd.DestinationMac))
			fmt.Fprintf(w, "</tr>\n")
		}
		fmt.Fprintf(w, "</table>\n")
	}

	fmt.Fprintf(w, "</body>\n</html>\n")
}
