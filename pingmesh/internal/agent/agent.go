package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"pingmesh/internal/api" // Added for ProbeDataPayload
	"pingmesh/internal/probe"
)

	"context"
)

// AgentConfig holds configuration for the Agent.
type AgentConfig struct {
	InitialTargets     []string
	ControllerEndpoint string
	ListenAddress      string        // Address for the agent's own HTTP API (e.g., ":8081")
	ShutdownTimeout    time.Duration // Graceful shutdown for agent's API server
	APITLSCertFile   string        // Path to TLS certificate file for agent API
	APITLSKeyFile    string        // Path to TLS key file for agent API
}

// DefaultAgentConfig returns a default configuration.
func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		InitialTargets:     []string{},
		ControllerEndpoint: "",
		ListenAddress:      "", // API server disabled by default
		ShutdownTimeout:    5 * time.Second,
		APITLSCertFile:   "", // TLS disabled by default
		APITLSKeyFile:    "", // TLS disabled by default
	}
}

// Agent manages a prober and forwards its data, and exposes an API for target management.
type Agent struct {
	config     AgentConfig
	prober     probe.Prober
	httpClient *http.Client // For sending data to controller
	apiServer  *http.Server // For the agent's own API
	shutdownChan chan struct{}
	wg         sync.WaitGroup
}

// NewAgent creates a new Agent.
func NewAgent(cfg AgentConfig, prober probe.Prober) (*Agent, error) {
	if prober == nil {
		return nil, fmt.Errorf("prober cannot be nil")
	}
	// ControllerEndpoint can be empty if agent is not expected to send data
	// ListenAddress can be empty if agent API is not needed

	agent := &Agent{
		config: cfg,
		prober: prober,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Default timeout for HTTP requests to controller
		},
		shutdownChan: make(chan struct{}),
	}

	if cfg.ListenAddress != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/targets/add", agent.handleAddTarget)
		mux.HandleFunc("/targets/remove", agent.handleRemoveTarget)
		mux.HandleFunc("/targets/list", agent.handleListTargets)
		agent.apiServer = &http.Server{
			Addr:    cfg.ListenAddress,
			Handler: mux,
		}
	}

	return agent, nil
}

// Start initializes the agent and begins probing, data forwarding, and its own API server.
func (a *Agent) Start() error {
	log.Println("Agent: Starting...")

	// Add initial targets to the prober
	for _, target := range a.config.InitialTargets {
		if err := a.prober.AddTarget(target); err != nil {
			// Log error but try to continue with other targets
			log.Printf("Agent: Failed to add initial target %s: %v\n", target, err)
		}
	}

	if err := a.prober.Start(); err != nil {
		return fmt.Errorf("agent failed to start prober: %w", err)
	}

	if a.config.ControllerEndpoint != "" {
		a.wg.Add(1)
		go a.runForwardDataLoop()
		log.Println("Agent: Data forwarding loop started.")
	} else {
		log.Println("Agent: No ControllerEndpoint configured, data forwarding disabled.")
	}

	if a.apiServer != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			useTLS := a.config.APITLSCertFile != "" && a.config.APITLSKeyFile != ""
			protocol := "HTTP"
			if useTLS {
				protocol = "HTTPS"
			}
			log.Printf("Agent: API server starting on %s (%s)...\n", a.config.ListenAddress, protocol)

			var err error
			if useTLS {
				err = a.apiServer.ListenAndServeTLS(a.config.APITLSCertFile, a.config.APITLSKeyFile)
			} else {
				err = a.apiServer.ListenAndServe()
			}

			if err != http.ErrServerClosed {
				log.Printf("Agent: API server ListenAndServe (%s) error: %v\n", protocol, err)
			} else {
				log.Printf("Agent: API server (%s) on %s shut down gracefully or was closed.\n", protocol, a.config.ListenAddress)
			}
			log.Println("Agent: API server goroutine finished.")
		}()
	}

	log.Println("Agent: Started successfully.")
	return nil
}

// Stop gracefully shuts down the agent.
func (a *Agent) Stop() error {
	log.Println("Agent: Stopping...")
	close(a.shutdownChan) // Signal forwardData loop and any other internal loops to stop

	// Stop the API server first
	if a.apiServer != nil {
		log.Println("Agent: Shutting down API server...")
		ctx, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
		defer cancel()
		if err := a.apiServer.Shutdown(ctx); err != nil {
			log.Printf("Agent: API server shutdown error: %v\n", err)
		} else {
			log.Println("Agent: API server stopped.")
		}
	}

	// Stop the prober
	if err := a.prober.Stop(); err != nil {
		log.Printf("Agent: Error stopping prober: %v\n", err)
	} else {
		log.Println("Agent: Prober stopped.")
	}

	a.wg.Wait() // Wait for forwardData loop and API server goroutine to finish
	log.Println("Agent: Stopped successfully.")
	return nil
}

// --- API Handlers ---

type targetManagementRequest struct {
	Target string `json:"target"`
}

func (a *Agent) handleAddTarget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}
	var reqPayload targetManagementRequest
	if err := json.NewDecoder(r.Body).Decode(&reqPayload); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if reqPayload.Target == "" {
		http.Error(w, "Target IP cannot be empty", http.StatusBadRequest)
		return
	}

	if err := a.prober.AddTarget(reqPayload.Target); err != nil {
		log.Printf("Agent API: Error adding target %s: %v\n", reqPayload.Target, err)
		http.Error(w, fmt.Sprintf("Failed to add target %s: %v", reqPayload.Target, err), http.StatusInternalServerError)
		return
	}

	log.Printf("Agent API: Target %s added successfully via API\n", reqPayload.Target)
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"status": "success", "message": "Target %s added"}`, reqPayload.Target)
}

func (a *Agent) handleRemoveTarget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}
	var reqPayload targetManagementRequest
	if err := json.NewDecoder(r.Body).Decode(&reqPayload); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if reqPayload.Target == "" {
		http.Error(w, "Target IP cannot be empty", http.StatusBadRequest)
		return
	}

	if err := a.prober.RemoveTarget(reqPayload.Target); err != nil {
		log.Printf("Agent API: Error removing target %s: %v\n", reqPayload.Target, err)
		http.Error(w, fmt.Sprintf("Failed to remove target %s: %v", reqPayload.Target, err), http.StatusInternalServerError)
		return
	}

	log.Printf("Agent API: Target %s removed successfully via API\n", reqPayload.Target)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": "success", "message": "Target %s removed"}`, reqPayload.Target)
}

func (a *Agent) handleListTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	targets, err := a.prober.GetTargets()
	if err != nil {
		log.Printf("Agent API: Error getting targets: %v\n", err)
		http.Error(w, "Failed to retrieve targets: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(targets); err != nil {
		log.Printf("Agent API: Error encoding targets to JSON: %v\n", err)
		// Hard to send an error back if we already started writing JSON
	}
}

// --- End API Handlers ---

// runForwardDataLoop processes results from the prober and "sends" them.
func (a *Agent) runForwardDataLoop() {
	defer a.wg.Done()
	log.Println("Agent: Starting data forwarding loop...")

	resultsChan := a.prober.Results()

	for {
		select {
		case result, ok := <-resultsChan:
			if !ok {
				log.Println("Agent: Prober results channel closed. Exiting forwardData loop.")
				return
			}
			a.processAndForwardResult(result)
		case <-a.shutdownChan:
			log.Println("Agent: Shutdown signal received. Exiting forwardData loop.")
			// Drain any remaining results in the channel to allow prober to shutdown cleanly if it's blocked sending.
			// This is a simple drain, more complex handling might be needed if resultsChan is buffered
			// and has many pending items.
			for result := range resultsChan {
				log.Println("Agent: Processing one last result after shutdown signal...")
				a.processAndForwardResult(result)
			}
			return
		}
	}
}

func (a *Agent) processAndForwardResult(result probe.ProbeResult) {
	// Convert probe.ProbeResult to api.ProbeDataPayload
	payload := api.ProbeDataPayload{
		SourceMac:      result.SourceMac,
		DestinationMac: result.DestinationMac,
		SourceIP:       result.SourceIP,
		DestinationIP:  result.DestinationIP,
		LatencyNS:      result.Latency.Nanoseconds(),
		PacketLoss:     result.PacketLoss,
		JitterNS:       result.Jitter.Nanoseconds(),
		Timestamp:      result.Timestamp,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Agent: Error marshalling ProbeDataPayload to JSON: %v. Payload: %+v\n", err, payload)
		return
	}

	req, err := http.NewRequest(http.MethodPost, a.config.ControllerEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Agent: Error creating HTTP request to %s: %v\n", a.config.ControllerEndpoint, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		log.Printf("Agent: Error sending data to controller %s: %v\n", a.config.ControllerEndpoint, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("Agent: Successfully sent data to %s, status: %s\n", a.config.ControllerEndpoint, resp.Status)
	} else {
		log.Printf("Agent: Failed to send data to %s, status: %s\n", a.config.ControllerEndpoint, resp.Status)
		// Optionally, log response body for more details
		// responseBody, _ := io.ReadAll(resp.Body)
		// log.Printf("Agent: Controller response body: %s\n", string(responseBody))
	}
}

// AddTarget dynamically adds a target to the prober.
func (a *Agent) AddTarget(ip string) error {
	log.Printf("Agent: Attempting to add target %s\n", ip)
	return a.prober.AddTarget(ip)
}

// RemoveTarget dynamically removes a target from the prober.
func (a *Agent) RemoveTarget(ip string) error {
	log.Printf("Agent: Attempting to remove target %s\n", ip)
	return a.prober.RemoveTarget(ip)
}
