package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pingmesh/internal/agent"
	"pingmesh/internal/probe"
)

func main() {
	log.Println("Starting Pingmesh Agent...")

	// 1. Initialize Agent Configuration
	agentConfig := agent.DefaultAgentConfig() // Start with defaults
	agentConfig.InitialTargets = []string{"8.8.8.8", "1.1.1.1"} // Example public DNS servers
	agentConfig.ControllerEndpoint = "http://localhost:8080/api/v1/submit-probe-data"
	agentConfig.ListenAddress = ":8081" // Agent's own API listens on port 8081
	// agentConfig.ShutdownTimeout = 10*time.Second // Example override for agent's API server shutdown
	// For TLS on Agent API:
	// agentConfig.APITLSCertFile = "/path/to/agent.crt"
	// agentConfig.APITLSKeyFile = "/path/to/agent.key"
	// Ensure these files exist and are readable by the agent process if uncommented.
	log.Printf("Agent Configuration: %+v\n", agentConfig)

	// 2. Create an ICMPProberConfig
	// Using default prober config for now. Can be customized.
	icmpConfig := probe.DefaultICMPProberConfig()
	icmpConfig.PingInterval = 10 * time.Second // Override default for quicker testing
	icmpConfig.PingCount = 3
	log.Printf("ICMP Prober Configuration: %+v\n", icmpConfig)

	// 3. Create an ICMPProber instance
	icmpProber, err := probe.NewICMPProber(&icmpConfig)
	if err != nil {
		log.Fatalf("Failed to create ICMPProber: %v", err)
	}
	log.Println("ICMPProber created.")

	// 4. Create a new Agent instance
	appAgent, err := agent.NewAgent(agentConfig, icmpProber)
	if err != nil {
		log.Fatalf("Failed to create Agent: %v", err)
	}
	log.Println("Agent created.")

	// 5. Start the Agent
	if err := appAgent.Start(); err != nil {
		log.Fatalf("Failed to start Agent: %v", err)
	}
	log.Println("Agent started successfully.")

	// 6. Set up signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Keep the main goroutine alive until a shutdown signal is received
	sig := <-shutdown
	log.Printf("Received signal: %v. Shutting down agent...\n", sig)

	// 7. Call Agent's Stop method
	if err := appAgent.Stop(); err != nil {
		log.Printf("Error during agent shutdown: %v\n", err)
	} else {
		log.Println("Agent shutdown complete.")
	}
}
