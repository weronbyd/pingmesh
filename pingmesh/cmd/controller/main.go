package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	// "time" // No longer needed directly here, controller config handles it

	"pingmesh/internal/controller"
)

func main() {
	log.Println("Starting Pingmesh Regional Controller...")

	// 1. Initialize Controller Configuration
	// Using default controller config for now.
	ctrlConfig := controller.DefaultControllerConfig()
	ctrlConfig.ListenAddress = ":8080" // Explicitly set, though it's the default
	ctrlConfig.MaxStoredResults = 1000
	ctrlConfig.DC_ID = "dc1-main"                                                          // Example DC ID
	ctrlConfig.GlobalAggregatorEndpoint = "http://localhost:9090/api/v1/submit-regional-data" // Aggregator endpoint
	// ctrlConfig.ShutdownTimeout = 10 * time.Second // Example of overriding
	log.Printf("Controller Configuration: %+v\n", ctrlConfig)

	// 2. Create a RegionalController instance
	regionalCtrl := controller.NewRegionalController(ctrlConfig)
	log.Println("RegionalController created.")

	// 3. Start the Controller
	if err := regionalCtrl.Start(); err != nil {
		log.Fatalf("Failed to start RegionalController: %v", err)
	}
	log.Println("RegionalController started successfully.")

	// 4. Set up signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Keep the main goroutine alive until a shutdown signal is received
	sig := <-shutdown
	log.Printf("Received signal: %v. Shutting down controller...\n", sig)

	// 5. Call Controller's Stop method
	if err := regionalCtrl.Stop(); err != nil {
		log.Printf("Error during controller shutdown: %v\n", err)
	} else {
		log.Println("Controller shutdown complete.")
	}
}
