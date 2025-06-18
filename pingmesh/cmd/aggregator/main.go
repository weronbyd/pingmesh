package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"pingmesh/internal/aggregator"
)

func main() {
	log.Println("Starting Pingmesh Global Aggregator...")

	// 1. Initialize Aggregator Configuration
	aggConfig := aggregator.DefaultAggregatorConfig()
	// aggConfig.ListenAddress = ":9090" // Default, can be overridden
	// aggConfig.MaxStoredRegionalData = 20000 // Example override
	log.Printf("Global Aggregator Configuration: %+v\n", aggConfig)

	// 2. Create a GlobalAggregator instance
	globalAgg := aggregator.NewGlobalAggregator(aggConfig)
	log.Println("GlobalAggregator created.")

	// 3. Start the Aggregator
	if err := globalAgg.Start(); err != nil {
		log.Fatalf("Failed to start GlobalAggregator: %v", err)
	}
	log.Println("GlobalAggregator started successfully.")

	// 4. Set up signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Keep the main goroutine alive until a shutdown signal is received
	sig := <-shutdown
	log.Printf("Received signal: %v. Shutting down Global Aggregator...\n", sig)

	// 5. Call Aggregator's Stop method
	if err := globalAgg.Stop(); err != nil {
		log.Printf("Error during Global Aggregator shutdown: %v\n", err)
	} else {
		log.Println("Global Aggregator shutdown complete.")
	}
}
