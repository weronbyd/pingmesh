package probe

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ICMPProberConfig holds configuration for the ICMPProber.
type ICMPProberConfig struct {
	PingInterval time.Duration // How often to ping each target
	PingCount    int           // Number of pings to send each interval
	PingTimeout  time.Duration // Timeout for each individual ping
}

// DefaultICMPProberConfig returns a default configuration.
func DefaultICMPProberConfig() ICMPProberConfig {
	return ICMPProberConfig{
		PingInterval: 15 * time.Second,
		PingCount:    5,
		PingTimeout:  5 * time.Second,
	}
}

// ICMPProber implements the Prober interface using ICMP.
type ICMPProber struct {
	config  ICMPProberConfig
	targets map[string]bool
	mu      sync.RWMutex // To protect access to targets map
	results chan ProbeResult
	stop    chan struct{} // To signal the probing goroutine to stop
	wg      sync.WaitGroup  // To wait for goroutines to finish
}

// NewICMPProber creates a new ICMPProber.
// If conf is nil, default configuration is used.
func NewICMPProber(conf *ICMPProberConfig) (*ICMPProber, error) {
	cfg := DefaultICMPProberConfig()
	if conf != nil {
		cfg = *conf
	}

	return &ICMPProber{
		config:  cfg,
		targets: make(map[string]bool),
		results: make(chan ProbeResult), // Unbuffered channel
		stop:    make(chan struct{}),
	}, nil
}

// Start begins the ICMP probing.
func (p *ICMPProber) Start() error {
	fmt.Println("ICMPProber: Starting...")
	p.wg.Add(1)
	go p.runProbingLoop()
	fmt.Println("ICMPProber: Started.")
	return nil
}

// Stop halts the ICMP probing.
func (p *ICMPProber) Stop() error {
	fmt.Println("ICMPProber: Stopping...")
	close(p.stop) // Signal the probing loop to stop
	p.wg.Wait()   // Wait for the probing loop to finish
	// Note: The results channel is closed by the runProbingLoop when it exits.
	fmt.Println("ICMPProber: Stopped.")
	return nil
}

// Results returns a channel for receiving probe results.
func (p *ICMPProber) Results() <-chan ProbeResult {
	return p.results
}

// AddTarget adds a new IP target for probing.
func (p *ICMPProber) AddTarget(ip string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.targets[ip]; exists {
		return fmt.Errorf("target %s already exists", ip)
	}
	p.targets[ip] = true
	fmt.Printf("ICMPProber: Target %s added\n", ip)
	return nil
}

// RemoveTarget removes an IP target from probing.
func (p *ICMPProber) RemoveTarget(ip string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.targets[ip]; !exists {
		return fmt.Errorf("target %s not found", ip)
	}
	delete(p.targets, ip)
	fmt.Printf("ICMPProber: Target %s removed\n", ip)
	return nil
}

// runProbingLoop is the main goroutine for sending pings.
func (p *ICMPProber) runProbingLoop() {
	defer p.wg.Done()
	defer close(p.results) // Ensure results channel is closed when loop exits

	ticker := time.NewTicker(p.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			fmt.Println("ICMPProber: Probing loop stopping.")
			return
		case <-ticker.C:
			p.mu.RLock()
			targetsToProbe := make([]string, 0, len(p.targets))
			for target := range p.targets {
				targetsToProbe = append(targetsToProbe, target)
			}
			p.mu.RUnlock()

			for _, targetIP := range targetsToProbe {
				p.wg.Add(1) // Add to wait group for each target probe
				go p.probeTarget(targetIP)
			}
		}
	}
}

// probeTarget pings a single target and sends the result.
func (p *ICMPProber) probeTarget(targetIP string) {
	defer p.wg.Done() // Decrement counter when goroutine finishes

	// --- Placeholder for actual ping logic ---
	// In a real implementation, you would use a library like github.com/go-ping/ping here.
	// Example:
	// pinger, err := ping.NewPinger(targetIP)
	// if err != nil {
	// 	  fmt.Printf("ICMPProber: Error creating pinger for %s: %v\n", targetIP, err)
	//    return
	// }
	// pinger.SetPrivileged(true) // or false, depending on environment
	// pinger.Count = p.config.PingCount
	// pinger.Timeout = p.config.PingTimeout
	// pinger.Run() // Blocks until finished
	// stats := pinger.Statistics() // Get the results
	//
	// avgLatency := stats.AvgRtt
	// packetLoss := stats.PacketLoss
	// jitter := stats.StdDevRtt // Jitter can be approximated by StdDevRtt

	// Simulated results:
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond) // Simulate network delay
	avgLatency := time.Duration(rand.Intn(100)+10) * time.Millisecond // 10-110 ms
	packetLoss := rand.Float32() * 5                                 // 0-5% loss
	jitter := time.Duration(rand.Intn(10)+1) * time.Millisecond      // 1-11 ms

	// Simulate some packet loss affecting results
	if rand.Float32() < packetLoss/100 {
		fmt.Printf("ICMPProber: Simulating full packet loss for a cycle to %s\n", targetIP)
		// Send a result indicating high loss or timeout
		// For now, we just skip sending a "successful" result for this cycle,
		// or we could send one with 100% loss for this particular probe attempt.
		// Actual library might give 0 successful packets.
		packetLoss = 100.0
		avgLatency = p.config.PingTimeout // Max out latency on full loss
	}

	// --- End of Placeholder ---

	result := ProbeResult{
		SourceIP:      "", // Source IP usually known by agent, can be filled by caller
		DestinationIP: targetIP,
		Latency:       avgLatency,
		PacketLoss:    packetLoss,
		Jitter:        jitter, // Placeholder value
		Timestamp:     time.Now(),
		// SourceMac and DestinationMac are typically not available with standard ICMP pings
	}

	select {
	case p.results <- result:
	case <-p.stop:
		fmt.Printf("ICMPProber: Probe for target %s cancelled during result send.\n", targetIP)
		return
	default:
		// This case can happen if the results channel is blocked (e.g. not being read from)
		// and we need to prevent probeTarget goroutines from leaking.
		// However, with an unbuffered results channel, the sender will block until a receiver is ready.
		// If the channel is buffered and full, this default case would be hit.
		// For an unbuffered channel, if Stop() is called and runProbingLoop closes p.results,
		// then `p.results <- result` would panic. The select with <-p.stop handles this.
		fmt.Printf("ICMPProber: Results channel full or closed, dropping result for %s\n", targetIP)
	}
}

// Helper to get a list of current targets safely
func (p *ICMPProber) getTargets() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	targets := make([]string, 0, len(p.targets))
	for t := range p.targets {
		targets = append(targets, t)
	}
	return targets
}

// GetTargets returns a list of current targets.
func (p *ICMPProber) GetTargets() ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	targets := make([]string, 0, len(p.targets))
	for target := range p.targets {
		targets = append(targets, target)
	}
	return targets, nil
}
