package probe

import "time"

// ProbeResult represents the result of a single probe.
type ProbeResult struct {
	SourceMac      string
	DestinationMac string
	SourceIP       string
	DestinationIP  string
	Latency        time.Duration
	PacketLoss     float32 // Percentage
	Jitter         time.Duration
	Timestamp      time.Time
}

// Prober defines the interface for network probing.
type Prober interface {
	Start() error
	Stop() error
	Results() <-chan ProbeResult // Channel to send results
	AddTarget(ip string) error
	RemoveTarget(ip string) error
	GetTargets() ([]string, error)
}
