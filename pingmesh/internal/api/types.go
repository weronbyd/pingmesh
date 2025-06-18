package api

import "time"

// ProbeDataPayload is the structure used for submitting probe results to the controller.
// It mirrors probe.ProbeResult but uses int64 for durations to simplify JSON handling.
type ProbeDataPayload struct {
	SourceMac      string    `json:"source_mac,omitempty"`
	DestinationMac string    `json:"destination_mac,omitempty"`
	SourceIP       string    `json:"source_ip"`
	DestinationIP  string    `json:"destination_ip"`
	LatencyNS      int64     `json:"latency_ns"`       // Latency in nanoseconds
	PacketLoss     float32   `json:"packet_loss"`      // Percentage
	JitterNS       int64     `json:"jitter_ns"`        // Jitter in nanoseconds
	Timestamp      time.Time `json:"timestamp"`
}
