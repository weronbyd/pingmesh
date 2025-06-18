package api

// RegionalDataPayload is the structure used for submitting probe results
// from a Regional Controller to the Global Aggregator.
type RegionalDataPayload struct {
	DC_ID     string           `json:"dc_id"`      // Identifier for the data center or region
	ProbeData ProbeDataPayload `json:"probe_data"` // The actual probe data
}
