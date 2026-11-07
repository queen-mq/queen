package feedrt

// Metrics is a feed runner's metrics (JS FeedRunner.metrics()). Map gives
// the JS shape (camelCase names, nulls where JS has them).
type Metrics struct {
	Role           string
	Cycles         int64
	EntriesRead    int64
	Emitted        int64
	Outputs        int64
	Effects        int64
	Commits        int64
	Conflicts      int64
	Errors         int64
	Retries        int64
	Poisoned       int64
	Skipped        int64
	DeadLettered   int64
	Late           int64
	Ticks          int64
	Takeovers      int64
	Snapshots      int64
	SnapshotBytes  int64
	SnapshotChunks int64
	SnapshotsFull  int64
	SnapshotLayers int64
	SnapshotMillis int64
	Units          int64
	CloseCommits   int64
	Replays        int64
	ReplayEntries  int64
	ReplayMillis   int64
	ReplaySkipped  int64
	TableRows      int64
	// Position is the committed position (a number, or "now"/"earliest"); nil before the first commit.
	Position interface{}
	Applied  *int64
	Lag      *int64
	// Stopped says why the runner stopped on its own (reason poison or config); nil otherwise.
	Stopped        map[string]interface{}
	PendingOutputs int64
	// Batches: the calls of a .foreachBatch() function that returned;
	// PendingBatch: the results its batch holds now (0 for other sinks).
	Batches      int64
	PendingBatch int64
	Shard        *ShardSpec

	// With state (a stateful step in the chain).
	HasState         bool
	Open             int64
	StateKeys        int64
	Steps            []StepMetrics
	Clock            *string
	Watermark        *string
	SnapshotPosition interface{}

	// A shard group (run with Shards): how many shards, how many active here, and each one's.
	Group    bool
	Shards   int
	Running  int
	PerShard []Metrics
}

// clone is a copy that shares nothing with the snapshot it came from.
func (m Metrics) clone() Metrics {
	if m.Applied != nil {
		v := *m.Applied
		m.Applied = &v
	}
	if m.Lag != nil {
		v := *m.Lag
		m.Lag = &v
	}
	if m.Stopped != nil {
		m.Stopped = copyMap(m.Stopped)
	}
	if m.Shard != nil {
		s := *m.Shard
		m.Shard = &s
	}
	if m.Clock != nil {
		c := *m.Clock
		m.Clock = &c
	}
	if m.Watermark != nil {
		w := *m.Watermark
		m.Watermark = &w
	}
	if m.Steps != nil {
		steps := make([]StepMetrics, len(m.Steps))
		for i, s := range m.Steps {
			if s.Watermark != nil {
				w := *s.Watermark
				s.Watermark = &w
			}
			if s.Held != nil {
				h := *s.Held
				s.Held = &h
			}
			steps[i] = s
		}
		m.Steps = steps
	}
	return m
}

func optInt(p *int64) interface{} {
	if p == nil {
		return nil
	}
	return float64(*p)
}

// Map is the metrics in the JS shape.
func (m Metrics) Map() map[string]interface{} {
	if m.Group {
		out := map[string]interface{}{"shards": float64(m.Shards), "running": float64(m.Running)}
		sums := map[string]int64{
			"cycles": m.Cycles, "entriesRead": m.EntriesRead, "emitted": m.Emitted, "outputs": m.Outputs, "effects": m.Effects,
			"commits": m.Commits, "conflicts": m.Conflicts, "errors": m.Errors, "retries": m.Retries, "poisoned": m.Poisoned,
			"skipped": m.Skipped, "deadLettered": m.DeadLettered, "late": m.Late, "ticks": m.Ticks, "takeovers": m.Takeovers,
			"snapshots": m.Snapshots, "replays": m.Replays, "replayEntries": m.ReplayEntries, "tableRows": m.TableRows,
			"pendingOutputs": m.PendingOutputs, "open": m.Open, "stateKeys": m.StateKeys,
			"batches": m.Batches, "pendingBatch": m.PendingBatch,
		}
		for k, v := range sums {
			out[k] = float64(v)
		}
		per := make([]interface{}, len(m.PerShard))
		for i, s := range m.PerShard {
			per[i] = s.Map()
		}
		out["perShard"] = per
		return out
	}
	var stopped interface{}
	if m.Stopped != nil {
		stopped = m.Stopped
	}
	out := map[string]interface{}{
		"role": m.Role, "cycles": float64(m.Cycles), "entriesRead": float64(m.EntriesRead), "emitted": float64(m.Emitted),
		"outputs": float64(m.Outputs), "effects": float64(m.Effects), "commits": float64(m.Commits),
		"conflicts": float64(m.Conflicts), "errors": float64(m.Errors), "retries": float64(m.Retries),
		"poisoned": float64(m.Poisoned), "skipped": float64(m.Skipped), "deadLettered": float64(m.DeadLettered),
		"late": float64(m.Late), "ticks": float64(m.Ticks), "takeovers": float64(m.Takeovers),
		"snapshots": float64(m.Snapshots), "snapshotBytes": float64(m.SnapshotBytes), "snapshotChunks": float64(m.SnapshotChunks),
		"snapshotsFull": float64(m.SnapshotsFull), "snapshotLayers": float64(m.SnapshotLayers), "snapshotMillis": float64(m.SnapshotMillis),
		"units": float64(m.Units), "closeCommits": float64(m.CloseCommits), "replays": float64(m.Replays),
		"replayEntries": float64(m.ReplayEntries), "replayMillis": float64(m.ReplayMillis), "replaySkipped": float64(m.ReplaySkipped),
		"tableRows": float64(m.TableRows), "position": m.Position, "applied": optInt(m.Applied), "lag": optInt(m.Lag),
		"stopped": stopped, "pendingOutputs": float64(m.PendingOutputs),
		"batches": float64(m.Batches), "pendingBatch": float64(m.PendingBatch),
	}
	if m.Shard != nil {
		out["shard"] = map[string]interface{}{"index": float64(m.Shard.Index), "count": float64(m.Shard.Count)}
	}
	if m.HasState {
		out["open"] = float64(m.Open)
		out["stateKeys"] = float64(m.StateKeys)
		steps := make([]interface{}, len(m.Steps))
		for i, s := range m.Steps {
			steps[i] = s.Map()
		}
		out["steps"] = steps
		if m.Clock != nil {
			out["clock"] = *m.Clock
		} else {
			out["clock"] = nil
		}
		if m.Watermark != nil {
			out["watermark"] = *m.Watermark
		}
		out["snapshotPosition"] = m.SnapshotPosition
	}
	return out
}
