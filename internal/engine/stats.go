package engine

import (
	"sort"
	"time"
)

type CheckpointStatus struct {
	Phase     string `json:"phase"`
	Sequence  uint64 `json:"sequence"`
	Started   string `json:"started,omitempty"`
	Shared    int32  `json:"shared"`
	Published uint64 `json:"published"`
	Error     string `json:"error,omitempty"`
}
type Stats struct {
	Sequence       uint64           `json:"sequence"`
	Horizon        uint64           `json:"horizon"`
	Active         []Transaction    `json:"active"`
	Keys           int              `json:"keys"`
	Versions       int              `json:"versions"`
	IndexEntries   map[string]int   `json:"index_entries"`
	JournalRecords int              `json:"journal_records"`
	HistoryBase    uint64           `json:"history_base"`
	RecoveredTail  int64            `json:"recovered_tail_bytes"`
	Commits        uint64           `json:"commits_since_open"`
	Conflicts      uint64           `json:"conflicts_since_open"`
	Queries        uint64           `json:"queries_since_open"`
	Checkpoint     CheckpointStatus `json:"checkpoint"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries}
	s.Checkpoint = CheckpointStatus{Phase: "idle", Published: d.checkpointPublished, Error: d.checkpointError}
	if flight := d.checkpoint; flight != nil {
		s.Checkpoint.Phase = "writing"
		s.Checkpoint.Sequence = flight.sequence
		s.Checkpoint.Started = flight.started.Format(time.RFC3339Nano)
		s.Checkpoint.Shared = flight.shared.Load()
	}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	return s, nil
}
