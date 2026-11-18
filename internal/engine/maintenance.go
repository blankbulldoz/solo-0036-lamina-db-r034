package engine

import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// checkpointFlight tracks one in-flight image write. Sequence and started are
// fixed at capture; err is assigned before done is closed, so coalesced
// requests that observe done may read the result without further locking.
type checkpointFlight struct {
	sequence uint64
	started  time.Time
	done     chan struct{}
	shared   atomic.Int32
	err      error
}

// capture freezes the current committed position with a structural share: the
// catalog is cloned and the store's maps are copied, but version chains are
// shared with the live store. Chains are append-only, vacuum replaces them
// copy-on-write, and stored rows are immutable, so the image stays consistent
// while commits and vacuum proceed; versions it references stay pinned until
// the image is dropped. Callers must hold d.mu.
func (d *DB) capture() snapshot.Image {
	return snapshot.Image{Format: 1, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Share()}
}

// Checkpoint installs a durable image of one committed position. Only the
// capture runs under the database lock; encoding and the durable install run
// outside it, so commits and queries continue while the image is written. A
// single write is in flight at a time: concurrent requests coalesce onto it
// and receive the same captured position instead of copying state or queueing
// a second image. A failed write keeps the previously installed image and
// does not affect the database.
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return 0, err
	}
	if flight := d.checkpoint; flight != nil {
		flight.shared.Add(1)
		d.mu.Unlock()
		<-flight.done
		return flight.sequence, flight.err
	}
	flight := &checkpointFlight{sequence: d.sequence, started: time.Now().UTC(), done: make(chan struct{})}
	d.checkpoint = flight
	image := d.capture()
	d.mu.Unlock()
	err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), image)
	d.mu.Lock()
	d.checkpoint = nil
	if err != nil {
		d.checkpointError = err.Error()
		flight.err = fault("unavailable", "checkpoint: %s", err)
	} else {
		d.checkpointPublished = flight.sequence
		d.checkpointError = ""
	}
	close(flight.done)
	d.mu.Unlock()
	return flight.sequence, flight.err
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	image := d.capture()
	d.mu.Unlock()
	b, err := snapshot.Encode(image)
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}
func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	beforeKeys, beforeVersions := d.store.Count()
	_ = beforeKeys
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return versions.VacuumResult{}, err
	}
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	return versions.VacuumResult{Horizon: horizon, Removed: beforeVersions - remaining, Retained: remaining, Pinned: horizon < requested}, nil
}
