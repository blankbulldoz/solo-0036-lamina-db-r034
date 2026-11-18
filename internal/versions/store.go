package versions

import "lamina/internal/datum"

type Mutation struct {
	Relation string    `json:"relation"`
	Key      string    `json:"key"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Version struct {
	Sequence uint64    `json:"sequence"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Store struct {
	Tables map[string]map[string][]Version `json:"tables"`
}

func New() *Store { return &Store{Tables: map[string]map[string][]Version{}} }
func (s *Store) Ensure(name string) {
	if s.Tables[name] == nil {
		s.Tables[name] = map[string][]Version{}
	}
}
func (s *Store) Apply(seq uint64, writes []Mutation) {
	for _, m := range writes {
		s.Ensure(m.Relation)
		s.Tables[m.Relation][m.Key] = append(s.Tables[m.Relation][m.Key], Version{Sequence: seq, Row: datum.Clone(m.Row), Deleted: m.Deleted})
	}
}
func Visible(chain []Version, at uint64) (Version, bool) {
	lo, hi := 0, len(chain)
	for lo < hi {
		mid := (lo + hi) / 2
		if chain[mid].Sequence <= at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Version{}, false
	}
	v := chain[lo-1]
	return v, !v.Deleted
}
func (s *Store) Read(table, key string, at uint64) (datum.Row, bool) {
	v, ok := Visible(s.Tables[table][key], at)
	return datum.Clone(v.Row), ok
}
func (s *Store) Latest(table, key string) uint64 {
	v := s.Tables[table][key]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Sequence
}
func (s *Store) Scan(table string, at uint64) map[string]datum.Row {
	out := map[string]datum.Row{}
	for k, c := range s.Tables[table] {
		if v, ok := Visible(c, at); ok {
			out[k] = datum.Clone(v.Row)
		}
	}
	return out
}

// Share copies the store's map structure while sharing every version chain
// with the receiver. Chains are append-only, Vacuum installs freshly allocated
// chains instead of editing existing ones, and stored rows are immutable, so
// the shared store stays consistent while the live store commits and vacuums.
// Versions referenced here remain pinned until the shared store is dropped.
func (s *Store) Share() *Store {
	out := New()
	for name, rows := range s.Tables {
		shared := make(map[string][]Version, len(rows))
		for key, chain := range rows {
			shared[key] = chain
		}
		out.Tables[name] = shared
	}
	return out
}
func (s *Store) Clone() *Store {
	out := New()
	for name, rows := range s.Tables {
		out.Ensure(name)
		for key, chain := range rows {
			for _, v := range chain {
				v.Row = datum.Clone(v.Row)
				out.Tables[name][key] = append(out.Tables[name][key], v)
			}
		}
	}
	return out
}
func (s *Store) Count() (keys, versions int) {
	for _, rows := range s.Tables {
		keys += len(rows)
		for _, v := range rows {
			versions += len(v)
		}
	}
	return
}
