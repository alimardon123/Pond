// history.rs — what a collection looked like before, and why it costs nothing
// on the write path.
//
// # The problem
//
// The engine publishes heads, not commits. That is what makes a commit one PUT
// with no coordination, and it is also why `pond history` had nothing to say:
// there is no chain to walk. Saying "(no commits)" was misleading — it sent
// people looking for commits rather than for the model.
//
// # Where history already is
//
// A publish writes a new head at a new sequence and deliberately does *not*
// delete the one it supersedes — retiring it would cost a round trip on the
// commit path to save readers nothing, since a superseded key is listed and
// never fetched. So between compactions, the superseded heads **are** the
// history: every root the collection has had, in order, still sitting in the
// store.
//
// Compaction is what removes them. So compaction is exactly where history has
// to be written down, and — conveniently — it is a maintenance pass that
// already reads every head and knows every root. Recording it there costs one
// object write per compaction and nothing at all per commit.
//
// That gives full granularity rather than one entry per compaction: a pass
// running after a hundred publishes sees all hundred superseded heads and
// records all hundred roots.
//
// # What is deliberately not here
//
// No timestamps beyond the sequence the writer stamped, because there is no
// global clock to trust and a fabricated one invites people to order events
// across writers by it.
//
// And the log is capped. Every root it names keeps a whole tree reachable, so
// an unbounded history is an unbounded storage bill — which is why every
// system with time travel has an expiry policy rather than a promise.

use std::collections::BTreeMap;

use pond_kernel::ObjectStore;

/// How many entries a collection's history keeps.
///
/// A retained root pins its whole tree against garbage collection — that is
/// the real cost of time travel, and it is why this is a cap rather than a
/// promise. Structural sharing makes consecutive entries cheap (they differ by
/// the nodes that changed) but not free.
pub const MAX_ENTRIES: usize = 64;

const MAGIC: &[u8; 4] = b"PHST";
const VERSION: u8 = 1;

/// Where a collection's history lives.
pub fn history_key(collection: &str) -> String {
    format!("history/{}", collection)
}

/// One published state of a collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The publishing writer's sequence. Meaningful within one writer; across
    /// writers it orders nothing, because there is no shared clock.
    pub seq: u64,
    /// Which writer published it.
    pub writer_id: u64,
    /// The collection's root at that point.
    pub root: String,
}

/// A collection's retained history, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub entries: Vec<Entry>,
}

impl History {
    /// Add entries, keeping the log ordered, deduplicated and capped.
    ///
    /// Deduplicated by (writer, seq): compaction re-reads heads it has already
    /// recorded, so without this a history would grow a duplicate on every
    /// pass and the cap would evict real entries to make room for them.
    pub fn absorb(&mut self, new: impl IntoIterator<Item = Entry>) {
        self.entries.extend(new);
        self.entries.sort_by(|a, b| {
            (a.seq, a.writer_id, &a.root).cmp(&(b.seq, b.writer_id, &b.root))
        });
        self.entries.dedup_by(|a, b| a.seq == b.seq && a.writer_id == b.writer_id);
        if self.entries.len() > MAX_ENTRIES {
            let drop = self.entries.len() - MAX_ENTRIES;
            self.entries.drain(..drop);
        }
    }

    /// Every root this history keeps reachable. Garbage collection needs these
    /// in its live set, or time travel points at deleted nodes.
    pub fn roots(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|e| e.root.as_str())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::from(*MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.seq.to_le_bytes());
            out.extend_from_slice(&e.writer_id.to_le_bytes());
            out.extend_from_slice(&(e.root.len() as u16).to_le_bytes());
            out.extend_from_slice(e.root.as_bytes());
        }
        out
    }

    /// Decode, returning `None` for anything this version does not understand.
    ///
    /// A history that cannot be read is not fatal anywhere: it is an
    /// observability record, so the caller treats it as empty and the data
    /// itself is untouched.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 || &bytes[..4] != MAGIC || bytes[4] != VERSION {
            return None;
        }
        let count = u32::from_le_bytes(bytes[5..9].try_into().ok()?) as usize;
        // Smallest entry is 8 + 8 + 2 = 18 bytes; refuse a count that cannot
        // fit rather than pre-allocating for it.
        if count.saturating_mul(18) > bytes.len().saturating_sub(9) {
            return None;
        }
        let mut pos = 9usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 18 > bytes.len() {
                return None;
            }
            let seq = u64::from_le_bytes(bytes[pos..pos + 8].try_into().ok()?);
            let writer_id = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().ok()?);
            let len = u16::from_le_bytes(bytes[pos + 16..pos + 18].try_into().ok()?) as usize;
            pos += 18;
            if pos + len > bytes.len() {
                return None;
            }
            let root = String::from_utf8(bytes[pos..pos + len].to_vec()).ok()?;
            pos += len;
            entries.push(Entry {
                seq,
                writer_id,
                root,
            });
        }
        Some(Self { entries })
    }
}

/// Read a collection's retained history. Absent or unreadable reads as empty.
pub fn load<S: ObjectStore + ?Sized>(store: &S, collection: &str) -> History {
    store
        .get_object(&history_key(collection))
        .and_then(|b| History::decode(&b))
        .unwrap_or_default()
}

/// Write a collection's history. One object per collection.
pub fn store_history<S: ObjectStore + ?Sized>(
    store: &S,
    collection: &str,
    history: &History,
) -> std::io::Result<()> {
    store.put_object(&history_key(collection), &history.encode())
}

/// Every collection that has a retained history.
pub fn collections<S: ObjectStore + ?Sized>(store: &S) -> Vec<String> {
    store
        .list_paths("history/")
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p.strip_prefix("history/").map(|s| s.to_string()))
        .collect()
}

/// Group entries by the collection they belong to.
pub type ByCollection = BTreeMap<String, Vec<Entry>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: u64, writer: u64, root: &str) -> Entry {
        Entry {
            seq,
            writer_id: writer,
            root: root.to_string(),
        }
    }

    #[test]
    fn round_trips() {
        let mut h = History::default();
        h.absorb([entry(1, 7, &"a".repeat(64)), entry(2, 7, &"b".repeat(64))]);
        assert_eq!(History::decode(&h.encode()), Some(h));
    }

    #[test]
    fn an_empty_history_round_trips() {
        let h = History::default();
        assert_eq!(History::decode(&h.encode()), Some(h));
    }

    /// Compaction re-reads heads it has already recorded, so absorbing the
    /// same entry twice must not grow the log — otherwise the cap evicts real
    /// history to make room for duplicates.
    #[test]
    fn absorbing_the_same_entry_twice_changes_nothing() {
        let mut h = History::default();
        h.absorb([entry(1, 7, "aa"), entry(2, 7, "bb")]);
        let once = h.clone();
        h.absorb([entry(1, 7, "aa"), entry(2, 7, "bb")]);
        assert_eq!(h, once);
    }

    #[test]
    fn entries_come_back_oldest_first_whatever_order_they_arrived() {
        let mut h = History::default();
        h.absorb([entry(3, 1, "c"), entry(1, 1, "a"), entry(2, 1, "b")]);
        let seqs: Vec<u64> = h.entries.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    /// The cap drops the oldest, not the newest — a history that forgets what
    /// just happened is worse than none.
    #[test]
    fn the_cap_evicts_the_oldest() {
        let mut h = History::default();
        h.absorb((0..(MAX_ENTRIES as u64 + 10)).map(|i| entry(i, 1, "r")));
        assert_eq!(h.entries.len(), MAX_ENTRIES);
        assert_eq!(h.entries.first().unwrap().seq, 10);
        assert_eq!(
            h.entries.last().unwrap().seq,
            MAX_ENTRIES as u64 + 9,
            "the most recent entry must survive the cap"
        );
    }

    /// Two writers at the same sequence are concurrent publishes, not
    /// duplicates, and both belong in the history.
    #[test]
    fn writers_at_the_same_sequence_are_both_kept() {
        let mut h = History::default();
        h.absorb([entry(5, 1, "from-one"), entry(5, 2, "from-two")]);
        assert_eq!(h.entries.len(), 2);
    }

    #[test]
    fn nonsense_bytes_are_refused_rather_than_guessed_at() {
        assert_eq!(History::decode(b""), None);
        assert_eq!(History::decode(b"NOPE"), None);
        let mut h = History::default();
        h.absorb([entry(1, 1, &"a".repeat(64))]);
        let full = h.encode();
        for cut in 1..12 {
            assert_eq!(
                History::decode(&full[..full.len() - cut]),
                None,
                "a history truncated by {} bytes must be refused",
                cut
            );
        }
    }

    /// A count field claiming more entries than the buffer can hold must not
    /// drive an allocation.
    #[test]
    fn an_absurd_count_is_refused_without_allocating() {
        let mut bytes = Vec::from(*MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(History::decode(&bytes), None);
    }
}
