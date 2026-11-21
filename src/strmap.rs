//! A sharded hash map from byte strings to values, for parsing in parallel.
//! Keys are compared byte for byte, so it is exact: a hash is only a hint.
//!
//! One side of it also works as an interner (`intern`), handing out dense
//! `u32` ids so the rest of the pipeline can use arrays instead of maps.
//!
//! Lookups that touch shared state take a shard lock, so the parse passes hand
//! their work over in batches (`intern_batch`, `upsert_batch`): one lock per
//! shard per batch instead of one per row.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

const SHARDS: usize = 16;

/// FxHash with a finalizer: fast on short ids, good enough spread.
fn hash_bytes(key: &[u8]) -> u64 {
    const K: u64 = 0x517c_c1b7_2722_0a95;
    let mut h: u64 = 0x517c_c1b7_2722_0a95;
    let mut rest = key;
    while rest.len() >= 8 {
        let mut word = [0u8; 8];
        word.copy_from_slice(&rest[..8]);
        h = (h ^ u64::from_le_bytes(word)).wrapping_mul(K);
        rest = &rest[8..];
    }
    if !rest.is_empty() {
        let mut word = [0u8; 8];
        word[..rest.len()].copy_from_slice(rest);
        h = (h ^ u64::from_le_bytes(word)).wrapping_mul(K);
    }
    h ^= h >> 29;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 32;
    h
}

pub struct Entry<V> {
    pub off: u32,
    pub len: u32,
    pub val: V,
}

/// Byte comparison without a `memcmp` call: the keys here are short ids.
#[inline]
fn bytes_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut at = 0;
    while at + 8 <= a.len() {
        let (x, y) = (
            u64::from_le_bytes(a[at..at + 8].try_into().unwrap()),
            u64::from_le_bytes(b[at..at + 8].try_into().unwrap()),
        );
        if x != y {
            return false;
        }
        at += 8;
    }
    while at < a.len() {
        if a[at] != b[at] {
            return false;
        }
        at += 1;
    }
    true
}

struct Shard<V> {
    slots: Vec<u32>, // 0 = empty, else (tag << 25) | entry index + 1
    entries: Vec<Entry<V>>,
    bytes: Vec<u8>,
}

/// Entries one shard can hold: 25 bits of index in a slot.
const MAX_ENTRIES: usize = 1 << 25;

pub struct StrMap<V> {
    shards: Vec<Mutex<Shard<V>>>,
    next_id: AtomicU32,
}

impl<V: Copy> StrMap<V> {
    pub fn new() -> Self {
        StrMap {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        slots: vec![0u32; 64],
                        entries: Vec::new(),
                        bytes: Vec::new(),
                    })
                })
                .collect(),
            next_id: AtomicU32::new(0),
        }
    }

    /// Makes room for roughly `entries` keys of `key_bytes` in total, so that
    /// parsing never grows a table while a shard is locked. Only ever grows.
    pub fn reserve(&self, entries: usize, key_bytes: usize) {
        let per_shard = entries / SHARDS + 64;
        let mut slots = 64;
        while slots / 2 < per_shard {
            slots *= 2;
        }
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            if shard.slots.len() < slots {
                shard.slots = vec![0u32; slots];
                shard.rehash();
            }
            shard.bytes.reserve(key_bytes / SHARDS + 1024);
            shard.entries.reserve(per_shard);
        }
    }

    /// `upsert` for a whole batch, taking each shard's lock once. `keys` and
    /// `vals` hold one entry each, in the same order.
    pub fn upsert_batch(&self, keys: &[&[u8]], vals: &[V], merge: &impl Fn(&mut V, V)) {
        debug_assert_eq!(keys.len(), vals.len());
        let mut hashes: Vec<u64> = Vec::with_capacity(keys.len());
        let mut buckets: Vec<Vec<u32>> = (0..SHARDS).map(|_| Vec::new()).collect();
        for (index, key) in keys.iter().enumerate() {
            let h = hash_bytes(key);
            hashes.push(h);
            buckets[(h as usize) & (SHARDS - 1)].push(index as u32);
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut shard = self.shards[s].lock().unwrap();
            for index in bucket {
                let index = index as usize;
                shard.upsert_hashed(hashes[index], keys[index], vals[index], merge);
            }
        }
    }

    /// `for_each_value` in parallel over the shards, so the walk is never a
    /// serial pass over the whole map.
    pub fn par_for_each_value<F: Fn(V) + Sync + Send>(&self, each: F)
    where
        V: Send,
    {
        use rayon::prelude::*;
        self.shards.par_iter().for_each(|shard| {
            let shard = shard.lock().unwrap();
            for entry in &shard.entries {
                each(entry.val);
            }
        });
    }

    /// `drain_values` in parallel over the shards. `each` must be safe to call
    /// from several threads at once.
    pub fn par_drain_values<F: Fn(V) + Sync + Send>(&self, each: F)
    where
        V: Send,
    {
        use rayon::prelude::*;
        self.shards.par_iter().for_each(|shard| {
            let mut shard = shard.lock().unwrap();
            let entries = std::mem::take(&mut shard.entries);
            shard.bytes = Vec::new();
            shard.slots = Vec::new();
            for entry in &entries {
                each(entry.val);
            }
        });
    }

    /// Copies every key and value out of the map, freeing each shard as it is
    /// done with. The keys are appended to `into` and `each` is called with the
    /// value and the key's range there.
    pub fn drain_into<F>(&self, into: &mut Vec<u8>, mut each: F)
    where
        F: FnMut(V, u32, u32),
    {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            let entries = std::mem::take(&mut shard.entries);
            let bytes = std::mem::take(&mut shard.bytes);
            shard.slots = Vec::new();
            for entry in &entries {
                let off = into.len() as u32;
                let key = &bytes[entry.off as usize..(entry.off + entry.len) as usize];
                into.extend_from_slice(key);
                each(entry.val, off, entry.len);
            }
        }
    }
}

impl<V: Copy + Hint> StrMap<V> {
    /// Interns `key`, returning a dense id: 0, 1, 2, ... in insertion order.
    pub fn intern(&self, key: &[u8]) -> u32 {
        let h = hash_bytes(key);
        let mut shard = self.shard(h).lock().unwrap();
        shard.intern_hashed(h, key, &self.next_id)
    }

    /// `intern` for a whole batch, taking each shard's lock once. The ids come
    /// back in `out`, one per key.
    pub fn intern_batch(&self, keys: &[&[u8]], out: &mut [u32]) {
        let mut hashes: Vec<u64> = Vec::with_capacity(keys.len());
        let mut buckets: Vec<Vec<u32>> = (0..SHARDS).map(|_| Vec::new()).collect();
        for (index, key) in keys.iter().enumerate() {
            let h = hash_bytes(key);
            hashes.push(h);
            buckets[(h as usize) & (SHARDS - 1)].push(index as u32);
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut shard = self.shards[s].lock().unwrap();
            for index in bucket {
                let index = index as usize;
                out[index] = shard.intern_hashed(hashes[index], keys[index], &self.next_id);
            }
        }
    }
}

impl<V: Copy> StrMap<V> {
    fn shard<'a>(&'a self, h: u64) -> &'a Mutex<Shard<V>> {
        &self.shards[(h as usize) & (SHARDS - 1)]
    }
}

impl<V: Copy> Shard<V> {
    fn upsert_hashed(
        &mut self,
        h: u64,
        key: &[u8],
        new: V,
        merge: impl FnOnce(&mut V, V),
    ) -> V {
        let tag = ((h >> 32) & 0x7f) as u32;
        let mut at = match self.probe(h, tag, key) {
            Found::Present(index) => {
                let entry = &mut self.entries[index];
                merge(&mut entry.val, new);
                return entry.val;
            }
            Found::Empty(at) => at,
        };
        if self.entries.len() + 1 > self.slots.len() / 2 {
            self.grow();
            at = match self.probe(h, tag, key) {
                Found::Present(index) => {
                    let entry = &mut self.entries[index];
                    merge(&mut entry.val, new);
                    return entry.val;
                }
                Found::Empty(at) => at,
            };
        }
        let off = self.bytes.len() as u32;
        self.bytes.extend_from_slice(key);
        self.entries.push(Entry {
            off,
            len: key.len() as u32,
            val: new,
        });
        self.slots[at] = slot_word(tag, self.entries.len());
        new
    }

    fn probe(&self, h: u64, tag: u32, key: &[u8]) -> Found {
        let mask = (self.slots.len() - 1) as u64;
        let mut at = h & mask;
        loop {
            let slot = self.slots[at as usize];
            if slot == 0 {
                return Found::Empty(at as usize);
            }
            let index = (slot & 0x01ff_ffff) as usize - 1;
            if slot >> 25 == tag {
                let entry = &self.entries[index];
                if bytes_eq(&self.bytes[entry.off as usize..(entry.off + entry.len) as usize], key)
                {
                    return Found::Present(index);
                }
            }
            at = (at + 1) & mask;
        }
    }

    fn grow(&mut self) {
        self.slots = vec![0u32; self.slots.len() * 2];
        self.rehash();
    }

    /// Puts every entry back into `slots`, which the caller has just resized.
    fn rehash(&mut self) {
        let mask = (self.slots.len() - 1) as u64;
        for (index, entry) in self.entries.iter().enumerate() {
            let key = &self.bytes[entry.off as usize..(entry.off + entry.len) as usize];
            let h = hash_bytes(key);
            let tag = ((h >> 32) & 0x7f) as u32;
            let mut at = h & mask;
            loop {
                if self.slots[at as usize] == 0 {
                    self.slots[at as usize] = slot_word(tag, index + 1);
                    break;
                }
                at = (at + 1) & mask;
            }
        }
    }
}

/// A slot is a 7-bit tag over a 25-bit entry index + 1 (0 means empty).
#[inline]
fn slot_word(tag: u32, index_plus_one: usize) -> u32 {
    assert!(index_plus_one < MAX_ENTRIES, "too many keys in one map shard");
    tag << 25 | index_plus_one as u32
}

enum Found {
    Present(usize),
    Empty(usize),
}

/// A byte-string map that is built once and then only read, so it needs no
/// locks: that is what the product and campaign lookups need, one per row.
pub struct ByteMap {
    slots: Vec<u32>, // 0 = empty, else (tag << 25) | entry index + 1
    entries: Vec<(u32, u32, u32)>, // key offset, key length, value
    bytes: Vec<u8>,
}

impl ByteMap {
    pub fn new() -> Self {
        ByteMap {
            slots: vec![0u32; 64],
            entries: Vec::new(),
            bytes: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], val: u32) {
        let h = hash_bytes(key);
        let tag = ((h >> 32) & 0x7f) as u32;
        if self.entries.len() + 1 > self.slots.len() / 2 {
            self.slots = vec![0u32; self.slots.len() * 2];
            let mask = (self.slots.len() - 1) as u64;
            for index in 0..self.entries.len() {
                let (off, len, _) = self.entries[index];
                let key = &self.bytes[off as usize..(off + len) as usize];
                let h = hash_bytes(key);
                let tag = ((h >> 32) & 0x7f) as u32;
                let mut at = h & mask;
                while self.slots[at as usize] != 0 {
                    at = (at + 1) & mask;
                }
                self.slots[at as usize] = slot_word(tag, index + 1);
            }
        }
        let mask = (self.slots.len() - 1) as u64;
        let mut at = h & mask;
        loop {
            let slot = self.slots[at as usize];
            if slot == 0 {
                break;
            }
            let (off, len, _) = self.entries[(slot & 0x01ff_ffff) as usize - 1];
            if slot >> 25 == tag && bytes_eq(&self.bytes[off as usize..(off + len) as usize], key) {
                return;
            }
            at = (at + 1) & mask;
        }
        let off = self.bytes.len() as u32;
        self.bytes.extend_from_slice(key);
        self.entries.push((off, key.len() as u32, val));
        self.slots[at as usize] = slot_word(tag, self.entries.len());
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<u32> {
        let h = hash_bytes(key);
        let tag = ((h >> 32) & 0x7f) as u32;
        let mask = (self.slots.len() - 1) as u64;
        let mut at = h & mask;
        loop {
            let slot = self.slots[at as usize];
            if slot == 0 {
                return None;
            }
            // The tag decides first: it skips the entry read for the 127 of
            // 128 occupied slots that belong to some other key.
            if slot >> 25 == tag {
                let (off, len, val) = self.entries[(slot & 0x01ff_ffff) as usize - 1];
                if bytes_eq(&self.bytes[off as usize..(off + len) as usize], key) {
                    return Some(val);
                }
            }
            at = (at + 1) & mask;
        }
    }
}

impl<V: Copy + Hint> Shard<V> {
    fn intern_hashed(&mut self, h: u64, key: &[u8], next_id: &AtomicU32) -> u32 {
        let tag = ((h >> 32) & 0x7f) as u32;
        let mut at = match self.probe(h, tag, key) {
            Found::Present(index) => return self.entries[index].val.hint(),
            Found::Empty(at) => at,
        };
        if self.entries.len() + 1 > self.slots.len() / 2 {
            self.grow();
            at = match self.probe(h, tag, key) {
                Found::Present(index) => return self.entries[index].val.hint(),
                Found::Empty(at) => at,
            };
        }
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let off = self.bytes.len() as u32;
        self.bytes.extend_from_slice(key);
        self.entries.push(Entry {
            off,
            len: key.len() as u32,
            val: V::from_hint(id),
        });
        self.slots[at] = slot_word(tag, self.entries.len());
        id
    }
}

/// Values that can double as interned ids. Only `StrMap::intern` uses this.
/// (Just `u32` in practice.)
pub trait Hint {
    fn hint(&self) -> u32;
    fn from_hint(id: u32) -> Self;
}

impl Hint for u32 {
    #[inline]
    fn hint(&self) -> u32 {
        *self
    }
    #[inline]
    fn from_hint(id: u32) -> Self {
        id
    }
}