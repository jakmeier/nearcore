use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{DBCol, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{TrieCacheMode, TrieNodesCount};
use std::cell::{Cell, RefCell};
use std::io::ErrorKind;

/// Wrapper over LruCache which doesn't hold too large elements.
#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<LruCache<CryptoHash, Arc<[u8]>>>>);

impl TrieCache {
    pub fn new() -> Self {
        Self::with_capacity(TRIE_DEFAULT_SHARD_CACHE_SIZE)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(cap))))
    }

    pub fn get(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.0.lock().expect(POISONED_LOCK_ERR).get(key).cloned()
    }

    pub fn clear(&self) {
        self.0.lock().expect(POISONED_LOCK_ERR).clear()
    }

    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<&Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    if value.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                        guard.put(hash, value.into());
                    }
                } else {
                    guard.pop(&hash);
                }
            } else {
                guard.pop(&hash);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.len()
    }
}

pub trait TrieStorage {
    /// Get bytes of a serialized TrieNode.
    /// # Errors
    /// StorageError if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError>;

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        None
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        None
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        None
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount;
}

/// Records every value read by retrieve_raw_bytes.
/// Used for obtaining state parts (and challenges in the future).
/// TODO (#6316): implement proper nodes counting logic as in TrieCachingStorage
pub struct TrieRecordingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,
    pub(crate) recorded: RefCell<HashMap<CryptoHash, Vec<u8>>>,
}

impl TrieStorage for TrieRecordingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        if let Some(val) = self.recorded.borrow().get(hash) {
            return Ok(val.as_slice().into());
        }
        let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
        let val = self
            .store
            .get(DBCol::State, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?;
        if let Some(val) = val {
            self.recorded.borrow_mut().insert(*hash, val.clone());
            Ok(val.into())
        } else {
            Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
        }
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        Some(self)
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!();
    }
}

/// Storage for validating recorded partial storage.
/// visited_nodes are to validate that partial storage doesn't contain unnecessary nodes.
pub struct TrieMemoryPartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Vec<u8>>,
    pub(crate) visited_nodes: RefCell<HashSet<CryptoHash>>,
}

impl TrieStorage for TrieMemoryPartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.as_slice().into()));
        if result.is_ok() {
            self.visited_nodes.borrow_mut().insert(*hash);
        }
        result
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        Some(self)
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!();
    }
}

/// Default number of cache entries.
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed 50_000 * 4 (number of shards) *
/// TRIE_LIMIT_CACHED_VALUE_SIZE * 2 (number of caches - for regular and view client) = 0.4 GB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const TRIE_DEFAULT_SHARD_CACHE_SIZE: usize = 50000;

#[cfg(feature = "no_cache")]
const TRIE_DEFAULT_SHARD_CACHE_SIZE: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that most of Trie inner nodes are smaller than this - e.g. branches use around 32 * 16 = 512 bytes.
pub(crate) const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 1000;

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,

    /// Caches ever requested items for the shard `shard_uid`. Used to speed up DB operations, presence of any item is
    /// not guaranteed.
    pub(crate) shard_cache: TrieCache,
    /// Caches all items requested in the mode `TrieCacheMode::CachingChunk`. It is created in
    /// `apply_transactions_with_optional_storage_proof` by calling `get_trie_for_shard`. Before we start to apply
    /// txs and receipts in the chunk, it must be empty, and all items placed here must remain until applying
    /// txs/receipts ends. Then cache is removed automatically in `apply_transactions_with_optional_storage_proof` when
    /// `TrieCachingStorage` is removed.
    /// Note that for both caches key is the hash of value, so for the fixed key the value is unique.
    pub(crate) chunk_cache: RefCell<HashMap<CryptoHash, Arc<[u8]>>>,
    pub(crate) cache_mode: Cell<TrieCacheMode>,

    /// Prefetching IO threads will insert fetched data here. This is also used
    /// to mark what is already being fetched, to avoid fetching the same data
    /// multiple times.
    pub(crate) prefetching: Arc<Mutex<HashMap<CryptoHash, PrefetchSlot>>>,

    /// Counts potentially expensive trie node reads which are served from disk in the worst case. Here we count reads
    /// from DB or shard cache.
    pub(crate) db_read_nodes: Cell<u64>,
    /// Counts trie nodes retrieved from the chunk cache.
    pub(crate) mem_read_nodes: Cell<u64>,
}

#[derive(Debug)]
pub(crate) enum PrefetchSlot {
    Pending,
    Done(Arc<[u8]>),
}

pub enum FireAndForgetIoRequest {
    Prefetch(Vec<u8>),
    StopSelf,
}

impl TrieCachingStorage {
    pub fn new(store: Store, shard_cache: TrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage {
            store,
            shard_uid,
            shard_cache,
            cache_mode: Cell::new(TrieCacheMode::CachingShard),
            prefetching: Default::default(),
            chunk_cache: RefCell::new(Default::default()),
            db_read_nodes: Cell::new(0),
            mem_read_nodes: Cell::new(0),
        }
    }

    pub(crate) fn get_shard_uid_and_hash_from_key(
        key: &[u8],
    ) -> Result<(ShardUId, CryptoHash), std::io::Error> {
        if key.len() != 40 {
            return Err(std::io::Error::new(ErrorKind::Other, "Key is always shard_uid + hash"));
        }
        let id = ShardUId::try_from(&key[..8]).unwrap();
        let hash = CryptoHash::try_from(&key[8..]).unwrap();
        Ok((id, hash))
    }

    pub(crate) fn get_key_from_shard_uid_and_hash(
        shard_uid: ShardUId,
        hash: &CryptoHash,
    ) -> [u8; 40] {
        let mut key = [0; 40];
        key[0..8].copy_from_slice(&shard_uid.to_bytes());
        key[8..].copy_from_slice(hash.as_ref());
        key
    }

    fn inc_db_read_nodes(&self) {
        self.db_read_nodes.set(self.db_read_nodes.get() + 1);
    }

    fn inc_mem_read_nodes(&self) {
        self.mem_read_nodes.set(self.mem_read_nodes.get() + 1);
    }

    /// Set cache mode.
    pub fn set_mode(&self, state: TrieCacheMode) {
        self.cache_mode.set(state);
    }

    pub fn start_io_thread(
        &self,
        root: CryptoHash,
    ) -> (std::thread::JoinHandle<()>, Sender<FireAndForgetIoRequest>) {
        // Spawn a new thread that has access to the same db connection and
        // shard cache (for reading what is already cached). Also share the same
        // prefetching space to coordinate in-flight requests.
        // This thread receives requests over an MPSC channel.
        let (tx, rx) = std::sync::mpsc::channel::<FireAndForgetIoRequest>();

        // `Trie` cannot be sent across threads but `TriePrefetchingStorage` can.
        //  Therefore, construct `Trie` in new thread.
        let prefetcher_storage = TriePrefetchingStorage::new(
            self.store.clone(),
            self.shard_uid.clone(),
            self.shard_cache.clone(),
            self.prefetching.clone(),
        );

        let thread_handle = std::thread::spawn(move || {
            let prefetcher_trie = crate::Trie::new(Box::new(prefetcher_storage), root, None);
            while let Ok(req) = rx.recv() {
                match req {
                    FireAndForgetIoRequest::Prefetch(storage_key) => {
                        if let Ok(Some(_value)) = prefetcher_trie.get(&storage_key) {
                            near_o11y::io_trace!(count: "prefetch_success");
                        }
                    }
                    FireAndForgetIoRequest::StopSelf => return,
                }
            }
        });
        (thread_handle, tx)
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from chunk cache containing nodes with cheaper access. We can do it for any `TrieCacheMode`,
        // because we charge for reading nodes only when `CachingChunk` mode is enabled anyway.
        if let Some(val) = self.chunk_cache.borrow_mut().get(hash) {
            self.inc_mem_read_nodes();
            return Ok(val.clone());
        }

        // Try to get value from shard cache containing most recently touched nodes.
        let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        let val = match guard.get(hash) {
            Some(val) => {
                near_o11y::io_trace!(count: "shard_cache_hit");
                val.clone()
            }
            None => {
                std::mem::drop(guard);
                near_o11y::io_trace!(count: "shard_cache_miss");
                // If data is already being prefetched, wait for that instead of sending a new request.
                let val: Arc<[u8]> = if let Some(val) =
                    wait_for_prefetched(&self.prefetching, hash.clone())
                {
                    val
                } else {
                    // If value is not present in cache, get it from the storage.
                    let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                    let val = self
                        .store
                        .get(DBCol::State, key.as_ref())
                        .map_err(|_| StorageError::StorageInternalError)?
                        .ok_or_else(|| {
                            StorageError::StorageInconsistentState("Trie node missing".to_string())
                        })?;
                    val.into()
                };

                // Insert value to shard cache, if its size is small enough.
                // It is fine to have a size limit for shard cache and **not** have a limit for chunk cache, because key
                // is always a value hash, so for each key there could be only one value, and it is impossible to have
                // **different** values for the given key in shard and chunk caches.
                if val.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                    let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
                    guard.put(*hash, val.clone());
                } else {
                    near_o11y::io_trace!(count: "shard_cache_too_large");
                }

                val
            }
        };

        // Because node is not present in chunk cache, increment the nodes counter and optionally insert it into the
        // chunk cache.
        // Note that we don't have a size limit for values in the chunk cache. There are two reasons:
        // - for nodes, value size is an implementation detail. If we change internal representation of a node (e.g.
        // change `memory_usage` field from `RawTrieNodeWithSize`), this would have to be a protocol upgrade.
        // - total size of all values is limited by the runtime fees. More thoroughly:
        // - - number of nodes is limited by receipt gas limit / touching trie node fee ~= 500 Tgas / 16 Ggas = 31_250;
        // - - size of trie keys and values is limited by receipt gas limit / lowest per byte fee
        // (`storage_read_value_byte`) ~= (500 * 10**12 / 5611005) / 2**20 ~= 85 MB.
        // All values are given as of 16/03/2022. We may consider more precise limit for the chunk cache as well.
        self.inc_db_read_nodes();
        if let TrieCacheMode::CachingChunk = self.cache_mode.borrow().get() {
            self.chunk_cache.borrow_mut().insert(*hash, val.clone());
        };

        Ok(val)
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        TrieNodesCount { db_reads: self.db_read_nodes.get(), mem_reads: self.mem_read_nodes.get() }
    }
}

/// Storage used by I/O threads to prefetch data.
///
/// Is always linked to a parent `TrieCachingStorage`. One  caching storage can
/// produce many I/O threads and each will have its own prefetching storage.
/// However, the underlying shard cache and prefetching map is shared among all
/// instances, including the parent.
pub struct TriePrefetchingStorage {
    /// Store is shared with parent `TrieCachingStorage`.
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,
    /// Shard cache is shared with parent `TrieCachingStorage`. But the
    /// pre-fetcher uses this in read-only mode to avoid premature evictions.
    pub(crate) shard_cache: TrieCache,
    /// Shared with parent `TrieCachingStorage`.
    ///
    /// Before starting a pre-fetch, a slot is reserved for it. Once the data is
    /// here, it will be put in that slot. The parent `TrieCachingStorage` needs
    /// to take it out and move it to the shard cache.
    pub(crate) prefetching: Arc<Mutex<HashMap<CryptoHash, PrefetchSlot>>>,
}

impl TriePrefetchingStorage {
    fn new(
        store: Store,
        shard_uid: ShardUId,
        shard_cache: TrieCache,
        prefetching: Arc<Mutex<HashMap<CryptoHash, PrefetchSlot>>>,
    ) -> Self {
        Self { store, shard_uid, shard_cache, prefetching }
    }
}

impl TrieStorage for TriePrefetchingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from shard cache containing most recently touched nodes.
        let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        let val = match guard.get(hash) {
            Some(val) => val.clone(),
            None => {
                // If data is already being prefetched, wait for that instead of sending a new request.
                let mut prefetch_guard = self.prefetching.lock().expect(POISONED_LOCK_ERR);

                if prefetch_guard.contains_key(hash) {
                    std::mem::drop(guard);
                    std::mem::drop(prefetch_guard);
                    wait_for_prefetched(&self.prefetching, hash.clone()).unwrap_or_else(|| {
                        self.shard_cache
                            .0
                            .lock()
                            .expect(POISONED_LOCK_ERR)
                            .get(hash)
                            .expect("must be prefetched by now")
                            .clone()
                    })
                } else {
                    prefetch_guard.insert(hash.clone(), PrefetchSlot::Pending);
                    // It's important that the chunk_cache guard is held until
                    // after inserting `PrefetchSlot::Pending`, to avoid
                    // multiple I/O threads fetching the same data.
                    std::mem::drop(guard);
                    std::mem::drop(prefetch_guard);
                    let key =
                        TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                    let val: Arc<[u8]> = self
                        .store
                        .get(DBCol::State, key.as_ref())
                        .map_err(|_| StorageError::StorageInternalError)?
                        .ok_or_else(|| {
                            StorageError::StorageInconsistentState("Trie node missing".to_string())
                        })?
                        .into();

                    let pending = self
                        .prefetching
                        .lock()
                        .expect(POISONED_LOCK_ERR)
                        .insert(hash.clone(), PrefetchSlot::Done(val.clone()));
                    // TODO: Remove panic / make it debug only
                    match pending {
                        Some(PrefetchSlot::Pending) => { /* OK */ }
                        _ => panic!("Slot should be pending"),
                    }
                    val
                }
            }
        };

        Ok(val)
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!()
    }
}

fn check_prefetched(
    prefetching: &Arc<Mutex<HashMap<CryptoHash, PrefetchSlot>>>,
    key: CryptoHash,
) -> Option<PrefetchSlot> {
    match prefetching.lock().expect(POISONED_LOCK_ERR).entry(key) {
        Entry::Occupied(entry) => match entry.get() {
            PrefetchSlot::Pending => Some(PrefetchSlot::Pending),
            PrefetchSlot::Done(_) => {
                let prefetch_slot = entry.remove();
                near_o11y::io_trace!(count: "prefetch_hit");
                Some(prefetch_slot)
            }
        },
        Entry::Vacant(_) => None,
    }
}

fn wait_for_prefetched(
    prefetching: &Arc<Mutex<HashMap<CryptoHash, PrefetchSlot>>>,
    key: CryptoHash,
) -> Option<Arc<[u8]>> {
    loop {
        match check_prefetched(prefetching, key) {
            Some(PrefetchSlot::Done(value)) => {
                near_o11y::io_trace!(count: "prefetch_hit");
                return Some(value);
            }
            Some(PrefetchSlot::Pending) => {
                near_o11y::io_trace!(count: "prefetch_pending");
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            None => return None,
        }
    }
}
