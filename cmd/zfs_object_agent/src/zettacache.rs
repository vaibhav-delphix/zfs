use crate::base_types::*;
use crate::block_access::*;
use crate::block_allocator::BlockAllocator;
use crate::block_allocator::BlockAllocatorPhys;
use crate::block_based_log::*;
use crate::extent_allocator::ExtentAllocator;
use crate::extent_allocator::ExtentAllocatorPhys;
use crate::index::*;
use anyhow::Result;
use futures::future;
use futures::stream::*;
use futures::Future;
use log::*;
use metered::common::*;
use metered::hdr_histogram::AtomicHdrHistogram;
use metered::metered;
use metered::time_source::StdInstantMicros;
use more_asserts::*;
use serde::{Deserialize, Serialize};
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Semaphore;

const SUPERBLOCK_SIZE: usize = 4 * 1024;
//const SUPERBLOCK_MAGIC: u64 = 0x2e11acac4e;
const DEFAULT_CHECKPOINT_RING_BUFFER_SIZE: usize = 1024 * 1024;
const DEFAULT_SLAB_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_METADATA_SIZE_PCT: f64 = 5.0; // Can lower this to test forced eviction.
const MAX_PENDING_CHANGES: usize = 50_000; // XXX should be based on RAM usage, ~tens of millions at least
const TARGET_CACHE_SIZE_PCT: u64 = 10;

#[derive(Serialize, Deserialize, Debug)]
struct ZettaSuperBlockPhys {
    checkpoint_ring_buffer_size: u32,
    slab_size: u32,
    last_checkpoint_id: CheckpointId,
    last_checkpoint_extent: Extent,
    // XXX put sector size in here too and verify it matches what the disk says now?
    // XXX put disk size in here so we can detect expansion?
}

impl ZettaSuperBlockPhys {
    // XXX when we have multiple disks, will this be stored on a specific one?  Or copied on all of them?
    async fn read(block_access: &BlockAccess) -> Result<ZettaSuperBlockPhys> {
        let raw = block_access
            .read_raw(Extent {
                location: DiskLocation { offset: 0 },
                size: SUPERBLOCK_SIZE,
            })
            .await;
        let (this, _): (Self, usize) = block_access.chunk_from_raw(&raw)?;
        debug!("got {:#?}", this);
        Ok(this)
    }

    async fn write(&self, block_access: &BlockAccess) {
        debug!("writing {:#?}", self);
        let raw = block_access.chunk_to_raw(EncodeType::Json, self);
        block_access
            .write_raw(DiskLocation { offset: 0 }, raw)
            .await;
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ZettaCheckpointPhys {
    generation: CheckpointId,
    extent_allocator: ExtentAllocatorPhys,
    block_allocator: BlockAllocatorPhys,
    //last_valid_data_offset: u64, // XXX move to BlockAllocatorPhys
    last_atime: Atime,
    index: ZettaCacheIndexPhys,
    chunk_summary: BlockBasedLogPhys,
    operation_log: BlockBasedLogPhys,
}

impl ZettaCheckpointPhys {
    async fn read(block_access: &BlockAccess, extent: Extent) -> ZettaCheckpointPhys {
        let raw = block_access.read_raw(extent).await;
        let (this, _): (Self, usize) = block_access.chunk_from_raw(&raw).unwrap();
        debug!("got {:#?}", this);
        this
    }

    /*
    fn all_logs(&self) -> Vec<&BlockBasedLogPhys> {
        vec![&self.index, &self.chunk_summary, &self.operation_log]
    }
    */
}

#[derive(Debug, Clone, Copy)]
enum PendingChange {
    Insert(IndexValue),
    UpdateAtime(IndexValue),
    Remove(),
    RemoveThenInsert(IndexValue),
}

#[derive(Clone)]
pub struct ZettaCache {
    // lock ordering: index first then state
    index: Arc<tokio::sync::RwLock<ZettaCacheIndex>>,
    // XXX may need to break up this big lock.  At least we aren't holding it while doing i/o
    state: Arc<tokio::sync::Mutex<ZettaCacheState>>,
    metrics: Arc<ZettaCacheMetrics>,
}

struct NonSendMutexGuard<'a, T> {
    inner: tokio::sync::MutexGuard<'a, T>,
    // force this to not be Send
    _marker: PhantomData<*const ()>,
}

impl<'a, T> std::ops::Deref for NonSendMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> std::ops::DerefMut for NonSendMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

async fn lock_non_send<T>(lock: &tokio::sync::Mutex<T>) -> NonSendMutexGuard<'_, T> {
    NonSendMutexGuard {
        inner: lock.lock().await,
        _marker: PhantomData,
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct ChunkSummaryEntry {
    offset: LogOffset,
    first_key: IndexKey,
}
impl OnDisk for ChunkSummaryEntry {}
impl BlockBasedLogEntry for ChunkSummaryEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum OperationLogEntry {
    Insert((IndexKey, IndexValue)),
    Remove((IndexKey, IndexValue)),
}
impl OnDisk for OperationLogEntry {}
impl BlockBasedLogEntry for OperationLogEntry {}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AtimeHistogramPhys {
    histogram: Vec<u64>,
    start: Atime,
}

impl AtimeHistogramPhys {
    pub fn new(start: Atime) -> AtimeHistogramPhys {
        AtimeHistogramPhys {
            histogram: Default::default(),
            start,
        }
    }

    pub fn get_start(&mut self) -> Atime {
        self.start
    }

    pub fn atime_for_target_size(&mut self, target_size: u64) -> Atime {
        info!(
            "histogram starts at {:?} and has {} entries",
            self.start,
            self.histogram.len()
        );
        let mut remaining = target_size;
        let mut target_index = self.start.0;
        for (index, count) in self.histogram.iter().enumerate().rev() {
            if remaining <= *count {
                trace!("final include of {} for target at bucket {}", count, index);
                target_index += index as u64;
                break;
            }
            trace!("including {} in target at bucket {}", count, index);
            remaining -= count;
        }
        Atime(target_index)
    }

    pub fn insert(&mut self, value: IndexValue) {
        assert_ge!(value.atime, self.start);
        let index = value.atime - self.start;
        if index >= self.histogram.len() {
            self.histogram.resize(index + 1, 0);
        }
        self.histogram[index] += value.size as u64;
    }

    pub fn remove(&mut self, value: IndexValue) {
        assert_ge!(value.atime, self.start);
        let index = value.atime - self.start;
        self.histogram[index] -= value.size as u64;
    }

    pub fn clear(&mut self) {
        self.histogram.clear();
    }
}

struct ZettaCacheState {
    block_access: Arc<BlockAccess>,
    super_phys: ZettaSuperBlockPhys,
    block_allocator: BlockAllocator,
    //last_valid_data_offset: u64, // XXX move to a BlockAllocator struct
    pending_changes: BTreeMap<IndexKey, PendingChange>,
    // XXX Given that we have to lock the entire State to do anything, we might
    // get away with this being a Rc?  And the ExtentAllocator doesn't really
    // need the lock inside it.  But hopefully we split up the big State lock
    // and then this is useful.  Same goes for block_access.
    extent_allocator: Arc<ExtentAllocator>,
    atime_histogram: AtimeHistogramPhys, // includes pending_changes, including AtimeUpdate which is not logged
    chunk_summary: BlockBasedLog<ChunkSummaryEntry>,
    // XXX move this to its own file/struct with methods to load, etc?
    operation_log: BlockBasedLog<OperationLogEntry>,
    // When i/o completes, the value will be sent, and the entry can be removed
    // from the tree.  These are needed to prevent the ExtentAllocator from
    // overwriting them while i/o is in flight, and to ensure that writes
    // complete before we complete the next checkpoint.
    outstanding_reads: BTreeMap<IndexValue, Arc<Semaphore>>,
    outstanding_writes: BTreeMap<IndexValue, Arc<Semaphore>>,
    atime: Atime,
}

#[metered(registry=ZettaCacheMetrics)]
impl ZettaCache {
    pub async fn create(path: &str) {
        let block_access = BlockAccess::new(path).await;
        let metadata_start = SUPERBLOCK_SIZE + DEFAULT_CHECKPOINT_RING_BUFFER_SIZE;
        let data_start = block_access.round_up_to_sector(
            metadata_start as u64
                + (DEFAULT_METADATA_SIZE_PCT / 100.0 * block_access.size() as f64) as u64,
        );
        let checkpoint = ZettaCheckpointPhys {
            generation: CheckpointId(0),
            extent_allocator: ExtentAllocatorPhys {
                first_valid_offset: metadata_start as u64,
                last_valid_offset: data_start as u64,
            },
            index: Default::default(),
            chunk_summary: Default::default(),
            operation_log: Default::default(),
            last_atime: Atime(0),
            block_allocator: BlockAllocatorPhys::new(
                data_start as u64,
                block_access.size() - data_start,
            ),
        };
        let raw = block_access.chunk_to_raw(EncodeType::Json, &checkpoint);
        assert_le!(raw.len(), DEFAULT_CHECKPOINT_RING_BUFFER_SIZE);
        let checkpoint_size = raw.len();
        block_access
            .write_raw(
                DiskLocation {
                    offset: SUPERBLOCK_SIZE as u64,
                },
                raw,
            )
            .await;
        let phys = ZettaSuperBlockPhys {
            checkpoint_ring_buffer_size: DEFAULT_CHECKPOINT_RING_BUFFER_SIZE as u32,
            slab_size: DEFAULT_SLAB_SIZE as u32,
            last_checkpoint_extent: Extent {
                location: DiskLocation {
                    offset: SUPERBLOCK_SIZE as u64,
                },
                size: checkpoint_size,
            },
            last_checkpoint_id: CheckpointId(0),
        };
        phys.write(&block_access).await;
    }

    pub async fn open(path: &str) -> ZettaCache {
        let block_access = Arc::new(BlockAccess::new(path).await);

        // if superblock not present, create new cache
        // XXX need a real mechanism for creating/managing the cache devices
        let phys = match ZettaSuperBlockPhys::read(&block_access).await {
            Ok(phys) => phys,
            Err(_) => {
                Self::create(path).await;
                ZettaSuperBlockPhys::read(&block_access).await.unwrap()
            }
        };

        let checkpoint =
            ZettaCheckpointPhys::read(&block_access, phys.last_checkpoint_extent).await;

        assert_eq!(checkpoint.generation, phys.last_checkpoint_id);

        let metadata_start = SUPERBLOCK_SIZE + phys.checkpoint_ring_buffer_size as usize;
        // XXX pass in the metadata_start to ExtentAllocator::open, rather than
        // having this represented twice in the on-disk format?
        assert_eq!(
            metadata_start as u64,
            checkpoint.extent_allocator.first_valid_offset
        );
        let extent_allocator = Arc::new(ExtentAllocator::open(&checkpoint.extent_allocator));

        let operation_log = BlockBasedLog::open(
            block_access.clone(),
            extent_allocator.clone(),
            checkpoint.operation_log,
        );

        let index = ZettaCacheIndex::open(
            block_access.clone(),
            extent_allocator.clone(),
            checkpoint.index,
        )
        .await;

        // XXX would be nice to periodically load the operation_log and verify
        // that our state's pending_changes & atime_histogram match it
        let (pending_changes, atime_histogram) =
            Self::load_operation_log(&operation_log, &index.atime_histogram).await;
        debug!("atime_histogram: {:#?}", atime_histogram);

        let state = ZettaCacheState {
            block_access: block_access.clone(),
            pending_changes,
            atime_histogram,
            chunk_summary: BlockBasedLog::open(
                block_access.clone(),
                extent_allocator.clone(),
                checkpoint.chunk_summary,
            ),
            operation_log,
            super_phys: phys,
            outstanding_reads: BTreeMap::new(),
            outstanding_writes: BTreeMap::new(),
            atime: checkpoint.last_atime,
            block_allocator: BlockAllocator::open(
                block_access.clone(),
                extent_allocator.clone(),
                checkpoint.block_allocator,
            )
            .await,
            extent_allocator,
        };

        let this = ZettaCache {
            index: Arc::new(tokio::sync::RwLock::new(index)),
            state: Arc::new(tokio::sync::Mutex::new(state)),
            metrics: Default::default(),
        };

        let my_cache = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut index = my_cache.index.write().await;
                my_cache
                    .state
                    .lock()
                    .await
                    .flush_checkpoint(&mut index)
                    .await;
            }
        });

        let my_cache = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                debug!("metrics: {:#?}", my_cache.metrics);
                my_cache.state.lock().await.block_access.dump_metrics();
            }
        });

        let my_cache = this.clone();
        tokio::spawn(async move {
            // XXX maybe we should bump the atime after a set number of
            // accesses, so each histogram bucket starts with the same count.
            // We could then add an auxiliary structure saying what wall clock
            // time each atime value corresponds to.
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let mut state = my_cache.state.lock().await;
                state.atime = state.atime.next();
                drop(state);
            }
        });

        this
    }

    async fn load_operation_log(
        operation_log: &BlockBasedLog<OperationLogEntry>,
        index_atime_histogram: &AtimeHistogramPhys,
    ) -> (BTreeMap<IndexKey, PendingChange>, AtimeHistogramPhys) {
        let begin = Instant::now();
        let mut num_insert_entries: u64 = 0;
        let mut num_remove_entries: u64 = 0;
        let mut pending_changes = BTreeMap::new();
        let mut atime_histogram = index_atime_histogram.clone();
        operation_log
            .iter()
            .for_each(|entry| {
                match entry {
                    OperationLogEntry::Insert((key, value)) => {
                        match pending_changes.entry(key) {
                            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                                PendingChange::Remove() => {
                                    trace!("insert with existing removal; changing to RemoveThenInsert: {:?} {:?}", key, value);
                                    oe.insert(PendingChange::RemoveThenInsert(value));
                                }
                                pc  => {
                                    panic!(
                                        "Inserting {:?} {:?} into already existing entry {:?}",
                                        key,
                                        value,
                                        pc,
                                    );
                                }
                            },
                            btree_map::Entry::Vacant(ve) => {
                                trace!("insert {:?} {:?}", key, value);
                                ve.insert(PendingChange::Insert(value));
                            }
                        }
                        num_insert_entries += 1;
                        atime_histogram.insert(value);
                    }
                    OperationLogEntry::Remove((key, value)) => {
                        match pending_changes.entry(key) {
                            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                                PendingChange::Insert(value) => {
                                    trace!("remove with existing insert; clearing {:?} {:?}", key, value);
                                    oe.remove();
                                }
                                PendingChange::RemoveThenInsert(value) => {
                                    trace!("remove with existing removetheninsert; changing to remove: {:?} {:?}", key, value);
                                    oe.insert(PendingChange::Remove());
                                }
                                pc  => {
                                    panic!(
                                        "Removing {:?} from already existing entry {:?}",
                                        key,
                                        pc,
                                    );
                                }
                            },
                            btree_map::Entry::Vacant(ve) => {
                                trace!("remove {:?}", key);
                                ve.insert(PendingChange::Remove());
                            }
                        }
                        num_remove_entries += 1;
                        atime_histogram.remove(value);
                    }
                };
                future::ready(())
            })
            .await;
        info!(
            "loaded operation_log from {} inserts and {} removes into {} pending_changes in {}ms",
            num_insert_entries,
            num_remove_entries,
            pending_changes.len(),
            begin.elapsed().as_millis()
        );
        (pending_changes, atime_histogram)
    }

    #[measure(HitCount)]
    fn cache_miss_without_index_read(&self, key: &IndexKey) {
        trace!("cache miss without reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_miss_after_index_read(&self, key: &IndexKey) {
        trace!("cache miss after reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_hit_without_index_read(&self, key: &IndexKey) {
        trace!("cache hit without reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_hit_after_index_read(&self, key: &IndexKey) {
        trace!("cache hit after reading index for {:?}", key);
    }

    async fn lock_state_non_async(&self) -> NonSendMutexGuard<'_, ZettaCacheState> {
        lock_non_send(&self.state).await
    }

    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn lookup(&self, guid: PoolGuid, block: BlockId) -> Option<Vec<u8>> {
        // We want to hold the index lock over the whole operation so that the index can't change after we get the value from it.
        // Lock ordering requres that we lock the index before locking the state.
        let index = self.index.read().await;
        let key = IndexKey { guid, block };
        let read_data_fut_opt = {
            // We don't want to hold the state lock while reading from disk.  We
            // use lock_state_non_async() to ensure that we can't hold it across
            // .await.
            let mut state = self.lock_state_non_async().await;
            match state.pending_changes.get(&key).copied() {
                Some(pc) => {
                    match pc {
                        PendingChange::Insert(value) => Some(state.lookup(key, value)),
                        PendingChange::RemoveThenInsert(value) => Some(state.lookup(key, value)),
                        PendingChange::UpdateAtime(value) => Some(state.lookup(key, value)),
                        PendingChange::Remove() => {
                            // Pending change says this has been removed
                            Some(data_reader_none())
                        }
                    }
                }
                None => {
                    // No pending change; need to look in index
                    None
                }
            }
        };

        if let Some(read_data_fut) = read_data_fut_opt {
            // pending state tells us what to do
            match read_data_fut.await {
                Some(vec) => {
                    self.cache_hit_without_index_read(&key);
                    return Some(vec);
                }
                None => {
                    self.cache_miss_without_index_read(&key);
                    return None;
                }
            }
        }

        trace!("lookup has no pending_change; checking index for {:?}", key);
        // read value from on-disk index
        match index.log.lookup_by_key(&key, |entry| entry.key).await {
            None => {
                // key not in index
                self.cache_miss_after_index_read(&key);
                None
            }
            Some(entry) => {
                // read data from location indicated by index

                // We don't want to hold the state lock while reading from disk.
                // We use lock_state_non_async() to ensure that we can't hold it
                // across .await.
                let read_data_fut = self
                    .lock_state_non_async()
                    .await
                    .lookup_with_value_from_index(key, Some(entry.value));
                match read_data_fut.await {
                    Some(vec) => {
                        self.cache_hit_after_index_read(&key);
                        Some(vec)
                    }
                    None => {
                        self.cache_miss_after_index_read(&key);
                        None
                    }
                }
            }
        }
    }

    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn insert(&self, guid: PoolGuid, block: BlockId, buf: Vec<u8>) {
        let mut state = self.state.lock().await;
        state.insert(guid, block, buf);
    }
}

type DataReader = Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send>>;

fn data_reader_none() -> DataReader {
    Box::pin(async move { None })
}

impl ZettaCacheState {
    fn lookup_with_value_from_index(
        &mut self,
        key: IndexKey,
        value_from_index_opt: Option<IndexValue>,
    ) -> DataReader {
        // Note: we're here because there was no PendingChange for this key, but
        // since we dropped the lock, a PendingChange may have been inserted
        // since then.  So we need to check for a PendingChange before using the
        // value from the index.
        let value = match self.pending_changes.get(&key) {
            Some(PendingChange::Insert(value_ref)) => *value_ref,
            Some(PendingChange::RemoveThenInsert(value_ref)) => *value_ref,
            Some(PendingChange::UpdateAtime(value_ref)) => *value_ref,
            Some(PendingChange::Remove()) => return data_reader_none(),
            None => {
                // use value from on-disk index
                match value_from_index_opt {
                    Some(value) => value,
                    None => return data_reader_none(),
                }
            }
        };

        self.lookup(key, value)
    }

    fn lookup(&mut self, key: IndexKey, mut value: IndexValue) -> DataReader {
        if value.location.offset < self.extent_allocator.get_phys().last_valid_offset {
            // The metadata overwrote this data, so it's no longer in the cache.
            // Remove from index and return None.
            trace!(
                "{:?} at {:?} was overwritten by metadata allocator; removing from cache",
                key,
                value
            );
            // Note: we could pass in the (mutable) pending_change reference,
            // which would let evict_block() avoid looking it up again.  But
            // this is not a common code path, and this interface seems cleaner.
            self.evict_block(key, value);
            return data_reader_none();
        }
        trace!("cache hit: reading {:?} from {:?}", key, value);
        if value.atime != self.atime {
            self.atime_histogram.remove(value);
            value.atime = self.atime;
            self.atime_histogram.insert(value);
        }
        // XXX looking up again.  But can't pass in both &mut self and &mut PendingChange
        let pc = self.pending_changes.get_mut(&key);
        match pc {
            Some(PendingChange::Insert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::RemoveThenInsert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::UpdateAtime(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::Remove()) => panic!("invalid state"),
            None => {
                // only in Index, not pending_changes
                trace!(
                    "adding UpdateAtime to pending_changes {:?} {:?}",
                    key,
                    value
                );
                // XXX would be nice to have saved the btreemap::Entry so we
                // don't have to traverse the tree again.
                self.pending_changes
                    .insert(key, PendingChange::UpdateAtime(value));
            }
        }

        // If there's a write to this location in progress, we will need to wait for it to complete before reading.
        // Since we won't be able to remove the entry from outstanding_writes after we wait, we just get the semaphore.
        let write_sem_opt = self
            .outstanding_writes
            .get_mut(&value)
            .map(|arc| arc.clone());

        let sem = Arc::new(Semaphore::new(0));
        let sem2 = sem.clone();
        self.outstanding_reads.insert(value, sem);
        let block_access = self.block_access.clone();
        Box::pin(async move {
            if let Some(write_sem) = write_sem_opt {
                trace!("{:?} at {:?}: waiting for outstanding write", key, value);
                let _permit = write_sem.acquire().await.unwrap();
            }

            let vec = block_access
                .read_raw(Extent {
                    location: value.location,
                    size: value.size,
                })
                .await;
            sem2.add_permits(1);
            // XXX we can easily handle an io error here by returning None
            Some(vec)
        })
    }

    fn evict_block(&mut self, key: IndexKey, value: IndexValue) {
        match self.pending_changes.get_mut(&key) {
            Some(PendingChange::Insert(value_ref)) => {
                // The operation_log has an Insert for this key, and the key
                // is not in the Index.  We don't need a
                // PendingChange::Removal since there's nothing to remove
                // from the index.
                assert_eq!(*value_ref, value);
                trace!("removing Insert from pending_changes {:?} {:?}", key, value);
                self.pending_changes.remove(&key);
            }
            Some(PendingChange::RemoveThenInsert(value_ref)) => {
                // The operation_log has a Remove, and then an Insert for
                // this key, so the key is in the Index.  We need a
                // PendingChange::Remove so that the Index entry won't be
                // found.
                assert_eq!(*value_ref, value);
                trace!(
                    "changing RemoveThenInsert to Remove in pending_changes {:?} {:?}",
                    key,
                    value,
                );
                self.pending_changes.insert(key, PendingChange::Remove());
            }
            Some(PendingChange::UpdateAtime(value_ref)) => {
                // It's just an atime update, so the operation_log doesn't
                // have an Insert for this key, but the key is in the
                // Index.
                assert_eq!(*value_ref, value);
                trace!(
                    "changing UpdateAtime to Remove in pending_changes {:?} {:?}",
                    key,
                    value,
                );
                self.pending_changes.insert(key, PendingChange::Remove());
            }
            Some(PendingChange::Remove()) => {
                panic!("invalid state");
            }
            None => {
                // only in Index, not pending_changes
                trace!("adding Remove to pending_changes {:?}", key);
                self.pending_changes.insert(key, PendingChange::Remove());
            }
        }
        trace!("adding Remove to operation_log {:?}", key);
        self.atime_histogram.remove(value);
        self.operation_log
            .append(OperationLogEntry::Remove((key, value)));
    }

    /// Insert this block to the cache, if space and performance parameters
    /// allow.  It may be a recent cache miss, or a recently-written block.
    fn insert(&mut self, guid: PoolGuid, block: BlockId, buf: Vec<u8>) {
        let buf_size = buf.len();
        let aligned_size = self.block_access.round_up_to_sector(buf.len());

        let aligned_buf = if buf_size == aligned_size {
            buf
        } else {
            // pad to sector size
            let mut tail: Vec<u8> = Vec::new();
            tail.resize(aligned_size - buf_size, 0);
            // XXX copying data around; have caller pass in larger buffer?  or
            // at least let us pass the unaligned buf to write_raw() which
            // already has to copy it around to get the pointer aligned.
            [buf, tail].concat()
        };
        let location_opt = self.allocate_block(aligned_buf.len());
        if location_opt.is_none() {
            return;
        }
        let location = location_opt.unwrap();

        // XXX if this is past the last block of the main index, we can write it
        // there (and location_dirty:false) instead of logging it

        let key = IndexKey { guid, block };
        let value = IndexValue {
            atime: self.atime,
            location,
            size: buf_size,
        };

        // XXX we'd like to assert that this is not already in the index
        // (otherwise we would need to use a PendingChange::RemoveThenInsert).
        // However, this is not an async fn so we can't do the read here.  We
        // could spawn a new task, but currently reading the index requires the
        // big lock.

        match self.pending_changes.entry(key) {
            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                PendingChange::Remove() => {
                    trace!(
                        "adding RemoveThenInsert to pending_changes {:?} {:?}",
                        key,
                        value
                    );
                    oe.insert(PendingChange::RemoveThenInsert(value));
                }
                _ => {
                    // Already in cache; ignore this insertion request?  Or panic?
                    todo!();
                }
            },
            btree_map::Entry::Vacant(ve) => {
                trace!("adding Insert to pending_changes {:?} {:?}", key, value);
                ve.insert(PendingChange::Insert(value));
            }
        }
        self.atime_histogram.insert(value);

        trace!("adding Insert to operation_log {:?} {:?}", key, value);
        self.operation_log
            .append(OperationLogEntry::Insert((key, value)));

        let sem = Arc::new(Semaphore::new(0));
        let sem2 = sem.clone();
        let block_access = self.block_access.clone();
        tokio::spawn(async move {
            block_access.write_raw(location, aligned_buf).await;
            sem2.add_permits(1);
        });
        // note: we don't need to insert before initiating the write, because we
        // have exclusive access to the State, so nobody can see the
        // outstanding_writes until we are done
        self.outstanding_writes.insert(value, sem);
    }

    /// returns offset, or None if there's no space
    fn allocate_block(&mut self, size: usize) -> Option<DiskLocation> {
        self.block_allocator
            .allocate(size as u64)
            .map(|extent| extent.location)
    }

    async fn flush_checkpoint(&mut self, index: &mut ZettaCacheIndex) {
        debug!(
            "flushing checkpoint {:?}",
            self.super_phys.last_checkpoint_id.next()
        );

        let begin = Instant::now();

        // Wait for all outstanding reads, so that if the ExtentAllocator needs
        // to overwrite some blocks, there aren't any outstanding i/os to that
        // region.
        // XXX It would be better to only wait for the reads that are in the
        // region that we're overwriting.  But it will be tricky to do the
        // waiting down in the ExtentAllocator.  If we get that working, we'll
        // still need to clean up the outstanding_reads entries that have
        // completed, at some point.  Even as-is, letting them accumulate for a
        // whole checkpoint might not be great.  We might want a "cleaner" to
        // run every second and remove completed entries.  Or have the read task
        // lock the outstanding_reads and remove itself (which might perform
        // worse due to contention on the global lock).
        for sem in self.outstanding_reads.values_mut() {
            let _permit = sem.acquire().await.unwrap();
        }
        self.outstanding_reads.clear();

        // Wait for all outstanding writes, for the same reason as reads, and
        // also so that if we crash, the blocks referenced by the
        // index/operation_log will actually have the correct contents.
        for sem in self.outstanding_writes.values_mut() {
            let _permit = sem.acquire().await.unwrap();
        }
        self.outstanding_writes.clear();

        trace!(
            "{:?} pending changes at checkpoint",
            self.pending_changes.len()
        );
        if self.pending_changes.len() > MAX_PENDING_CHANGES {
            index.flush().await;
            let new_index = self.merge_pending_changes(index).await;
            index.clear();
            *index = new_index;
        }

        let (index_phys, chunk_summary_phys, operation_log_phys) = future::join3(
            index.flush(),
            self.chunk_summary.flush(),
            self.operation_log.flush(),
        )
        .await;

        let checkpoint = ZettaCheckpointPhys {
            generation: self.super_phys.last_checkpoint_id.next(),
            extent_allocator: self.extent_allocator.get_phys(),
            index: index_phys,
            chunk_summary: chunk_summary_phys,
            operation_log: operation_log_phys,
            last_atime: self.atime,
            block_allocator: self.block_allocator.flush().await,
        };

        let mut checkpoint_location = self.super_phys.last_checkpoint_extent.location
            + self.super_phys.last_checkpoint_extent.size;

        let raw = self
            .block_access
            .chunk_to_raw(EncodeType::Json, &checkpoint);
        if raw.len()
            > (checkpoint.extent_allocator.first_valid_offset - checkpoint_location.offset) as usize
        {
            // Out of space; go back to the beginning of the checkpoint space.
            checkpoint_location.offset = SUPERBLOCK_SIZE as u64;
            assert_le!(
                raw.len(),
                (self.super_phys.last_checkpoint_extent.location.offset
                    - checkpoint_location.offset) as usize,
            );
            // XXX The above assertion could fail if there isn't enough
            // checkpoint space for 3 checkpoints (the existing one that
            // we're writing before, the one we're writing, and the space
            // after the existing one that we aren't using).  Note that we
            // could in theory reduce this to 2 checkpoints if we allowed a
            // single checkpoint to wrap around (part of it at the end and
            // then part at the beginning of the space).
        }
        debug!("writing to {:?}: {:#?}", checkpoint_location, checkpoint);

        self.super_phys.last_checkpoint_extent = Extent {
            location: checkpoint_location,
            size: raw.len(),
        };
        self.block_access.write_raw(checkpoint_location, raw).await;

        self.super_phys.last_checkpoint_id = self.super_phys.last_checkpoint_id.next();
        self.super_phys.write(&self.block_access).await;

        self.extent_allocator.checkpoint_done();

        debug!(
            "completed checkpoint {:?} in {}ms",
            self.super_phys.last_checkpoint_id,
            begin.elapsed().as_millis()
        );
    }

    async fn merge_pending_changes(&mut self, old_index: &ZettaCacheIndex) -> ZettaCacheIndex {
        // Helper function to determine if an entry being merged should be added to the new index or,
        // if due to eviction, should be freed.
        fn add_to_index_or_list(
            index: &mut ZettaCacheIndex,
            list: &mut Vec<IndexValue>,
            entry: IndexEntry,
        ) {
            if entry.value.atime >= index.get_histogram_start() {
                index.append(entry);
            } else {
                list.push(entry.value);
            }
        }

        let begin = Instant::now();
        // XXX when we are continually merging, over multiple checkpoints, we
        // will probably want the BlockBasedLog to know about multiple
        // generations, and therefore we'd keep the one BlockBasedLog but create
        // a new generation (as we do with ObjectBasedLog).

        // Calculate an eviction atime for the new index: use 10% of available space:
        let target_size = (self.block_access.size() / 100) * TARGET_CACHE_SIZE_PCT;
        info!(
            "target cache size for storage size {} is {}",
            self.block_access.size(),
            target_size
        );
        let mut new_index = ZettaCacheIndex::open(
            self.block_access.clone(),
            self.extent_allocator.clone(),
            ZettaCacheIndexPhys::new(self.atime_histogram.atime_for_target_size(target_size)),
        )
        .await;
        info!(
            "set new eviction atime to {:?}",
            new_index.get_histogram_start()
        );

        info!(
            "writing new index to merge {} pending changes into index of {} entries ({} MB)",
            self.pending_changes.len(),
            old_index.log.len(),
            old_index.log.num_bytes() / 1024 / 1024,
        );
        // XXX load operation_log and verify that the pending_changes match it?
        let mut free_list: Vec<IndexValue> = Vec::new();
        let mut pending_changes_iter = self.pending_changes.iter().peekable();
        old_index
            .log
            .iter()
            .for_each(|entry| {
                // First, process any pending changes which are before this
                // index entry, which must be all Inserts (Removes,
                // RemoveThenInserts, and AtimeUpdates refer to existing Index
                // entries).
                trace!("next index entry: {:?}", entry);
                while let Some((pc_key, PendingChange::Insert(pc_value))) =
                    pending_changes_iter.peek()
                {
                    if **pc_key >= entry.key {
                        break;
                    }
                    // Add this new entry to the index
                    add_to_index_or_list(
                        &mut new_index,
                        &mut free_list,
                        IndexEntry {
                            key: **pc_key,
                            value: *pc_value,
                        },
                    );
                    pending_changes_iter.next();
                }

                let next_pc_opt = pending_changes_iter.peek();
                match next_pc_opt {
                    Some((pc_key, PendingChange::Remove())) => {
                        if **pc_key == entry.key {
                            // Don't write this entry to the new generation.
                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // There shouldn't be a pending removal of an entry that doesn't exist in the index.
                            assert_gt!(**pc_key, entry.key);
                            add_to_index_or_list(&mut new_index, &mut free_list, entry);
                        }
                    }
                    Some((pc_key, PendingChange::Insert(_pc_value))) => {
                        // Insertions are processed above.  There can't be an
                        // index entry with the same key.  If there were, it has
                        // to be removed first, resulting in a
                        // PendingChange::RemoveThenInsert.
                        assert_gt!(**pc_key, entry.key);
                        add_to_index_or_list(&mut new_index, &mut free_list, entry);
                    }
                    Some((pc_key, PendingChange::RemoveThenInsert(pc_value))) => {
                        if **pc_key == entry.key {
                            // This key must have been removed (evicted) and then re-inserted.
                            // Add the pending change to the next generation instead of the current index's entry
                            assert_eq!(pc_value.size, entry.value.size);
                            add_to_index_or_list(
                                &mut new_index,
                                &mut free_list,
                                IndexEntry {
                                    key: **pc_key,
                                    value: *pc_value,
                                },
                            );

                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // We shouldn't have skipped any, because there has to be a corresponding Index entry
                            assert_gt!(**pc_key, entry.key);
                            add_to_index_or_list(&mut new_index, &mut free_list, entry);
                        }
                    }
                    Some((pc_key, PendingChange::UpdateAtime(pc_value))) => {
                        if **pc_key == entry.key {
                            // Add the pending entry to the next generation instead of the current index's entry
                            assert_eq!(pc_value.location, entry.value.location);
                            assert_eq!(pc_value.size, entry.value.size);
                            add_to_index_or_list(
                                &mut new_index,
                                &mut free_list,
                                IndexEntry {
                                    key: **pc_key,
                                    value: *pc_value,
                                },
                            );

                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // We shouldn't have skipped any, because there has to be a corresponding Index entry
                            assert_gt!(**pc_key, entry.key);
                            add_to_index_or_list(&mut new_index, &mut free_list, entry);
                        }
                    }
                    None => {
                        // no more pending changes
                        add_to_index_or_list(&mut new_index, &mut free_list, entry);
                    }
                }
                future::ready(())
            })
            .await;
        while let Some((pc_key, PendingChange::Insert(pc_value))) = pending_changes_iter.peek() {
            // Add this new entry to the index
            trace!(
                "remaining pending change, appending to new index: {:?} {:?}",
                pc_key,
                pc_value
            );
            add_to_index_or_list(
                &mut new_index,
                &mut free_list,
                IndexEntry {
                    key: **pc_key,
                    value: *pc_value,
                },
            );
            // Consume pending change.  We don't do that in the `while let`
            // because we want to leave any unmatched items in the iterator so
            // that we can print them out when failing below.
            pending_changes_iter.next();
        }
        // Other pending changes refer to existing index entries and therefore should have been processed above
        assert!(
            pending_changes_iter.peek().is_none(),
            "next={:?}",
            pending_changes_iter.peek().unwrap()
        );

        // Note: the caller is about to flush as well, but we want to count the time in the below info!
        new_index.flush().await;

        // Free the evicted blocks from the cache
        debug!("freeing {} blocks", free_list.len());
        for value in free_list {
            let extent = Extent {
                location: value.location,
                size: value.size,
            };
            self.block_allocator.free(&extent);
        }

        debug!("new histogram: {:#?}", new_index.atime_histogram);

        self.pending_changes.clear();
        self.operation_log.clear();
        self.atime_histogram = new_index.atime_histogram.clone();

        info!(
            "wrote new index with {} entries ({} MB) in {:.1}s ({:.1}MB/s)",
            new_index.log.len(),
            new_index.log.num_bytes() / 1024 / 1024,
            begin.elapsed().as_secs_f64(),
            (new_index.log.num_bytes() as f64 / 1024f64 / 1024f64) / begin.elapsed().as_secs_f64(),
        );
        new_index
    }
}
