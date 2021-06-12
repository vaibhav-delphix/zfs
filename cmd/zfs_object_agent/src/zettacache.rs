use crate::base_types::BlockID;
use crate::base_types::OnDisk;
use crate::base_types::PoolGUID;
use crate::block_access::*;
use crate::block_based_log::*;
use crate::extent_allocator::ExtentAllocator;
use crate::extent_allocator::ExtentAllocatorPhys;
use anyhow::Result;
use futures::future;
use futures::stream::*;
use log::*;
use more_asserts::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

const SUPERBLOCK_SIZE: usize = 4 * 1024;
//const SUPERBLOCK_MAGIC: u64 = 0x2e11acac4e;
const DEFAULT_CHECKPOINT_RING_BUFFER_SIZE: usize = 1 * 1024 * 1024;
const DEFAULT_SLAB_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_METADATA_SIZE: usize = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
struct ZettaSuperBlockPhys {
    checkpoint_ring_buffer_size: u32,
    slab_size: u32,
    last_generation: GenerationID,
    last_checkpoint: Extent,
    // XXX put sector size in here too and verify it matches what the disk says now?
    // XXX put disk size in here so we can detect expansion?
}

impl ZettaSuperBlockPhys {
    // XXX when we have multiple disks, will this be stored on a specific one?  Or copied on all of them?
    async fn read(block_access: &BlockAccess) -> Result<ZettaSuperBlockPhys> {
        let raw = block_access
            .read_raw(DiskLocation { offset: 0 }, SUPERBLOCK_SIZE)
            .await;
        let (this, _): (Self, usize) = block_access.json_chunk_from_raw(&raw)?;
        debug!("got {:#?}", this);
        Ok(this)
    }

    async fn write(&self, block_access: &BlockAccess) {
        debug!("writing {:#?}", self);
        let raw = block_access.json_chunk_to_raw(self);
        block_access
            .write_raw(DiskLocation { offset: 0 }, raw)
            .await;
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct DiskLocation {
    // note: will need to add disk ID to support multiple disks
    pub offset: u64,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Extent {
    pub location: DiskLocation,
    // XXX for space efficiency and clarity, make this u32? since it's stored on disk?
    pub size: usize, // note: since we read it into contiguous memory, it can't be more than usize
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct GenerationID(u64);
impl GenerationID {
    pub fn next(&self) -> GenerationID {
        GenerationID(self.0 + 1)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ZettaCheckpointPhys {
    generation: GenerationID,
    extent_allocator: ExtentAllocatorPhys,
    last_valid_data_offset: u64, // XXX move to BlockAllocatorPhys
    index: BlockBasedLogPhys,
    chunk_summary: BlockBasedLogPhys,
    operation_log: BlockBasedLogPhys,
}

impl ZettaCheckpointPhys {
    async fn read(
        block_access: &BlockAccess,
        location: DiskLocation,
        size: usize,
    ) -> ZettaCheckpointPhys {
        let raw = block_access.read_raw(location, size).await;
        let (this, _): (Self, usize) = block_access.json_chunk_from_raw(&raw).unwrap();
        debug!("got {:#?}", this);
        this
    }

    /*
    fn all_logs(&self) -> Vec<&BlockBasedLogPhys> {
        vec![&self.index, &self.chunk_summary, &self.operation_log]
    }
    */
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]

struct IndexKey {
    guid: PoolGUID,
    block: BlockID,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct IndexValue {
    atime: u64,
    location: DiskLocation,
    // XXX remove this and figure out based on which slab it's in?  However,
    // currently we need to return the right buffer size to the kernel, and it
    // isn't passing us the expected read size.  So we need to change some
    // interfaces to make that work right.
    size: usize,
}

struct PendingChangeWithValue {
    value: IndexValue,
    atime_dirty: bool,
    #[allow(dead_code)] // XXX location_dirty is not yet used
    location_dirty: bool,
}

enum PendingChange {
    WithValue(PendingChangeWithValue),
    Removal(),
}

#[derive(Clone)]
pub struct ZettaCache {
    state: Arc<tokio::sync::Mutex<ZettaCacheState>>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct IndexEntry {
    key: IndexKey,
    value: IndexValue,
}
impl OnDisk for IndexEntry {}
impl BlockBasedLogEntry for IndexEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct ChunkSummaryEntry {
    first_key: IndexKey,
    offset: u64, // logical offset in log
}
impl OnDisk for ChunkSummaryEntry {}
impl BlockBasedLogEntry for ChunkSummaryEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum OperationLogEntry {
    Insert((IndexKey, IndexValue)),
    Remove(IndexKey),
}
impl OnDisk for OperationLogEntry {}
impl BlockBasedLogEntry for OperationLogEntry {}

struct ZettaCacheState {
    block_access: Arc<BlockAccess>,
    size: u64,
    super_phys: ZettaSuperBlockPhys,
    last_valid_data_offset: u64, // XXX move to a BlockAllocator struct
    pending_changes: BTreeMap<IndexKey, PendingChange>,
    extent_allocator: Arc<ExtentAllocator>,
    index: BlockBasedLog<IndexEntry>,
    chunk_summary: BlockBasedLog<ChunkSummaryEntry>,
    operation_log: BlockBasedLog<OperationLogEntry>,
}

impl ZettaCache {
    pub async fn create(path: &str) {
        let block_access = BlockAccess::new(path).await;
        let metadata_start = SUPERBLOCK_SIZE + DEFAULT_CHECKPOINT_RING_BUFFER_SIZE;
        let data_start = metadata_start + DEFAULT_METADATA_SIZE;
        let checkpoint = ZettaCheckpointPhys {
            generation: GenerationID(0),
            extent_allocator: ExtentAllocatorPhys {
                first_valid_offset: metadata_start as u64,
                last_valid_offset: data_start as u64,
            },
            last_valid_data_offset: data_start as u64,
            index: Default::default(),
            chunk_summary: Default::default(),
            operation_log: Default::default(),
        };
        let raw = block_access.json_chunk_to_raw(&checkpoint);
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
            last_checkpoint: Extent {
                location: DiskLocation {
                    offset: SUPERBLOCK_SIZE as u64,
                },
                size: checkpoint_size,
            },
            last_generation: GenerationID(0),
        };
        phys.write(&block_access).await;
    }

    pub async fn open(path: &str) -> ZettaCache {
        let block_access = Arc::new(BlockAccess::new(path).await);
        let size = block_access.size();

        // if superblock not present, create new cache
        // XXX need a real mechanism for creating/managing the cache
        let phys = match ZettaSuperBlockPhys::read(&block_access).await {
            Ok(phys) => phys,
            Err(_) => {
                Self::create(path).await;
                ZettaSuperBlockPhys::read(&block_access).await.unwrap()
            }
        };

        let checkpoint = ZettaCheckpointPhys::read(
            &block_access,
            phys.last_checkpoint.location,
            phys.last_checkpoint.size,
        )
        .await;

        assert_eq!(checkpoint.generation, phys.last_generation);

        let metadata_start = SUPERBLOCK_SIZE + phys.checkpoint_ring_buffer_size as usize;
        // XXX pass in the metadata_start to ExtentAllocator::open, rather than
        // having this represented twice in the on-disk format?
        assert_eq!(
            metadata_start as u64,
            checkpoint.extent_allocator.first_valid_offset
        );
        let extent_allocator = Arc::new(ExtentAllocator::open(&checkpoint.extent_allocator));

        let mut state = ZettaCacheState {
            block_access: block_access.clone(),
            size,
            pending_changes: BTreeMap::new(),
            index: BlockBasedLog::open(
                block_access.clone(),
                extent_allocator.clone(),
                &checkpoint.index,
            ),
            chunk_summary: BlockBasedLog::open(
                block_access.clone(),
                extent_allocator.clone(),
                &checkpoint.chunk_summary,
            ),
            operation_log: BlockBasedLog::open(
                block_access.clone(),
                extent_allocator.clone(),
                &checkpoint.operation_log,
            ),
            extent_allocator,
            last_valid_data_offset: checkpoint.last_valid_data_offset,
            super_phys: phys,
        };

        // load operation_log from disk into the in-memory pending_changes
        let begin = Instant::now();
        let mut num_insert_entries: u64 = 0;
        let mut num_remove_entries: u64 = 0;
        state
            .operation_log
            .iter()
            .for_each(|entry| {
                match entry {
                    OperationLogEntry::Insert((key, value)) => {
                        state.pending_changes.insert(
                            key,
                            PendingChange::WithValue(PendingChangeWithValue {
                                value,
                                atime_dirty: false,
                                location_dirty: false,
                            }),
                        );
                        num_insert_entries += 1;
                    }
                    OperationLogEntry::Remove(key) => {
                        state.pending_changes.insert(key, PendingChange::Removal());
                        num_remove_entries += 1;
                    }
                };
                future::ready(())
            })
            .await;
        info!(
            "loaded operation_log from {} allocs and {} frees in {}ms",
            num_insert_entries,
            num_remove_entries,
            begin.elapsed().as_millis()
        );

        let this = ZettaCache {
            state: Arc::new(tokio::sync::Mutex::new(state)),
        };

        let my_cache = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                my_cache.state.lock().await.flush_checkpoint().await;
            }
        });

        this
    }

    pub async fn get(&self, guid: PoolGUID, block: BlockID) -> Option<Vec<u8>> {
        let mut state = self.state.lock().await;
        state.get(guid, block).await
    }

    pub async fn put(&self, guid: PoolGUID, block: BlockID, buf: Vec<u8>) {
        let mut state = self.state.lock().await;
        state.put(guid, block, buf).await
    }
}

impl ZettaCacheState {
    /// Retrieve from cache, if available.  We assume that we won't often have
    /// concurrent requests for the same block, so in that case we may read it
    /// multiple times.
    async fn get(&mut self, guid: PoolGUID, block: BlockID) -> Option<Vec<u8>> {
        let key = IndexKey { guid, block };
        let mut value = match self.pending_changes.get(&key) {
            Some(PendingChange::WithValue(x)) => x.value,
            Some(PendingChange::Removal()) => return None,
            None => {
                // XXX Check on-disk index, yield new value to insert to pending
                return None;
            }
        };

        if value.location.offset < self.extent_allocator.get_phys().last_valid_offset {
            // The metadata overwrote this data, so it's no longer in the cache.
            // Remove from index and return None.
            trace!(
                "{:?} at {:?} was overwritten by metadata allocator; removing from cache",
                key,
                value
            );
            self.pending_changes.insert(key, PendingChange::Removal());
            self.operation_log.append(OperationLogEntry::Remove(key));
            return None;
        }

        trace!("cache hit: reading {:?} from {:?}", key, value);
        let vec = self.block_access.read_raw(value.location, value.size).await;

        value.atime = self.current_atime();

        self.pending_changes
            .entry(key)
            .and_modify(|e| match e {
                PendingChange::WithValue(with_value) => {
                    assert_eq!(with_value.value.location, value.location);
                    with_value.value = value;
                    with_value.atime_dirty = true;
                }
                PendingChange::Removal() => {
                    *e = PendingChange::WithValue(PendingChangeWithValue {
                        value,
                        atime_dirty: true,
                        location_dirty: false,
                    })
                }
            })
            .or_insert(PendingChange::WithValue(PendingChangeWithValue {
                value,
                atime_dirty: true,
                location_dirty: false,
            }));

        Some(vec)
    }

    /// Insert this block to the cache, if space and performance parameters
    /// allow.  It may be a recent cache miss, or a recently-written block.
    async fn put(&mut self, guid: PoolGUID, block: BlockID, buf: Vec<u8>) {
        // XXX ideally this function would not be async and would not block, just kick off the writes
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

        // XXX we don't really want to wait for the write to complete here
        self.block_access.write_raw(location, aligned_buf).await;

        // XXX if this is past the last block of the main index, we can write it
        // there (and location_dirty:false) instead of logging it

        let key = IndexKey { guid, block };
        let value = IndexValue {
            atime: self.current_atime(),
            location,
            size: buf_size,
        };

        self.pending_changes.insert(
            key.clone(),
            PendingChange::WithValue(PendingChangeWithValue {
                value: value.clone(),
                atime_dirty: true,
                location_dirty: true,
            }),
        );

        self.operation_log
            .append(OperationLogEntry::Insert((key, value)));
    }

    // XXX change to return a newtype?
    pub fn current_atime(&self) -> u64 {
        // XXX change to minutes of running pool?
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// returns offset, or None if there's no space
    fn allocate_block(&mut self, size: usize) -> Option<DiskLocation> {
        let end = self.last_valid_data_offset + size as u64;
        if end < self.size {
            let location_opt = Some(DiskLocation {
                offset: self.last_valid_data_offset,
            });
            self.last_valid_data_offset = end;
            location_opt
        } else {
            debug!("block allocation of {} bytes failed", size);
            None
        }
    }

    async fn flush_checkpoint(&mut self) {
        debug!(
            "flushing checkpoint {:?}",
            self.super_phys.last_generation.next()
        );

        let begin = Instant::now();

        future::join3(
            self.index.flush(),
            self.chunk_summary.flush(),
            self.operation_log.flush(),
        )
        .await;

        let checkpoint = ZettaCheckpointPhys {
            generation: self.super_phys.last_generation.next(),
            extent_allocator: self.extent_allocator.get_phys(),
            index: self.index.get_phys(),
            chunk_summary: self.chunk_summary.get_phys(),
            operation_log: self.operation_log.get_phys(),
            last_valid_data_offset: self.last_valid_data_offset,
        };

        let mut checkpoint_location = DiskLocation {
            offset: self.super_phys.last_checkpoint.location.offset
                + self.super_phys.last_checkpoint.size as u64,
        };

        let raw = self.block_access.json_chunk_to_raw(&checkpoint);
        if raw.len()
            > (checkpoint.extent_allocator.first_valid_offset - checkpoint_location.offset) as usize
        {
            // Out of space; go back to the beginning of the checkpoint space.
            checkpoint_location.offset = SUPERBLOCK_SIZE as u64;
            assert_le!(
                raw.len(),
                (self.super_phys.last_checkpoint.location.offset - checkpoint_location.offset)
                    as usize,
            );
            // XXX The above assertion could fail if there isn't enough
            // checkpoint space for 3 checkpoints (the existing one that
            // we're writing before, the one we're writing, and the space
            // after the existing one that we aren't using).  Note that we
            // could in theory reduce this to 2 checkpoints if we allowed a
            // single checkpoint to wrap around (part of it at the end and
            // then part at the beginning of the space).
        }
        self.super_phys.last_checkpoint = Extent {
            location: checkpoint_location,
            size: raw.len(),
        };
        self.block_access.write_raw(checkpoint_location, raw).await;

        self.super_phys.last_generation = self.super_phys.last_generation.next();
        self.super_phys.write(&self.block_access).await;

        debug!(
            "completed checkpoint {:?} in {}ms",
            self.super_phys.last_generation,
            begin.elapsed().as_millis()
        );
    }
}
