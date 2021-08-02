use crate::base_types::*;
use crate::object_access::ObjectAccess;
use crate::pool::*;
use crate::server::handler_return_ok;
use crate::server::HandlerReturn;
use crate::server::SerialHandlerReturn;
use crate::server::Server;
use anyhow::anyhow;
use anyhow::Result;
use log::*;
use nvpair::{NvData, NvList, NvListRef};
use std::sync::Arc;
use uuid::Uuid;
use zettacache::base_types::*;
use zettacache::ZettaCache;

pub struct KernelServerState {
    cache: Option<ZettaCache>,
    id: Uuid,
}

#[derive(Default)]
struct KernelConnectionState {
    pool: Option<Arc<Pool>>,
    cache: Option<ZettaCache>,
    id: Uuid,
}

impl KernelServerState {
    fn connection_handler(&self) -> KernelConnectionState {
        KernelConnectionState {
            cache: self.cache.as_ref().cloned(),
            id: self.id,
            ..Default::default()
        }
    }

    pub fn start(socket_dir: &str, cache: Option<ZettaCache>, id: Uuid) {
        let socket_path = format!("{}/zfs_kernel_socket", socket_dir);
        let mut server = Server::new(
            &socket_path,
            KernelServerState { cache, id },
            Box::new(Self::connection_handler),
        );

        KernelConnectionState::register(&mut server);
        server.start();
    }
}

impl KernelConnectionState {
    fn register(server: &mut Server<KernelServerState, KernelConnectionState>) {
        server.register_serial_handler("create pool", Box::new(Self::create_pool));
        server.register_serial_handler("open pool", Box::new(Self::open_pool));
        server.register_serial_handler("resume complete", Box::new(Self::resume_complete));
        server.register_handler("begin txg", Box::new(Self::begin_txg));
        server.register_handler("resume txg", Box::new(Self::resume_txg));
        server.register_handler("flush writes", Box::new(Self::flush_writes));
        server.register_handler("end txg", Box::new(Self::end_txg));
        server.register_handler("write block", Box::new(Self::write_block));
        server.register_handler("free block", Box::new(Self::free_block));
        server.register_handler("read block", Box::new(Self::read_block));
        server.register_handler("close pool", Box::new(Self::close_pool));
        server.register_handler("exit agent", Box::new(Self::exit_agent));
    }

    fn get_object_access(nvl: &NvListRef) -> Result<ObjectAccess> {
        let bucket_name = nvl.lookup_string("bucket")?;
        let region_str = nvl.lookup_string("region")?;
        let endpoint = nvl.lookup_string("endpoint")?;
        let readonly = nvl.lookup_string("readonly").is_ok();
        let credentials_profile: Option<String> = nvl
            .lookup_string("credentials_profile")
            .ok()
            .map(|s| s.to_string_lossy().to_string());
        Ok(ObjectAccess::new(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            bucket_name.to_str().unwrap(),
            credentials_profile,
            readonly,
        ))
    }

    fn create_pool(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);
        Box::pin(async move {
            let guid = PoolGuid(nvl.lookup_uint64("GUID")?);
            let name = nvl.lookup_string("name")?;
            let object_access = Self::get_object_access(nvl.as_ref())?;

            Pool::create(&object_access, name.to_str()?, guid).await;
            let mut response = NvList::new_unique_names();
            response.insert("Type", "pool create done").unwrap();
            response.insert("GUID", &guid.0).unwrap();

            debug!("sending response: {:?}", response);
            Ok(Some(response))
        })
    }

    fn open_pool(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);
        Box::pin(async move {
            let guid = PoolGuid(nvl.lookup_uint64("GUID")?);
            let object_access = Self::get_object_access(nvl.as_ref())?;
            let cache = self.cache.as_ref().cloned();

            let (pool, phys_opt, next_block) =
                match Pool::open(&object_access, guid, cache, self.id).await {
                    Err(PoolOpenError::MmpError(hostname)) => {
                        let mut response = NvList::new_unique_names();
                        response.insert("Type", "pool open failed").unwrap();
                        response.insert("cause", "MMP").unwrap();
                        response.insert("hostname", hostname.as_str()).unwrap();
                        debug!("sending response: {:?}", response);
                        return Ok(Some(response));
                    }
                    x => x.unwrap(),
                };

            let mut response = NvList::new_unique_names();
            response.insert("Type", "pool open done").unwrap();
            response.insert("GUID", &guid.0).unwrap();
            if let Some(phys) = phys_opt {
                response
                    .insert("uberblock", &phys.get_zfs_uberblock()[..])
                    .unwrap();
                response
                    .insert("config", &phys.get_zfs_config()[..])
                    .unwrap();
            }

            response.insert("next_block", &next_block.0).unwrap();

            self.pool = Some(Arc::new(pool));
            debug!("sending response: {:?}", response);
            Ok(Some(response))
        })
    }

    fn begin_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG")?);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.begin_txg(txg);

        handler_return_ok(None)
    }

    fn resume_txg(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG")?);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.resume_txg(txg);

        handler_return_ok(None)
    }

    fn resume_complete(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);

        // This is .await'ed by the server's thread, so we can't see any new writes
        // come in while it's in progress.
        Box::pin(async move {
            let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
            pool.resume_complete().await;
            Ok(None)
        })
    }

    fn flush_writes(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        let block = BlockId(nvl.lookup_uint64("block")?);
        pool.initiate_flush(block);
        handler_return_ok(None)
    }

    // sends response when completed
    async fn end_txg_impl(
        pool: Arc<Pool>,
        uberblock: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<Option<NvList>> {
        let stats = pool.end_txg(uberblock, config).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "end txg done").unwrap();
        nvl.insert("blocks_count", &stats.blocks_count).unwrap();
        nvl.insert("blocks_bytes", &stats.blocks_bytes).unwrap();
        nvl.insert("pending_frees_count", &stats.pending_frees_count)
            .unwrap();
        nvl.insert("pending_frees_bytes", &stats.pending_frees_bytes)
            .unwrap();
        nvl.insert("objects_count", &stats.objects_count).unwrap();
        debug!("sending response: {:?}", nvl);
        Ok(Some(nvl))
    }

    fn end_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
            let uberblock = nvl.lookup("uberblock").unwrap().data();
            let config = nvl.lookup("config").unwrap().data();
            if let NvData::Uint8Array(slice) = uberblock {
                if let NvData::Uint8Array(slice2) = config {
                    Self::end_txg_impl(pool, slice.to_vec(), slice2.to_vec()).await
                } else {
                    panic!("config not expected type")
                }
            } else {
                panic!("uberblock not expected type")
            }
        }))
    }

    /// queue write, sends response when completed (persistent).
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, nvl: NvList) -> HandlerReturn {
        let block = BlockId(nvl.lookup_uint64("block")?);
        let data = nvl.lookup("data")?.data();
        let request_id = nvl.lookup_uint64("request_id")?;
        let token = nvl.lookup_uint64("token")?;
        if let NvData::Uint8Array(slice) = data {
            trace!(
                "got write request id={}: {:?} len={}",
                request_id,
                block,
                slice.len()
            );

            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow!("no pool open"))?
                .clone();
            // Need to write_block() before spawning, so that the Pool knows what's been written before resume_complete()
            let fut = pool.write_block(block, slice.to_vec());
            Ok(Box::pin(async move {
                fut.await;
                let mut nvl = NvList::new_unique_names();
                nvl.insert("Type", "write done").unwrap();
                nvl.insert("block", &block.0).unwrap();
                nvl.insert("request_id", &request_id).unwrap();
                nvl.insert("token", &token).unwrap();
                trace!("sending response: {:?}", nvl);
                Ok(Some(nvl))
            }))
        } else {
            Err(anyhow!("data {:?} not expected type", data))
        }
    }

    fn free_block(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block")?);
        let size = nvl.lookup_uint64("size")? as u32;

        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.free_block(block, size);

        handler_return_ok(None)
    }

    fn read_block(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block")?);
        let request_id = nvl.lookup_uint64("request_id")?;
        let token = nvl.lookup_uint64("token")?;

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
            let data = pool.read_block(block).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            nvl.insert("token", &token).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            trace!(
                "sending read done response: block={} req={} data=[{} bytes]",
                block,
                request_id,
                data.len()
            );
            Ok(Some(nvl))
        }))
    }

    fn close_pool(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        let pool_opt = self.pool.take();
        Ok(Box::pin(async move {
            if let Some(pool) = pool_opt {
                Arc::try_unwrap(pool)
                    .map_err(|_| {
                        anyhow!("pool close request while there are other operations in progress")
                    })?
                    .close()
                    .await;
            }
            let mut response = NvList::new_unique_names();
            response.insert("Type", "pool close done").unwrap();
            debug!("sending response: {:?}", response);
            Ok(Some(response))
        }))
    }

    // XXX This doesn't actually exit the agent, it just closes the connection,
    // which the kernel could do from its end.  It's unclear what the kernel
    // really wants.
    fn exit_agent(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        Err(anyhow!("exit requested"))
    }
}
