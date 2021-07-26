use anyhow::{Context, Result};
use async_stream::stream;
use bytes::Bytes;
use core::time::Duration;
use futures::{future::Either, Future, StreamExt};
use http::StatusCode;
use lazy_static::lazy_static;
use log::*;
use lru::LruCache;
use rand::prelude::*;
use rusoto_core::{ByteStream, RusotoError};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::*;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, fmt::Display};
use std::{env, error::Error};
use tokio::{sync::watch, time::error::Elapsed};

struct ObjectCache {
    // XXX cache key should include Bucket
    cache: LruCache<String, Arc<Vec<u8>>>,
    reading: HashMap<String, watch::Receiver<Option<Arc<Vec<u8>>>>>,
}

lazy_static! {
    static ref CACHE: std::sync::Mutex<ObjectCache> = std::sync::Mutex::new(ObjectCache {
        cache: LruCache::new(100),
        reading: HashMap::new(),
    });
    static ref PREFIX: String = match env::var("AWS_PREFIX") {
        Ok(val) => format!("{}/", val),
        Err(_) => "".to_string(),
    };
}

// log operations that take longer than this with info!()
const LONG_OPERATION_DURATION: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ObjectAccess {
    client: rusoto_s3::S3Client,
    bucket_str: String,
    readonly: bool,
    region_str: String,
    endpoint_str: String,
}

/// For testing, prefix all object keys with this string.
pub fn prefixed(key: &str) -> String {
    format!("{}{}", *PREFIX, key)
}

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum OAError<E>
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
{
    TimeoutError(Elapsed),
    RequestError(E),
}

impl<E> Display for OAError<E>
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OAError::TimeoutError(e) => e.fmt(f),
            OAError::RequestError(e) => std::fmt::Display::fmt(&e, f),
        }
    }
}

impl<E> Error for OAError<E> where E: core::fmt::Debug + core::fmt::Display + std::error::Error {}

async fn retry_impl<F, O, E>(msg: &str, f: impl Fn() -> F) -> Result<O, E>
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
    F: Future<Output = (bool, Result<O, E>)>,
{
    let mut delay = Duration::from_secs_f64(thread_rng().gen_range(0.001..0.2));
    let result = loop {
        match f().await {
            (true, Err(e)) => {
                debug!(
                    "{} returned: {:?}; retrying in {}ms",
                    msg,
                    e,
                    delay.as_millis()
                );
                if delay > LONG_OPERATION_DURATION {
                    info!(
                        "long retry: {} returned: {:?}; retrying in {:?}",
                        msg, e, delay
                    );
                }
            }
            (_, res) => {
                break res;
            }
        }
        tokio::time::sleep(delay).await;
        delay = delay.mul_f64(thread_rng().gen_range(1.5..2.5));
    };
    result
}

async fn retry<F, O, E>(
    msg: &str,
    timeout_opt: Option<Duration>,
    f: impl Fn() -> F,
) -> Result<O, OAError<E>>
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
    F: Future<Output = (bool, Result<O, E>)>,
{
    debug!("{}: begin", msg);
    let begin = Instant::now();
    let result = match timeout_opt {
        Some(timeout) => match tokio::time::timeout(timeout, retry_impl(msg, f)).await {
            Err(e) => Err(OAError::TimeoutError(e)),
            Ok(res2) => res2.map_err(OAError::RequestError),
        },
        None => retry_impl(msg, f).await.map_err(OAError::RequestError),
    };
    let elapsed = begin.elapsed();
    debug!("{}: returned in {}ms", msg, elapsed.as_millis());
    if elapsed > LONG_OPERATION_DURATION {
        info!("long completion: {}: returned in {:?}", msg, elapsed);
    }
    result
}

impl ObjectAccess {
    fn get_custom_region(endpoint: &str, region_str: &str) -> rusoto_core::Region {
        rusoto_core::Region::Custom {
            name: region_str.to_owned(),
            endpoint: endpoint.to_owned(),
        }
    }

    pub fn get_client_with_creds(
        endpoint: &str,
        region_str: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> S3Client {
        info!("region: {:?}", region_str);
        info!("Endpoint: {}", endpoint);

        let http_client = rusoto_core::HttpClient::new().unwrap();
        let creds = rusoto_core::credential::StaticProvider::new(
            access_key_id.to_string(),
            secret_access_key.to_string(),
            None,
            None,
        );
        let region = ObjectAccess::get_custom_region(endpoint, region_str);
        rusoto_s3::S3Client::new_with(http_client, creds, region)
    }

    pub fn get_client(endpoint: &str, region_str: &str) -> S3Client {
        info!("region: {:?}", region_str);
        info!("Endpoint: {}", endpoint);

        let creds = DefaultCredentialsProvider::new().unwrap();
        let http_client = rusoto_core::HttpClient::new().unwrap();
        let region = ObjectAccess::get_custom_region(endpoint, region_str);
        rusoto_s3::S3Client::new_with(http_client, creds, region)
    }

    pub fn from_client(
        client: rusoto_s3::S3Client,
        bucket: &str,
        readonly: bool,
        endpoint: &str,
        region: &str,
    ) -> Self {
        ObjectAccess {
            client,
            bucket_str: bucket.to_string(),
            readonly,
            region_str: region.to_string(),
            endpoint_str: endpoint.to_string(),
        }
    }

    pub fn new(endpoint: &str, region_str: &str, bucket: &str, readonly: bool) -> Self {
        let client = ObjectAccess::get_client(endpoint, region_str);

        ObjectAccess {
            client,
            bucket_str: bucket.to_string(),
            readonly,
            region_str: region_str.to_string(),
            endpoint_str: endpoint.to_string(),
        }
    }

    pub fn release_client(self) -> S3Client {
        self.client
    }

    pub async fn get_object_impl(&self, key: &str, timeout: Option<Duration>) -> Result<Vec<u8>> {
        let msg = format!("get {}", prefixed(key));
        let output = retry(&msg, timeout, || async {
            let req = GetObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            let res = self.client.get_object(req).await;
            match res {
                Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => (false, res),
                _ => (true, res),
            }
        })
        .await
        .with_context(|| format!("Failed to {}", msg))?;
        let begin = Instant::now();
        let mut v = match output.content_length {
            None => Vec::new(),
            Some(len) => Vec::with_capacity(len as usize),
        };
        output
            .body
            .unwrap() // XXX could the connection die while we're reading the bytestream?
            .for_each(|x| {
                let b = x.unwrap();
                v.extend_from_slice(&b);
                async {}
            })
            .await;
        debug!(
            "{}: got {} bytes of data in additional {}ms",
            msg,
            v.len(),
            begin.elapsed().as_millis()
        );

        Ok(v)
    }

    pub async fn get_object(&self, key: &str) -> Result<Arc<Vec<u8>>> {
        let either = {
            // need this block separate so that we can drop the mutex before the .await
            let mut c = CACHE.lock().unwrap();
            let mykey = key.to_string();
            match c.cache.get(&mykey) {
                Some(v) => {
                    debug!("found {} in cache", key);
                    return Ok(v.clone());
                }
                None => match c.reading.get(key) {
                    None => {
                        let (tx, rx) = watch::channel::<Option<Arc<Vec<u8>>>>(None);
                        c.reading.insert(mykey, rx);
                        Either::Left(async move {
                            let v = Arc::new(self.get_object_impl(key, None).await?);
                            let mut myc = CACHE.lock().unwrap();
                            tx.send(Some(v.clone())).unwrap();
                            myc.cache.put(key.to_string(), v.clone());
                            myc.reading.remove(key);
                            Ok(v)
                        })
                    }
                    Some(rx) => {
                        debug!("found {} read in progress", key);
                        let mut myrx = rx.clone();
                        Either::Right(async move {
                            if let Some(vec) = myrx.borrow().as_ref() {
                                return Ok(vec.clone());
                            }
                            // Note: "else" or "match" statement not allowed
                            // here because the .borrow()'ed Ref is not dropped
                            // until the end of the else/match

                            // XXX if the sender drops due to
                            // get_object_impl() failing, we don't get a
                            // very good error message, but maybe that
                            // doesn't matter since the behavior is
                            // otherwise correct (we return an Error)
                            // XXX should we make a wrapper around the
                            // watch::channel that has borrow() wait until the
                            // first value is sent?
                            myrx.changed().await?;
                            let b = myrx.borrow();
                            // Note: we assume that the once it's changed, it
                            // has to be Some()
                            Ok(b.as_ref().unwrap().clone())
                        })
                    }
                },
            }
        };
        either.await
    }
    pub async fn list_objects(
        &self,
        prefix: &str,
        start_after: Option<String>,
    ) -> Vec<ListObjectsV2Output> {
        self.list_objects_impl(prefix, start_after, Some("/".to_owned()))
            .await
    }

    pub async fn list_objects_impl(
        &self,
        prefix: &str,
        start_after: Option<String>,
        delimiter: Option<String>,
    ) -> Vec<ListObjectsV2Output> {
        let full_prefix = prefixed(prefix);
        let full_start_after = match start_after {
            Some(sa) => Some(prefixed(&sa)),
            None => None,
        };
        let mut results = Vec::new();
        let mut continuation_token = None;
        loop {
            continuation_token = match retry(
                &format!("list {} (after {:?})", full_prefix, full_start_after),
                None,
                || async {
                    let req = ListObjectsV2Request {
                        bucket: self.bucket_str.clone(),
                        continuation_token: continuation_token.clone(),
                        delimiter: delimiter.clone(),
                        fetch_owner: Some(false),
                        prefix: Some(full_prefix.clone()),
                        start_after: full_start_after.clone(),
                        ..Default::default()
                    };
                    (true, self.client.list_objects_v2(req).await)
                },
            )
            .await
            .unwrap()
            {
                #[rustfmt::skip]
                output @ ListObjectsV2Output {next_continuation_token: Some(_), ..} => {
                    let next_token = output.next_continuation_token.clone();
                    results.push(output);
                    next_token
                }
                output => {
                    results.push(output);
                    break;
                }
            };
        }
        results
    }

    pub async fn collect_objects(&self, prefix: &str, start_after: Option<String>) -> Vec<String> {
        let mut vec = Vec::new();
        for output in self.list_objects(prefix, start_after).await {
            if let Some(objects) = output.contents {
                for object in objects {
                    vec.push(object.key.unwrap());
                }
            }
        }
        vec
    }

    pub async fn collect_prefixes(&self, prefix: &str) -> Vec<String> {
        let mut vec = Vec::new();
        for output in self.list_objects(prefix, None).await {
            if let Some(prefixes) = output.common_prefixes {
                for prefix in prefixes {
                    vec.push(prefix.prefix.unwrap());
                }
            }
        }
        vec
    }

    pub async fn collect_all_objects(&self, prefix: &str) -> Vec<String> {
        let mut vec = Vec::new();
        for output in self.list_objects_impl(prefix, None, None).await {
            if let Some(objects) = output.contents {
                for object in objects {
                    vec.push(object.key.unwrap());
                }
            }
        }
        vec
    }

    pub async fn head_object(&self, key: &str) -> Option<HeadObjectOutput> {
        let res = retry(&format!("head {}", prefixed(key)), None, || async {
            let req = HeadObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            let res = self.client.head_object(req).await;
            match res {
                Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => (false, res),
                Err(RusotoError::Unknown(rusoto_core::request::BufferedHttpResponse {
                    status: StatusCode::NOT_FOUND,
                    body: _,
                    headers: _,
                })) => (false, res),
                _ => (true, res),
            }
        })
        .await;
        res.ok()
    }

    pub async fn object_exists(&self, key: &str) -> bool {
        self.head_object(key).await.is_some()
    }

    async fn put_object_impl(
        &self,
        key: &str,
        data: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<PutObjectOutput, OAError<RusotoError<PutObjectError>>> {
        let len = data.len();
        let bytes = Bytes::from(data);
        assert!(!self.readonly);
        retry(
            &format!("put {} ({} bytes)", prefixed(key), len),
            timeout,
            || async {
                let my_bytes = bytes.clone();
                let stream = ByteStream::new_with_size(stream! { yield Ok(my_bytes)}, len);

                let req = PutObjectRequest {
                    bucket: self.bucket_str.clone(),
                    key: prefixed(key),
                    body: Some(stream),
                    ..Default::default()
                };
                (true, self.client.put_object(req).await)
            },
        )
        .await
    }

    fn invalidate_cache(key: &str, data: &Vec<u8>) {
        let mut c = CACHE.lock().unwrap();
        let mykey = key.to_string();
        if c.cache.contains(&mykey) {
            debug!("found {} in cache when putting - invalidating", key);
            // XXX unfortuate to be copying; this happens every time when
            // freeing (we get/modify/put the object).  Maybe when freeing,
            // the get() should not add to the cache since it's probably
            // just polluting.
            c.cache.put(mykey, Arc::new(data.clone()));
        }
    }

    pub async fn put_object(&self, key: &str, data: Vec<u8>) {
        Self::invalidate_cache(key, &data);

        self.put_object_impl(key, data, None).await.unwrap();
    }

    pub async fn put_object_timed(
        &self,
        key: &str,
        data: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<PutObjectOutput, OAError<RusotoError<PutObjectError>>> {
        Self::invalidate_cache(key, &data);

        self.put_object_impl(key, data, timeout).await
    }

    pub async fn delete_object(&self, key: &str) {
        self.delete_objects(&[key.to_string()]).await;
    }

    // return if retry needed
    // XXX unfortunate that this level of retry can't be handled by retry()
    // XXX ideally we would only retry the failed deletions, not all of them
    async fn delete_objects_impl(&self, keys: &[String]) -> bool {
        let msg = format!(
            "delete {} objects including {}",
            keys.len(),
            prefixed(&keys[0])
        );
        assert!(!self.readonly);
        let output = retry(&msg, None, || async {
            let v: Vec<_> = keys
                .iter()
                .map(|x| ObjectIdentifier {
                    key: prefixed(x),
                    ..Default::default()
                })
                .collect();
            let req = DeleteObjectsRequest {
                bucket: self.bucket_str.clone(),
                delete: Delete {
                    objects: v,
                    quiet: Some(true),
                },
                ..Default::default()
            };
            (true, self.client.delete_objects(req).await)
        })
        .await
        .unwrap();
        if let Some(errs) = output.errors {
            if !errs.is_empty() {}
            for err in errs {
                debug!("delete: error from s3; retrying in 100ms: {:?}", err);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            return true;
        }
        return false;
    }

    // XXX just have it take ObjectIdentifiers? but they would need to be
    // prefixed already, to avoid creating new ObjectIdentifiers
    // Note: AWS supports up to 1000 keys per delete_objects request.
    // XXX should we split them up here?
    pub async fn delete_objects(&self, keys: &[String]) {
        while self.delete_objects_impl(keys).await {}
    }

    pub fn bucket(&self) -> String {
        self.bucket_str.clone()
    }

    pub fn region(&self) -> String {
        self.region_str.clone()
    }

    pub fn endpoint(&self) -> String {
        self.endpoint_str.clone()
    }

    pub fn readonly(&self) -> bool {
        self.readonly
    }
}
