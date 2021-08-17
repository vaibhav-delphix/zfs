use anyhow::{anyhow, Context, Result};
use async_stream::stream;
use bytes::Bytes;
use core::time::Duration;
use futures::future::Either;
use futures::{future, Future, TryStreamExt};
use http::StatusCode;
use lazy_static::lazy_static;
use log::*;
use lru::LruCache;
use rand::prelude::*;
use rusoto_core::{ByteStream, RusotoError};
use rusoto_credential::{AutoRefreshingProvider, ChainProvider, ProfileProvider};
use rusoto_s3::*;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, fmt::Display};
use std::{env, error::Error};
use tokio::{sync::watch, time::error::Elapsed};
use zettacache::get_tunable;

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
    static ref NON_RETRYABLE_ERRORS: Vec<StatusCode> = vec![
        StatusCode::BAD_REQUEST,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::METHOD_NOT_ALLOWED,
        StatusCode::PRECONDITION_FAILED,
        StatusCode::PAYLOAD_TOO_LARGE,
    ];
    // log operations that take longer than this with info!()
    static ref LONG_OPERATION_DURATION: Duration = Duration::from_secs(get_tunable("long_operation_secs", 2));

    static ref OBJECT_DELETION_BATCH_SIZE: usize = get_tunable("object_deletion_batch_size", 1000);
}

#[derive(Clone)]
pub struct ObjectAccess {
    client: rusoto_s3::S3Client,
    bucket_str: String,
    readonly: bool,
    region_str: String,
    endpoint_str: String,
}

/*
 * For testing, prefix all object keys with this string. In cases where objects are returned from
 * a call like list_objects and then fetched with get, we could end up doubling the prefix. We
 * could either strip the prefix from the beginning of every object we return, or we can only
 * prefix an object if it isn't already prefixed. We do the latter here, for conciseness, but in
 * the future we may want to revisit this decision.
 */
pub fn prefixed(key: &str) -> String {
    match key.starts_with(format!("{}zfs", *PREFIX).as_str()) {
        true => key.to_string(),
        false => format!("{}{}", *PREFIX, key),
    }
}

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum OAError<E> {
    TimeoutError(Elapsed),
    RequestError(RusotoError<E>),
    Other(anyhow::Error),
}

impl<E> Display for OAError<E>
where
    E: std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OAError::TimeoutError(e) => e.fmt(f),
            OAError::RequestError(e) => e.fmt(f),
            OAError::Other(e) => e.fmt(f),
        }
    }
}

impl<E> Error for OAError<E> where E: std::error::Error + 'static {}

impl<E> From<RusotoError<E>> for OAError<E> {
    fn from(e: RusotoError<E>) -> Self {
        Self::RequestError(e)
    }
}

async fn retry_impl<F, O, E>(msg: &str, f: impl Fn() -> F) -> Result<O, OAError<E>>
where
    E: core::fmt::Debug,
    F: Future<Output = Result<O, OAError<E>>>,
{
    let mut delay = Duration::from_secs_f64(thread_rng().gen_range(0.001..0.2));
    loop {
        match f().await {
            res @ Ok(_) => return res,
            res @ Err(OAError::RequestError(RusotoError::Service(_))) => return res,
            res @ Err(OAError::RequestError(RusotoError::Credentials(_))) => return res,
            Err(OAError::RequestError(RusotoError::Unknown(bhr))) => {
                if NON_RETRYABLE_ERRORS.contains(&bhr.status) {
                    return Err(OAError::RequestError(RusotoError::Unknown(bhr)));
                }
            }
            Err(e) => {
                debug!(
                    "{} returned: {:?}; retrying in {}ms",
                    msg,
                    e,
                    delay.as_millis()
                );
                if delay > *LONG_OPERATION_DURATION {
                    info!(
                        "long retry: {} returned: {:?}; retrying in {:?}",
                        msg, e, delay
                    );
                }
            }
        }
        tokio::time::sleep(delay).await;
        delay = delay.mul_f64(thread_rng().gen_range(1.5..2.5));
    }
}

async fn retry<F, O, E>(
    msg: &str,
    timeout_opt: Option<Duration>,
    f: impl Fn() -> F,
) -> Result<O, OAError<E>>
where
    E: core::fmt::Debug,
    F: Future<Output = Result<O, OAError<E>>>,
{
    debug!("{}: begin", msg);
    let begin = Instant::now();
    let result = match timeout_opt {
        Some(timeout) => match tokio::time::timeout(timeout, retry_impl(msg, f)).await {
            Err(e) => Err(OAError::TimeoutError(e)),
            Ok(res2) => res2,
        },
        None => retry_impl(msg, f).await,
    };
    let elapsed = begin.elapsed();
    debug!("{}: returned in {}ms", msg, elapsed.as_millis());
    if elapsed > *LONG_OPERATION_DURATION {
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

    pub fn get_client(endpoint: &str, region_str: &str, profile: Option<String>) -> S3Client {
        info!("region: {}", region_str);
        info!("Endpoint: {}", endpoint);
        info!("Profile: {:?}", profile);

        let auto_refreshing_provider =
            AutoRefreshingProvider::new(ChainProvider::with_profile_provider(
                ProfileProvider::with_default_credentials(
                    profile.unwrap_or_else(|| "default".to_owned()),
                )
                .unwrap(),
            ))
            .unwrap();

        let http_client = rusoto_core::HttpClient::new().unwrap();
        let region = ObjectAccess::get_custom_region(endpoint, region_str);
        rusoto_s3::S3Client::new_with(http_client, auto_refreshing_provider, region)
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

    pub fn new(
        endpoint: &str,
        region_str: &str,
        bucket: &str,
        profile: Option<String>,
        readonly: bool,
    ) -> Self {
        let client = ObjectAccess::get_client(endpoint, region_str, profile);

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
        let v = retry(&msg, timeout, || async {
            let req = GetObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            let output = self.client.get_object(req).await?;
            let begin = Instant::now();
            let mut v = Vec::with_capacity(output.content_length.unwrap_or(0) as usize);
            match output
                .body
                .unwrap()
                .try_for_each(|b| {
                    v.extend_from_slice(&b);
                    future::ready(Ok(()))
                })
                .await
            {
                Err(e) => {
                    debug!("{}: error while reading ByteStream: {}", msg, e);
                    Err(OAError::RequestError(e.into()))
                }
                Ok(_) => {
                    debug!(
                        "{}: got {} bytes of data in {}ms",
                        msg,
                        v.len(),
                        begin.elapsed().as_millis()
                    );
                    Ok(v)
                }
            }
        })
        .await
        .with_context(|| format!("Failed to {}", msg))?;

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
        let full_start_after = start_after.map(|sa| prefixed(&sa));
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
                    // Note: Ok(...?) converts the RusotoError to an OAError for us
                    Ok(self.client.list_objects_v2(req).await?)
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
            // Note: Ok(...?) converts the RusotoError to an OAError for us
            Ok(self.client.head_object(req).await?)
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
    ) -> Result<PutObjectOutput, OAError<PutObjectError>> {
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
                // Note: Ok(...?) converts the RusotoError to an OAError for us
                Ok(self.client.put_object(req).await?)
            },
        )
        .await
    }

    fn invalidate_cache(key: &str, data: &[u8]) {
        let mut c = CACHE.lock().unwrap();
        let mykey = key.to_string();
        if c.cache.contains(&mykey) {
            debug!("found {} in cache when putting - invalidating", key);
            // XXX unfortuate to be copying; this happens every time when
            // freeing (we get/modify/put the object).  Maybe when freeing,
            // the get() should not add to the cache since it's probably
            // just polluting.
            c.cache.put(mykey, Arc::new(data.to_vec()));
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
    ) -> Result<PutObjectOutput, OAError<PutObjectError>> {
        Self::invalidate_cache(key, &data);

        self.put_object_impl(key, data, timeout).await
    }

    pub async fn delete_object(&self, key: &str) {
        self.delete_objects(&[key.to_string()]).await;
    }

    pub async fn delete_objects(&self, keys: &[String]) {
        for chunk in keys.chunks(*OBJECT_DELETION_BATCH_SIZE) {
            let msg = format!(
                "delete {} objects including {}",
                chunk.len(),
                prefixed(&chunk[0])
            );
            assert!(!self.readonly);
            retry(&msg, None, || async {
                let v: Vec<_> = chunk
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
                let output = self.client.delete_objects(req).await?;
                if let Some(errs) = output.errors {
                    match errs.get(0) {
                        Some(e) => return Err(OAError::Other(anyhow!("{:?}", e))),
                        None => return Ok(()),
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        }
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
