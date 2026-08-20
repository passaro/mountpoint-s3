//! A local S3 endpoint for the `data::` tests, built on `wiremock`.
//!
//! # What it implements
//!
//! `HeadObject`, and `GetObject` with a `Range` header, including `If-Match` precondition
//! checking. For writes ([`UploadFixture`]): the multipart set (`CreateMultipartUpload`,
//! `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`) and `PutObject`, recording
//! every part body so a test can assemble what S3 would have stored.
//!
//! A deliberately small subset. The point is to observe what the data plane does on the wire,
//! which a fuller S3 implementation would not make any clearer, and to be able to inject a
//! condition a real endpoint will not: a per-response delay, and failing `UploadPart`.
//!
//! It records **every requested range**, which is what makes assertions possible against the
//! wire rather than against the reader's own counters: a test can check the exact byte ranges
//! asked for rather than only the bytes returned.
//!
//! Its main limitation is pacing: a delay applies to a whole response, so a slow body arrives
//! late but all at once rather than trickling.

// Shared by three test binaries, each using a different part of this fixture, so per-binary
// dead-code analysis flags the items the other binaries use.
#![allow(dead_code)]

pub mod workload;

use std::sync::{Arc, Mutex};

use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize};
use mountpoint_s3_fs::data::{ObjectSpec, RtmConfig, RtmDataPlane, WriteSpec};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// The object's content: every byte is a function of its own offset.
///
/// A constant fill, or anything repeating within a part, would let an offset bug pass — a read
/// served from the wrong place would still contain the "right" bytes. With this, a wrong byte
/// identifies where the data actually came from.
pub fn expected_byte(offset: u64) -> u8 {
    // 251 is prime, so the cycle aligns with no power-of-two part or read size.
    (offset % 251) as u8
}

pub fn expected_bytes(offset: u64, len: usize) -> Vec<u8> {
    (0..len as u64).map(|i| expected_byte(offset + i)).collect()
}

/// The `Range` values the endpoint was asked for, in arrival order.
pub type RangeLog = Arc<Mutex<Vec<String>>>;

const ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

/// Serves `HeadObject` and ranged `GetObject` for synthetic objects.
///
/// Every key returns the same synthetic content, so one responder serves any number of distinct
/// objects — which is what [`SharedFixture`] needs to give each reader its own object without
/// standing up an endpoint per reader.
struct ObjectResponder {
    size: u64,
    ranges: RangeLog,
    /// Delay applied per response, standing in for a slow link.
    ///
    /// Coarse: the whole response is held back and then arrives at once, rather than the body
    /// trickling. What it does reproduce is the part that matters for scheduling — a request
    /// occupies its concurrency slot far longer than an in-memory endpoint would hold it.
    delay: Option<std::time::Duration>,
}

impl Respond for ObjectResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        // If-Match, when present, must match or the request is a 412. This is what makes the
        // read path's `ObjectSpec::etag` guard observable.
        if let Some(if_match) = request.headers.get("if-match")
            && if_match.to_str().unwrap_or_default() != ETAG
        {
            return ResponseTemplate::new(412);
        }

        let head = request.method.as_str().eq_ignore_ascii_case("HEAD");
        let range = request
            .headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        if let Some(range) = range.as_deref()
            && !head
        {
            self.ranges.lock().unwrap().push(range.to_owned());
        }

        let (start, end) = match range.as_deref().and_then(parse_range) {
            // Inclusive on the wire; clamp to the object.
            Some((start, end)) => (start, end.min(self.size.saturating_sub(1))),
            None => (0, self.size.saturating_sub(1)),
        };
        if start >= self.size {
            return ResponseTemplate::new(416);
        }
        let len = (end - start + 1) as usize;

        let mut response = ResponseTemplate::new(if range.is_some() { 206 } else { 200 })
            .append_header("etag", ETAG)
            .append_header("accept-ranges", "bytes")
            .append_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        if range.is_some() {
            response = response.append_header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, self.size).as_str(),
            );
        }
        // Only bodies are paced; a HEAD is metadata and should stay cheap.
        if let Some(delay) = self.delay
            && !head
        {
            response = response.set_delay(delay);
        }
        if head {
            // A HEAD carries the length but no body.
            return response.append_header("content-length", self.size.to_string().as_str());
        }
        response.set_body_bytes(expected_bytes(start, len))
    }
}

/// First and last byte of an HTTP `bytes=<start>-<end>` request range.
fn parse_range(value: &str) -> Option<(u64, u64)> {
    let (start, end) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
    let start = start.trim().parse().ok()?;
    let end = end.trim().parse().unwrap_or(u64::MAX);
    Some((start, end))
}

/// One endpoint and **one RTM client**, shared by several readers.
///
/// Necessary for any test about priority. Priority is a weight applied within a single
/// scheduler's ready set, so [`Fixture`]'s client-per-reader arrangement leaves nothing to
/// order — one transfer per scheduler means no queue. Here the readers share a scheduler and
/// [`ConcurrencyMode::Explicit`] caps in-flight requests across all of them, so they genuinely
/// queue for slots.
///
/// The trade-off: each top-level transfer forms its own scheduling group and fan-out buys no
/// extra share, so a reader holding more cursors than another takes more of the client. Tests
/// using this must hold cursor counts equal across arms, or report them.
pub struct SharedFixture {
    plane: RtmDataPlane,
    size: u64,
    ranges: RangeLog,
    _server: MockServer,
}

impl SharedFixture {
    /// `concurrency` is the cap on in-flight requests across every reader; `delay` is applied to
    /// each response body.
    pub async fn new(size: u64, config: RtmConfig, concurrency: usize, delay: Option<std::time::Duration>) -> Self {
        let server = MockServer::start().await;
        let ranges: RangeLog = Arc::new(Mutex::new(Vec::new()));

        Mock::given(|_: &Request| true)
            .respond_with(ObjectResponder {
                size,
                ranges: ranges.clone(),
                delay,
            })
            .mount(&server)
            .await;

        let tm = aws_sdk_s3_transfer_manager::Client::new(
            aws_sdk_s3_transfer_manager::Config::builder()
                .client(test_s3_client_for(&server))
                .concurrency(ConcurrencyMode::Explicit(concurrency))
                .part_size(PartSize::Target(DEFAULT_PART_SIZE))
                .build(),
        );

        Self {
            plane: RtmDataPlane::new(tm, config),
            size,
            ranges,
            _server: server,
        }
    }

    /// An [`ObjectSpec`] for a distinct key. Distinct keys keep readers from serving each other
    /// from resident data, which would be a different experiment.
    pub fn spec(&self, key: &str) -> ObjectSpec {
        ObjectSpec::new("test-bucket", key, ETAG, self.size)
    }

    pub fn plane(&self) -> &RtmDataPlane {
        &self.plane
    }

    pub fn requested_ranges(&self) -> Vec<String> {
        self.ranges.lock().unwrap().clone()
    }
}

/// An `aws_sdk_s3::Client` pointed at a local mock endpoint.
///
/// `pub` so a test can build a transfer manager the fixtures would not — e.g. one that leaves the
/// part size unset, to check that the data plane rejects it.
pub fn test_s3_client_for(server: &MockServer) -> aws_sdk_s3::Client {
    let creds = aws_sdk_s3::config::Credentials::new("ATESTCLIENT", "astestsecretkey", None, None, "test");
    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(server.uri())
        // Path addressing: the mock has no per-bucket virtual hosts.
        .force_path_style(true)
        .credentials_provider(creds)
        .build();
    aws_sdk_s3::Client::from_conf(sdk_config)
}

/// A running endpoint plus an [`RtmDataPlane`] pointed at it.
///
/// Holds the `MockServer`: dropping the fixture stops the endpoint, so tests must keep it
/// alive for as long as they read.
pub struct Fixture {
    pub plane: RtmDataPlane,
    pub spec: ObjectSpec,
    /// Ranges requested so far, in arrival order.
    pub ranges: RangeLog,
    _server: MockServer,
}

/// Part size used unless a test names its own.
///
/// Set explicitly rather than left to `PartSize::Auto`, for two reasons. Part size is the quantum
/// the read-ahead ceiling is divided into, so leaving it implicit makes the resolved ceiling depend
/// on a default the test does not state. And `Auto` resolves to 5 MiB for downloads while the
/// benchmark runs at 8 MiB, so tests and measurements would not describe the same configuration.
/// This matches the benchmark.
pub const DEFAULT_PART_SIZE: u64 = 8 * 1024 * 1024;

impl Fixture {
    pub async fn new(size: u64, config: RtmConfig) -> Self {
        Self::build(size, config, DEFAULT_PART_SIZE, None).await
    }

    /// As [`new`](Self::new), with every response delayed — a stand-in for a slow link.
    pub async fn slow(size: u64, config: RtmConfig, delay: std::time::Duration) -> Self {
        Self::build(size, config, DEFAULT_PART_SIZE, Some(delay)).await
    }

    /// As [`new`](Self::new), with an explicit RTM part size.
    ///
    /// Part size is the quantum `ReadAhead::Parts(n)` works in, and what a byte read-ahead ceiling
    /// is divided by, so tests probing either must set it. Note the RTM raises any value below
    /// 5 MiB to 5 MiB for downloads.
    pub async fn with_part_size(size: u64, config: RtmConfig, part_size: u64) -> Self {
        Self::build(size, config, part_size, None).await
    }

    async fn build(size: u64, config: RtmConfig, part_size: u64, delay: Option<std::time::Duration>) -> Self {
        let server = MockServer::start().await;
        let ranges: RangeLog = Arc::new(Mutex::new(Vec::new()));

        Mock::given(|_: &Request| true)
            .respond_with(ObjectResponder {
                size,
                ranges: ranges.clone(),
                delay,
            })
            .mount(&server)
            .await;

        let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(test_s3_client_for(&server))
            .part_size(PartSize::Target(part_size));
        let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config.build());

        Self {
            plane: RtmDataPlane::new(tm, config),
            spec: ObjectSpec::new("test-bucket", "object", ETAG, size),
            ranges,
            _server: server,
        }
    }

    /// Ranges requested so far, in arrival order.
    pub fn requested_ranges(&self) -> Vec<String> {
        self.ranges.lock().unwrap().clone()
    }

    /// Total bytes covered by every range requested so far.
    ///
    /// This is what the data plane pulled from S3, measured at the endpoint rather than
    /// self-reported — so it is an independent check on `ReaderStats::bytes_fetched`.
    pub fn bytes_requested(&self) -> u64 {
        self.requested_ranges()
            .iter()
            .filter_map(|r| parse_range(r))
            .map(|(start, end)| end.saturating_sub(start) + 1)
            .sum()
    }
}

/// What an upload did on the wire, recorded so a test can assert on it rather than on the
/// writer's own counters.
#[derive(Debug, Default)]
pub struct UploadLog {
    /// `CreateMultipartUpload` calls.
    pub creates: usize,
    /// `(part number, bytes)`, in arrival order — which is *not* part order, since RTM uploads
    /// parts concurrently.
    pub parts: Vec<(i32, Vec<u8>)>,
    /// `CompleteMultipartUpload` bodies, as XML.
    pub completes: Vec<String>,
    /// `AbortMultipartUpload` calls.
    pub aborts: usize,
    /// `PutObject` bodies — a non-multipart upload.
    pub puts: Vec<Vec<u8>>,
    /// `x-amz-checksum-crc64nvme` values seen on `CompleteMultipartUpload`, which is where a
    /// full-object checksum computed by the caller lands.
    pub complete_checksums: Vec<String>,
}

impl UploadLog {
    /// The object S3 would have stored: parts ordered by number and concatenated.
    ///
    /// This is the check that matters for write correctness — the writer's own byte count can
    /// be right while the assembled object is wrong.
    pub fn assembled(&self) -> Vec<u8> {
        if let Some(body) = self.puts.first() {
            return body.clone();
        }
        let mut parts: Vec<&(i32, Vec<u8>)> = self.parts.iter().collect();
        parts.sort_by_key(|(n, _)| *n);
        parts.iter().flat_map(|(_, data)| data.iter().copied()).collect()
    }

    /// Part sizes in part-number order. S3 requires every part but the last to be equal.
    pub fn part_sizes(&self) -> Vec<usize> {
        let mut parts: Vec<&(i32, Vec<u8>)> = self.parts.iter().collect();
        parts.sort_by_key(|(n, _)| *n);
        parts.iter().map(|(_, data)| data.len()).collect()
    }

    /// Whether this went out as a multipart upload.
    pub fn is_multipart(&self) -> bool {
        self.creates > 0
    }
}

const UPLOAD_ID: &str = "test-upload-id";

/// Serves `PutObject` and the multipart set, recording everything.
struct UploadResponder {
    log: Arc<Mutex<UploadLog>>,
    /// Applied to `UploadPart` and `PutObject` responses, standing in for a slow link.
    delay: Option<std::time::Duration>,
    /// When set, `UploadPart` fails with this status. Used to test the failure path — a writer
    /// blocked on a full channel must learn the transfer died rather than hang.
    fail_parts_with: Option<u16>,
}

impl Respond for UploadResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let method = request.method.as_str().to_ascii_uppercase();
        let query = request.url.query().unwrap_or_default();
        let has = |key: &str| request.url.query_pairs().any(|(k, _)| k == key);
        let part_number = request
            .url
            .query_pairs()
            .find(|(k, _)| k == "partNumber")
            .and_then(|(_, v)| v.parse::<i32>().ok());

        // POST ?uploads — CreateMultipartUpload
        if method == "POST" && has("uploads") {
            self.log.lock().unwrap().creates += 1;
            return ResponseTemplate::new(200).set_body_raw(
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <InitiateMultipartUploadResult>\
                     <Bucket>test-bucket</Bucket><Key>object</Key>\
                     <UploadId>{UPLOAD_ID}</UploadId>\
                     </InitiateMultipartUploadResult>"
                ),
                "application/xml",
            );
        }

        // PUT ?partNumber=N&uploadId=... — UploadPart
        if method == "PUT"
            && let Some(part_number) = part_number
        {
            if let Some(status) = self.fail_parts_with {
                return ResponseTemplate::new(status);
            }
            self.log.lock().unwrap().parts.push((part_number, request.body.clone()));
            let mut response = ResponseTemplate::new(200).append_header("etag", part_etag(part_number).as_str());
            if let Some(delay) = self.delay {
                response = response.set_delay(delay);
            }
            return response;
        }

        // POST ?uploadId=... — CompleteMultipartUpload
        if method == "POST" && has("uploadId") {
            let body = String::from_utf8_lossy(&request.body).into_owned();
            {
                let mut log = self.log.lock().unwrap();
                log.completes.push(body);
                if let Some(checksum) = request
                    .headers
                    .get("x-amz-checksum-crc64nvme")
                    .and_then(|v| v.to_str().ok())
                {
                    log.complete_checksums.push(checksum.to_owned());
                }
            }
            return ResponseTemplate::new(200).set_body_raw(
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <CompleteMultipartUploadResult>\
                     <Location>http://localhost/test-bucket/object</Location>\
                     <Bucket>test-bucket</Bucket><Key>object</Key>\
                     <ETag>{ETAG_ESCAPED}</ETag>\
                     </CompleteMultipartUploadResult>"
                ),
                "application/xml",
            );
        }

        // DELETE ?uploadId=... — AbortMultipartUpload
        if method == "DELETE" && has("uploadId") {
            self.log.lock().unwrap().aborts += 1;
            return ResponseTemplate::new(204);
        }

        // PUT with no partNumber — PutObject
        if method == "PUT" {
            self.log.lock().unwrap().puts.push(request.body.clone());
            let mut response = ResponseTemplate::new(200).append_header("etag", ETAG);
            if let Some(delay) = self.delay {
                response = response.set_delay(delay);
            }
            return response;
        }

        panic!("upload endpoint got an unexpected request: {method} {query}");
    }
}

/// The etag S3 would return for a part. Distinct per part so a wrong `CompleteMultipartUpload`
/// part list is visible in the recorded XML.
fn part_etag(part_number: i32) -> String {
    format!("\"part-{part_number}\"")
}

/// `ETAG` with its quotes XML-escaped, for embedding in a response body.
const ETAG_ESCAPED: &str = "&quot;d41d8cd98f00b204e9800998ecf8427e&quot;";

/// An endpoint that accepts uploads, plus an [`RtmDataPlane`] pointed at it.
pub struct UploadFixture {
    pub plane: RtmDataPlane,
    pub log: Arc<Mutex<UploadLog>>,
    _server: MockServer,
}

impl UploadFixture {
    pub async fn new(config: RtmConfig) -> Self {
        Self::build(config, None, None, None, None).await
    }

    /// With an explicit RTM part size, which is the quantum MPU parts are cut to.
    pub async fn with_part_size(config: RtmConfig, part_size: u64) -> Self {
        Self::build(config, Some(part_size), None, None, None).await
    }

    /// With every part response delayed and RTM concurrency capped, so the writer cannot get more
    /// than one part ahead of RTM's take and stalls become observable. The cap matters: with spare
    /// concurrency RTM takes every part instantly and the single-part rendezvous never blocks.
    pub async fn slow(config: RtmConfig, part_size: u64, delay: std::time::Duration) -> Self {
        Self::build(config, Some(part_size), Some(delay), None, Some(2)).await
    }

    /// With `UploadPart` failing, to exercise the path where a writer must learn the transfer
    /// died rather than hang waiting on a part nobody will take. Left on default concurrency: a
    /// cap would keep failing/retrying parts occupying the slots and delay the transfer dropping
    /// the source, which is what wakes the writer.
    pub async fn failing_parts(config: RtmConfig, part_size: u64, status: u16) -> Self {
        Self::build(config, Some(part_size), None, Some(status), None).await
    }

    async fn build(
        config: RtmConfig,
        part_size: Option<u64>,
        delay: Option<std::time::Duration>,
        fail_parts_with: Option<u16>,
        concurrency: Option<usize>,
    ) -> Self {
        let server = MockServer::start().await;
        let log = Arc::new(Mutex::new(UploadLog::default()));

        Mock::given(|_: &Request| true)
            .respond_with(UploadResponder {
                log: log.clone(),
                delay,
                fail_parts_with,
            })
            .mount(&server)
            .await;

        let mut tm_config = aws_sdk_s3_transfer_manager::Config::builder().client(test_s3_client_for(&server));
        if let Some(part_size) = part_size {
            // Anything below 5 MiB is silently raised to it, so tests that care about part
            // boundaries must use multiples of 5 MiB.
            tm_config = tm_config.part_size(PartSize::Target(part_size));
        }
        if let Some(concurrency) = concurrency {
            tm_config = tm_config.concurrency(ConcurrencyMode::Explicit(concurrency));
        }
        // No `multipart_threshold` override: it would have no effect. A caller-supplied part
        // stream can only be uploaded as a multipart upload, whatever its size.
        let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config.build());

        Self {
            plane: RtmDataPlane::new(tm, config),
            log,
            _server: server,
        }
    }

    /// A [`WriteSpec`] for the test object. Size-less: the writer streams to end-of-stream with
    /// no declared bound, as a filesystem writer does.
    pub fn spec(&self) -> WriteSpec {
        WriteSpec::new("test-bucket", "object")
    }

    /// Snapshot of what the endpoint saw.
    pub fn log(&self) -> std::sync::MutexGuard<'_, UploadLog> {
        self.log.lock().unwrap()
    }
}
