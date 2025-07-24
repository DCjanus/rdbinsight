use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::{Context as AnyhowContext, anyhow, bail, ensure};
use async_async_io::read::{AsyncAsyncRead, PollRead};
use async_trait::async_trait;
use backoff::{ExponentialBackoff, backoff::Backoff};
use memchr::memmem;
use redis_protocol::{
    codec::resp2_encode_command,
    resp2::{
        decode::decode,
        encode::encode_bytes,
        types::{OwnedFrame, Resp2Frame},
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tokio_util::time::FutureExt;
use tracing::{debug, info, warn};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            combinators::{read_exact, read_tag, read_while},
            ring_buffer::RingBuffer,
        },
        error::NeedMoreData,
    },
    parser_trace,
    source::RDBStream,
};

/// Delimiter-based reader for diskless mode
pub struct DelimiterReader {
    inner: TcpStream,
    delimiter: [u8; 40],
    buff: RingBuffer,
    done: bool,
    eof_reached: bool, // Track if we've reached EOF from the TCP stream
}

impl DelimiterReader {
    pub fn new(inner: TcpStream, delimiter: [u8; 40], mut buff: RingBuffer) -> Self {
        let mut done = false;
        if let Some(pos) = memmem::find(buff.filled(), &delimiter) {
            debug!(name: "delimiter_reader.done", "delimiter found in function DelimiterReader::new");
            done = true;
            buff.truncate(pos);
        }

        Self {
            inner,
            delimiter,
            buff,
            done,
            eof_reached: false,
        }
    }

    async fn feed_more(&mut self) -> AnyResult<()> {
        if self.done {
            return Ok(());
        }

        if self.buff.remaining_capacity() == 0 {
            return Ok(());
        }

        let n = self.buff.read_from(&mut self.inner).await?;
        parser_trace!("delimiter_reader.feed_more");

        // Check if we've reached EOF from the TCP stream
        if n == 0 {
            self.eof_reached = true;
            // If we reached EOF but haven't found the delimiter, this is an error
            if !self.done {
                let delimiter_hex = hex::encode(self.delimiter);
                bail!(
                    "DelimiterReader: TCP connection ended (EOF) without finding expected delimiter. \
                     Expected delimiter: {}, buffered data size: {} bytes. \
                     This indicates that Redis did not send the expected end-of-stream marker, \
                     confirming the race condition where RDB data ends but no delimiter follows.",
                    delimiter_hex,
                    self.buff.size()
                );
            }
            return Ok(());
        }

        let search_start = self
            .buff
            .size()
            .saturating_sub(n + self.delimiter.len() - 1);
        let data = self.buff.filled();
        let search_zone = &data[search_start..];

        if let Some(relative_pos) = memmem::find(search_zone, &self.delimiter) {
            self.done = true;
            let delimiter_pos = search_start + relative_pos;
            self.buff.truncate(delimiter_pos);
            debug!(name: "delimiter_reader.done", "delimiter found in function DelimiterReader::feed_more");
        }

        Ok(())
    }
}

impl AsyncAsyncRead for DelimiterReader {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.done && self.buff.is_empty() {
            return Ok(0);
        }

        // Check if we've reached EOF without finding delimiter before attempting to read
        if self.eof_reached && !self.done {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "DelimiterReader: TCP stream ended without finding expected delimiter. \
                     Expected delimiter: {}, buffered data: {} bytes",
                    hex::encode(self.delimiter),
                    self.buff.size()
                ),
            ));
        }

        // Keep trying to feed more data until we have enough data or reach EOF or find delimiter
        loop {
            // Check if we're done first
            let is_done = self.done;

            // Get current safe zone data (this needs to be separate from the done check)
            let safe_zone_data = {
                let data = self.buff.filled();
                if is_done {
                    data.to_vec()
                } else {
                    // Return data excluding potential partial delimiter at the end
                    let safe_len = data.len().saturating_sub(self.delimiter.len() - 1);
                    data[..safe_len].to_vec()
                }
            };

            // If we have data to return or we're done, process and return
            if !safe_zone_data.is_empty() || is_done {
                let max_copy = safe_zone_data.len().min(buf.len());
                if max_copy > 0 {
                    buf[..max_copy].copy_from_slice(&safe_zone_data[..max_copy]);
                    self.buff.consume(max_copy);
                }
                return Ok(max_copy);
            }

            // If we reached EOF and don't have delimiter, this should have been caught above
            if self.eof_reached {
                return Ok(0);
            }

            // Try to feed more data
            if let Err(e) = self.feed_more().await {
                return Err(std::io::Error::other(format!(
                    "DelimiterReader feed_more failed: {e}"
                )));
            }

            // Check again if we've reached EOF without finding delimiter
            if self.eof_reached && !self.done {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "DelimiterReader: TCP stream ended without finding expected delimiter. \
                         Expected delimiter: {}, buffered data: {} bytes",
                        hex::encode(self.delimiter),
                        self.buff.size()
                    ),
                ));
            }
        }
    }
}

pub struct LimitedReader {
    inner: TcpStream,
    remaining: u64,
    buffer: RingBuffer,
}

impl LimitedReader {
    pub fn new(inner: TcpStream, limit: u64, buffer: RingBuffer) -> Self {
        // Account for data already in the buffer to ensure total read bytes don't exceed limit
        let buffered_size = buffer.size() as u64;
        let remaining = limit.saturating_sub(buffered_size);

        if buffered_size > 0 {
            debug!(
                "LimitedReader: limit={}, buffered_size={}, adjusted_remaining={}",
                limit, buffered_size, remaining
            );
        }

        Self {
            inner,
            remaining,
            buffer,
        }
    }

    async fn feed_more(&mut self) -> AnyResult<()> {
        if self.remaining == 0 {
            return Ok(());
        }

        let unfilled = self.buffer.unfilled_mut();
        if unfilled.is_empty() {
            return Ok(());
        }

        let max_read = unfilled.len().min(self.remaining as usize);
        let unfilled = &mut unfilled[..max_read];
        let n = self.inner.read(unfilled).await?;
        self.buffer.commit(n);
        self.remaining -= n as u64;
        parser_trace!("limited_reader.feed_more");

        Ok(())
    }
}

impl AsyncAsyncRead for LimitedReader {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 && self.buffer.is_empty() {
            return Ok(0);
        }

        // Only try to feed more data if buffer is empty and we have remaining bytes to read
        if self.buffer.is_empty() && self.remaining > 0 {
            self.feed_more().await.map_err(std::io::Error::other)?;
        }

        let data = self.buffer.filled();
        let to_copy = data.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&data[..to_copy]);
        self.buffer.consume(to_copy);

        Ok(to_copy)
    }
}

/// Reader enum for different RDB modes
pub enum RedisReader {
    Disk(PollRead<LimitedReader>),
    Diskless(PollRead<DelimiterReader>),
}

impl AsyncRead for RedisReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            RedisReader::Disk(reader) => Pin::new(reader).poll_read(cx, buf),
            RedisReader::Diskless(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

pub enum RDBMode {
    Diskless { delimiter: [u8; 40] },
    Disk { remain: u64 },
}

pub struct RedisRdbStream {
    reader: Option<RedisReader>,
    address: String,
    username: Option<String>,
    password: Option<String>,
}

impl RedisRdbStream {
    pub fn new(address: String, username: Option<String>, password: Option<String>) -> Self {
        Self {
            reader: None,
            address,
            username,
            password,
        }
    }
}

#[async_trait]
impl RDBStream for RedisRdbStream {
    async fn prepare(&mut self) -> AnyResult<()> {
        use tracing::debug;

        ensure!(self.reader.is_none(), "RedisRdbStream already prepared");

        debug!("[{}] Starting Redis connection...", self.address);
        let mut stream = TcpStream::connect(&self.address).await.with_context(|| {
            let address = &self.address;
            format!("Failed to connect to Redis instance at {address}")
        })?;
        let mut buffer = RingBuffer::default();

        debug!("[{}] Starting RDB handshake...", self.address);
        perform_rdb_handshake(
            &mut buffer,
            &mut stream,
            self.username.as_deref(),
            self.password.as_deref(),
        )
        .await
        .with_context(|| {
            let address = &self.address;
            format!("RDB handshake failed for instance {address}")
        })?;

        debug!("[{}] Reading RDB mode...", self.address);
        let mode = read_rdb_mode(&mut buffer, &mut stream)
            .await
            .context("read rdb mode")
            .with_context(|| {
                let address = &self.address;
                format!("Failed to determine RDB mode for instance {address}")
            })?;

        let reader = match mode {
            RDBMode::Disk { remain } => {
                info!(
                    "[{}] Using disk-based RDB mode, size: {} bytes",
                    self.address, remain
                );
                let async_reader = LimitedReader::new(stream, remain, buffer);
                RedisReader::Disk(PollRead::new(async_reader))
            }
            RDBMode::Diskless { delimiter } => {
                info!(
                    "[{}] Using diskless RDB mode, delimiter: {:?}",
                    self.address,
                    &delimiter[..8]
                );
                let async_reader = DelimiterReader::new(stream, delimiter, buffer);
                RedisReader::Diskless(PollRead::new(async_reader))
            }
        };

        self.reader = Some(reader);
        debug!(
            "[{}] RDB stream preparation completed successfully",
            self.address
        );
        Ok(())
    }

    fn instance(&self) -> String {
        self.address.clone()
    }
}

impl AsyncRead for RedisRdbStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this.reader {
            Some(ref mut reader) => Pin::new(reader).poll_read(cx, buf),
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "RedisRdbStream not initialized",
            ))),
        }
    }
}

pub async fn perform_rdb_handshake(
    buf: &mut RingBuffer,
    stream: &mut TcpStream,
    username: Option<&str>,
    password: Option<&str>,
) -> AnyResult<()> {
    perform_rdb_handshake_with_retry(buf, stream, username, password, 3).await
}

/// Perform RDB handshake with retry mechanism
pub async fn perform_rdb_handshake_with_retry(
    buf: &mut RingBuffer,
    stream: &mut TcpStream,
    username: Option<&str>,
    password: Option<&str>,
    max_retries: usize,
) -> AnyResult<()> {
    use tracing::debug;

    let mut backoff = ExponentialBackoff {
        max_elapsed_time: Some(Duration::from_secs(30)),
        max_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let mut attempt = 0;

    loop {
        attempt += 1;
        debug!("RDB handshake attempt {}/{}", attempt, max_retries + 1);

        match perform_rdb_handshake_internal(buf, stream, username, password).await {
            Ok(()) => {
                if attempt > 1 {
                    debug!("RDB handshake succeeded after {} attempts", attempt);
                }
                return Ok(());
            }
            Err(e) if attempt <= max_retries && is_retryable_error(&e) => {
                warn!(
                    "RDB handshake attempt {} failed (retryable): {}",
                    attempt, e
                );

                if let Some(delay) = backoff.next_backoff() {
                    debug!("Waiting {:?} before retry...", delay);
                    tokio::time::sleep(delay).await;
                    continue;
                } else {
                    return Err(e).context("RDB handshake failed after exhausting backoff time");
                }
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "RDB handshake failed on attempt {}/{} (non-retryable)",
                        attempt,
                        max_retries + 1
                    )
                });
            }
        }
    }
}

/// Check if an error is retryable
fn is_retryable_error(error: &anyhow::Error) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // Network-related errors that might be temporary
    error_msg.contains("connection refused") ||
    error_msg.contains("connection reset") ||
    error_msg.contains("timeout") ||
    error_msg.contains("temporarily unavailable") ||
    error_msg.contains("network unreachable") ||
    error_msg.contains("host unreachable") ||
    // Redis-specific temporary errors
    error_msg.contains("loading") ||
    error_msg.contains("busy") ||
    error_msg.contains("background save")
}

async fn perform_rdb_handshake_internal(
    buf: &mut RingBuffer,
    stream: &mut TcpStream,
    username: Option<&str>,
    password: Option<&str>,
) -> AnyResult<()> {
    use tracing::debug;

    if let Some(password) = password {
        debug!("Performing Redis authentication...");
        let mut parts = vec!["AUTH"];
        let username = username.unwrap_or("");
        if !username.is_empty() {
            debug!("Using username authentication for user: {}", username);
            parser_trace!("auth.with_username");
            parts.push(username);
        } else {
            debug!("Using password-only authentication");
            parser_trace!("auth.password_only");
        }
        parts.push(password);
        let command = parts.join(" ");
        send_command(stream, &command)
            .await
            .context("send auth command")?;
        let response = read_response(buf, stream, Duration::from_secs(1))
            .await
            .context("read auth response")?;
        ensure!(
            response.as_bytes() == Some(b"OK"),
            "unexpected response from auth: {:?}",
            response
        );
        debug!("Redis authentication successful");
    } else {
        debug!("No authentication required");
    }

    debug!("Sending PING command...");
    send_command(stream, "PING")
        .await
        .context("send ping command")?;
    let response = read_response(buf, stream, Duration::from_secs(1))
        .await
        .context("read ping response")?;
    ensure!(
        response.as_bytes() == Some(b"PONG"),
        "unexpected response from ping: {:?}",
        response
    );
    debug!("PING successful");

    debug!("Sending REPLCONF capa eof command...");
    send_command(stream, "REPLCONF capa eof")
        .await
        .context("send replconf command")?;
    let response = read_response(buf, stream, Duration::from_secs(1))
        .await
        .context("read replconf response")?;
    ensure!(
        response.as_bytes() == Some(b"OK"),
        "unexpected response from replconf: {:?}",
        response
    );
    debug!("REPLCONF capa eof successful");

    debug!("Sending REPLCONF rdb-only 1 command...");
    send_command(stream, "REPLCONF rdb-only 1")
        .await
        .context("send replconf rdb-only command")?;
    let response = read_response(buf, stream, Duration::from_secs(1))
        .await
        .context("read replconf rdb-only response")?;
    match response {
        OwnedFrame::SimpleString(s) if s == b"OK" => {
            parser_trace!("replconf.rdb_only.supported");
            debug!(name: "replconf_rdb_only", "Master supports rdb-only option");
        }
        OwnedFrame::Error(s) if s.starts_with("ERR Unrecognized REPLCONF option") => {
            parser_trace!("replconf.rdb_only.unsupported");
            debug!(name: "replconf_rdb_only", "Master does not support rdb-only option (non-critical): {:?}", s);
        }
        _ => anyhow::bail!("unexpected response from replconf rdb-only: {:?}", response),
    }

    debug!("Sending PSYNC ? -1 command...");
    send_command(stream, "PSYNC ? -1")
        .await
        .context("send psync command")?;

    // repl-diskless-sync-delay may cause a long time to wait for response
    debug!("Waiting for PSYNC response (timeout: 30s)...");
    let response = read_response(buf, stream, Duration::from_secs(30))
        .await
        .context("read psync response")?;
    ensure!(
        response
            .as_bytes()
            .map(|b| b.starts_with(b"FULLRESYNC"))
            .unwrap_or(false),
        "unexpected response from psync: {:?}",
        response
    );
    debug!("PSYNC successful, received FULLRESYNC response");

    Ok(())
}

pub async fn read_rdb_mode(buf: &mut RingBuffer, stream: &mut TcpStream) -> AnyResult<RDBMode> {
    loop {
        // Get input slice in a limited scope and calculate consumption
        let (parse_result, bytes_to_consume) = {
            let input = buf.filled();
            match try_read_rdb_mode(input) {
                Ok((remaining_input, mode)) => {
                    let consumed = input.len() - remaining_input.len();
                    (Ok(mode), consumed)
                }
                Err(e) => (Err(e), 0),
            }
        };

        match parse_result {
            Ok(mode) => {
                buf.consume(bytes_to_consume);
                return Ok(mode);
            }
            Err(e) => {
                if e.is::<NeedMoreData>() {
                    // Read more data into buffer with timeout
                    if buf.remaining_capacity() > 0 {
                        buf.read_from(stream)
                            .timeout(Duration::from_secs(10))
                            .await
                            .context("read more data meet timeout")?
                            .context("read more data meet error")?;
                    } else {
                        // Buffer is full, we need to consume some data first
                        // This should not happen in normal parsing, but let's be safe
                        return Err(anyhow!(
                            "buffer full while parsing RDB mode - this indicates a parsing bug"
                        ));
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }
}

fn try_read_rdb_mode(input: &[u8]) -> AnyResult<(&[u8], RDBMode)> {
    let (input, _skipped_whitespace) = read_while(input, |b| b == b'\n' || b == b'\r')?;
    if input.starts_with(b"$EOF:") {
        let input = read_tag(input, b"$EOF:").context("read $EOF:")?;
        let (input, delimiter) = read_exact(input, 40).context("read delimiter")?;
        let input = read_tag(input, b"\r\n").context("read \\r\\n")?;
        let mut output = [0u8; 40];
        output.copy_from_slice(delimiter);
        parser_trace!("rdb.diskless");
        debug!(name: "rdb_mode", mode = "diskless");
        Ok((input, RDBMode::Diskless { delimiter: output }))
    } else {
        let input = read_tag(input, b"$").context("read $")?;
        let (input, len) = read_while(input, |b| b.is_ascii_digit()).context("read length")?;
        let len = str::from_utf8(len)
            .context("rdb length is not a valid UTF-8 string")?
            .parse::<u64>()
            .context("rdb length is not a valid number")?;
        let input = read_tag(input, b"\r\n").context("read \\r\\n")?;
        parser_trace!("rdb.disk");
        debug!(name: "rdb_mode", mode = "disk", len = len);
        Ok((input, RDBMode::Disk { remain: len }))
    }
}

pub async fn send_command(stream: &mut TcpStream, command: &str) -> AnyResult<()> {
    debug!(name: "send_command", command = command);
    let command = resp2_encode_command(command);
    let mut buffer = vec![0u8; command.encode_len(false)];
    let wrote = encode_bytes(&mut buffer, &command, false).context("encode command")?;
    ensure!(wrote == buffer.len(), "mismatch in encoded command length");
    stream.write_all(&buffer).await.context("write command")?;
    Ok(())
}

pub async fn read_response(
    buf: &mut RingBuffer,
    stream: &mut TcpStream,
    timeout: Duration,
) -> AnyResult<OwnedFrame> {
    let deadline = Instant::now() + timeout;
    loop {
        // Get input slice in a limited scope and calculate consumption
        let (decode_result, bytes_to_consume) = {
            let full_input = buf.filled();
            let (input, skipped_whitespace) = read_while(full_input, |b| b == b'\n' || b == b'\r')?;
            match decode(input) {
                Ok(Some((frame, amt))) => {
                    let total_consumed = skipped_whitespace.len() + amt;
                    (Ok(Some(frame)), total_consumed)
                }
                Ok(None) => (Ok(None), 0),
                Err(e) => (Err(e), 0),
            }
        };

        match decode_result? {
            Some(frame) => {
                buf.consume(bytes_to_consume);
                if let Some(frame) = frame.as_str() {
                    debug!(name: "read_response", frame = ?frame);
                } else {
                    debug!(name: "read_response", frame = ?frame);
                }
                return Ok(frame);
            }
            None => {
                let remain_time = deadline.saturating_duration_since(Instant::now());
                if remain_time.is_zero() {
                    return Err(anyhow!("timeout"));
                }

                // Read more data into buffer with timeout
                if buf.remaining_capacity() > 0 {
                    buf.read_from(stream).timeout(remain_time).await??;
                } else {
                    // Buffer is full, we need to consume some data first
                    // This should not happen in normal parsing, but let's be safe
                    return Err(anyhow!(
                        "buffer full while reading response - this indicates a parsing bug"
                    ));
                }
            }
        }
    }
}
