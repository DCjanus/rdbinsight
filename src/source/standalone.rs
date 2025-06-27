use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
    vec,
};

use anyhow::{Context as AnyhowContext, anyhow, ensure};
use async_async_io::read::{AsyncAsyncRead, PollRead};
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
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpStream, tcp::OwnedReadHalf},
};
use tokio_util::time::FutureExt;
use tracing::debug;

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
    source::RdbSourceConfig,
};

pub struct Config {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl RdbSourceConfig for Config {
    async fn get_rdb_stream(&self) -> AnyResult<Pin<Box<dyn AsyncRead + Send>>> {
        let source = StandaloneSource::init(self).await?;
        Ok(Box::pin(source))
    }
}

/// Delimiter-based reader for diskless mode
struct DelimiterReader {
    inner: OwnedReadHalf,
    delimiter: [u8; 40],
    buff: RingBuffer,
    done: bool,
}

impl DelimiterReader {
    fn new(inner: OwnedReadHalf, delimiter: [u8; 40], buff: RingBuffer) -> Self {
        Self {
            inner,
            delimiter,
            buff,
            done: false,
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

        let search_start = self
            .buff
            .size()
            .saturating_sub(n + self.delimiter.len() - 1);
        let data = self.buff.filled();
        let search_zone = &data[search_start..];

        if let Some(relative_pos) = memmem::find(search_zone, &self.delimiter) {
            self.done = true;
            // Truncate to remove the delimiter and data after it
            let delimiter_pos = search_start + relative_pos;
            self.buff.truncate(delimiter_pos);
        }

        Ok(())
    }

    fn safe_zone(&mut self) -> &[u8] {
        let data = self.buff.filled();
        if self.done {
            return data;
        }

        // Return data excluding potential partial delimiter at the end
        &data[..data.len().saturating_sub(self.delimiter.len() - 1)]
    }
}

impl AsyncAsyncRead for DelimiterReader {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.done && self.buff.is_empty() {
            return Ok(0);
        }

        // Ensure we have enough data or feed more
        let safe_zone = self.safe_zone();
        if safe_zone.len() < self.delimiter.len() && !self.done {
            self.feed_more().await.map_err(std::io::Error::other)?;
        }

        let safe_zone = self.safe_zone();
        let max_copy = safe_zone.len().min(buf.len());

        if max_copy > 0 {
            buf[..max_copy].copy_from_slice(&safe_zone[..max_copy]);
            self.buff.consume(max_copy);
        }

        Ok(max_copy)
    }
}

struct LimitedReader {
    inner: OwnedReadHalf,
    remaining: u64,
    buffer: RingBuffer,
}

impl LimitedReader {
    fn new(inner: OwnedReadHalf, limit: u64, buffer: RingBuffer) -> Self {
        Self {
            inner,
            remaining: limit,
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

/// Refactored reader enum
enum StandaloneReader {
    Disk(PollRead<LimitedReader>),
    Diskless(PollRead<DelimiterReader>),
}

impl AsyncRead for StandaloneReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            StandaloneReader::Disk(reader) => Pin::new(reader).poll_read(cx, buf),
            StandaloneReader::Diskless(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

struct StandaloneSource {
    reader: StandaloneReader,
}

enum RDBMode {
    Diskless { delimiter: [u8; 40] },
    Disk { remain: u64 },
}

impl StandaloneSource {
    pub async fn init(config: &Config) -> AnyResult<Self> {
        let stream = TcpStream::connect(&config.address).await?;

        let (mut rx, mut tx) = stream.into_split();
        let mut buffer = RingBuffer::default();
        Self::prepare(&mut buffer, &mut rx, &mut tx, config).await?;
        let mode = Self::read_rdb_mode(&mut buffer, &mut rx, Duration::from_secs(1)).await?;

        let reader = match mode {
            RDBMode::Disk { remain } => {
                let async_reader = LimitedReader::new(rx, remain, buffer);
                StandaloneReader::Disk(PollRead::new(async_reader))
            }
            RDBMode::Diskless { delimiter } => {
                let async_reader = DelimiterReader::new(rx, delimiter, buffer);
                StandaloneReader::Diskless(PollRead::new(async_reader))
            }
        };

        Ok(Self { reader })
    }

    async fn read_rdb_mode(
        buf: &mut RingBuffer,
        rx: &mut (impl AsyncRead + Unpin),
        timeout: Duration,
    ) -> AnyResult<RDBMode> {
        let deadline = Instant::now() + timeout;
        loop {
            // Get input slice in a limited scope and calculate consumption
            let (parse_result, bytes_to_consume) = {
                let input = buf.filled();
                match Self::try_read_rdb_mode(input) {
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
                        let remain_time = deadline.saturating_duration_since(Instant::now());
                        if remain_time.is_zero() {
                            return Err(anyhow!("timeout"));
                        }
                        // Read more data into buffer with timeout
                        if buf.remaining_capacity() > 0 {
                            buf.read_from(rx).timeout(remain_time).await??;
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
        if input.starts_with(b"$EOF:") {
            let input = read_tag(input, b"$EOF:")?;
            let (input, delimiter) = read_exact(input, 40)?;
            let input = read_tag(input, b"\r\n")?;
            let mut output = [0u8; 40];
            output.copy_from_slice(delimiter);
            parser_trace!("rdb.diskless");
            Ok((input, RDBMode::Diskless { delimiter: output }))
        } else {
            let input = read_tag(input, b"$")?;
            let (input, len) = read_while(input, |b| b.is_ascii_digit())?;
            let len = str::from_utf8(len)
                .context("rdb length is not a valid UTF-8 string")?
                .parse::<u64>()
                .context("rdb length is not a valid number")?;
            let input = read_tag(input, b"\r\n")?;
            parser_trace!("rdb.disk");
            Ok((input, RDBMode::Disk { remain: len }))
        }
    }

    async fn prepare(
        buf: &mut RingBuffer,
        rx: &mut (impl AsyncRead + Unpin),
        tx: &mut (impl AsyncWrite + Unpin),
        config: &Config,
    ) -> AnyResult<()> {
        if let Some(password) = &config.password {
            let mut parts = vec!["AUTH"];
            if let Some(username) = &config.username {
                parser_trace!("auth.with_username");
                parts.push(username);
            } else {
                parser_trace!("auth.password_only");
            }
            parts.push(password);
            let command = parts.join(" ");
            Self::send_command(tx, &command)
                .await
                .context("send auth command")?;
            let response = Self::read_response(buf, rx, Duration::from_secs(1))
                .await
                .context("read auth response")?;
            ensure!(
                response.as_bytes() == Some(b"OK"),
                "unexpected response from auth: {:?}",
                response
            );
        }

        Self::send_command(tx, "PING")
            .await
            .context("send ping command")?;
        let response = Self::read_response(buf, rx, Duration::from_secs(1))
            .await
            .context("read ping response")?;
        ensure!(
            response.as_bytes() == Some(b"PONG"),
            "unexpected response from ping: {:?}",
            response
        );

        Self::send_command(tx, "REPLCONF capa eof")
            .await
            .context("send replconf command")?;
        let response = Self::read_response(buf, rx, Duration::from_secs(1))
            .await
            .context("read replconf response")?;
        ensure!(
            response.as_bytes() == Some(b"OK"),
            "unexpected response from replconf: {:?}",
            response
        );

        Self::send_command(tx, "PSYNC ? -1")
            .await
            .context("send psync command")?;

        // repl-diskless-sync-delay may cause a long time to wait for response
        let response = Self::read_response(buf, rx, Duration::from_secs(30))
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

        Ok(())
    }

    async fn send_command(tx: &mut (impl AsyncWrite + Unpin), command: &str) -> AnyResult<()> {
        debug!(name: "send_command", command = command);
        let command = resp2_encode_command(command);
        let mut buffer = vec![0u8; command.encode_len(false)];
        let wrote = encode_bytes(&mut buffer, &command, false).context("encode command")?;
        ensure!(wrote == buffer.len(), "mismatch in encoded command length");
        tx.write_all(&buffer).await.context("write command")?;
        Ok(())
    }

    async fn read_response(
        buf: &mut RingBuffer,
        rx: &mut (impl AsyncRead + Unpin),
        timeout: Duration,
    ) -> AnyResult<OwnedFrame> {
        let deadline = Instant::now() + timeout;
        loop {
            // Get input slice in a limited scope and calculate consumption
            let (decode_result, bytes_to_consume) = {
                let full_input = buf.filled();
                let (input, skipped_whitespace) =
                    read_while(full_input, |b| b == b'\n' || b == b'\r')?;
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
                    debug!(name: "read_response", frame = ?frame);
                    return Ok(frame);
                }
                None => {
                    let remain_time = deadline.saturating_duration_since(Instant::now());
                    if remain_time.is_zero() {
                        return Err(anyhow!("timeout"));
                    }

                    // Read more data into buffer with timeout
                    if buf.remaining_capacity() > 0 {
                        buf.read_from(rx).timeout(remain_time).await??;
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
}

impl AsyncRead for StandaloneSource {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}
