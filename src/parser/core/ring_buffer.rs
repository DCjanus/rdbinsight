use std::fmt::{Debug, Formatter, Result as FmtResult};

use tokio::io::{AsyncRead, AsyncReadExt};

/// A ring buffer implementation with automatic memory compaction.
///
/// This ring buffer maintains a fixed-size internal Vec<u8> and provides
/// efficient unfilled_mut and filled operations. When the data would wrap
/// around the end of the buffer, it automatically compacts the memory
/// to ensure both methods always return contiguous slices.
#[derive(Clone)]
pub struct RingBuffer {
    buff: Vec<u8>,
    head: usize,
    size: usize,
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new(32 * 1024)
    }
}

impl RingBuffer {
    /// Creates a new RingBuffer with the specified capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "RingBuffer capacity must be greater than 0");
        Self {
            buff: vec![0; capacity],
            head: 0,
            size: 0,
        }
    }

    /// Returns the capacity of the ring buffer.
    pub fn capacity(&self) -> usize {
        self.buff.len()
    }

    /// Returns the current length of data in the buffer.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns the remaining capacity.
    pub fn remaining_capacity(&self) -> usize {
        self.capacity() - self.size
    }

    /// Returns a mutable slice view of the unfilled portion of the buffer.
    ///
    /// This method ensures the returned slice is always contiguous by
    /// performing memory compaction when necessary. After writing data to
    /// the returned slice, call `commit(written_bytes)` to update the buffer size.
    pub fn unfilled_mut(&mut self) -> &mut [u8] {
        let remaining = self.remaining_capacity();
        if remaining == 0 {
            return &mut [];
        }

        let capacity = self.capacity();
        let end = (self.head + self.size) % capacity;

        // Check if unfilled area would wrap around
        if end + remaining > capacity {
            // Need to compact to ensure contiguous unfilled area
            // Force compaction by moving data to start from 0
            self.compact(true);
            // After compaction, data starts at 0, so unfilled area is contiguous
            &mut self.buff[self.size..]
        } else {
            // Unfilled area is already contiguous
            &mut self.buff[end..end + remaining]
        }
    }

    /// Returns a slice view of the data in the buffer.
    ///
    /// This method ensures the returned slice is always contiguous by
    /// performing memory compaction when necessary.
    pub fn filled(&mut self) -> &[u8] {
        if self.size == 0 {
            return &[];
        }

        // Check if data wraps around
        if self.head + self.size > self.capacity() {
            self.compact(false);
            // After compaction, data starts at index 0
            &self.buff[..self.size]
        } else {
            // Data is already contiguous
            &self.buff[self.head..self.head + self.size]
        }
    }

    /// Commits the specified number of bytes as written to the buffer.
    ///
    /// This should be called after writing data to the slice returned by `unfilled_mut()`.
    /// The `written_bytes` must not exceed the length of the slice returned by `unfilled_mut()`.
    ///
    /// # Panics
    ///
    /// Panics if `written_bytes` exceeds the remaining capacity of the buffer.
    pub fn commit(&mut self, written_bytes: usize) {
        let remaining = self.remaining_capacity();
        assert!(
            written_bytes <= remaining,
            "Cannot commit {written_bytes} bytes: only {remaining} bytes remaining in buffer",
        );
        self.size += written_bytes;
    }

    /// Consumes the specified number of bytes from the front of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds the current size of the buffer.
    pub fn consume(&mut self, count: usize) {
        assert!(
            count <= self.size,
            "Cannot consume {} bytes: only {} bytes available in buffer",
            count,
            self.size
        );

        self.head = (self.head + count) % self.capacity();
        self.size -= count;

        // Reset start to 0 if buffer becomes empty for efficiency
        if self.size == 0 {
            self.head = 0;
        }
    }

    /// Consumes bytes from the front of the buffer up to the specified pointer.
    ///
    /// This method calculates the number of bytes to consume based on the pointer
    /// offset from the beginning of the current data slice. This is useful for
    /// compatibility with parser APIs that return pointers into the buffer.
    ///
    /// # Panics
    ///
    /// Panics if the pointer is not within the current data range or if the
    /// calculated offset exceeds the current buffer size.
    pub fn consume_to(&mut self, ptr: *const u8) {
        let base_ptr = self.filled().as_ptr() as usize;
        let dest_ptr = ptr as usize;
        assert!(
            dest_ptr >= base_ptr,
            "pointer is before the start of buffer data"
        );

        let bytes_to_consume = dest_ptr - base_ptr;
        let size = self.size();
        assert!(
            bytes_to_consume <= self.size,
            "pointer is beyond buffer data range: trying to consume {bytes_to_consume} bytes but only {size} available",
        );

        self.consume(bytes_to_consume);
    }

    /// Truncates the buffer to the specified length.
    ///
    /// If `len` is greater than or equal to the current size, this has no effect.
    /// If `len` is less than the current size, the buffer is truncated to `len` bytes,
    /// keeping the first `len` bytes and discarding the rest.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.size {
            return;
        }

        self.size = len;

        // Reset head to 0 if buffer becomes empty for efficiency
        if self.size == 0 {
            self.head = 0;
        }
    }

    /// Compacts the buffer to ensure all data is contiguous starting from index 0.
    /// Only performs compaction when data actually wraps around the end of the buffer.
    fn compact(&mut self, force: bool) {
        // Handle empty buffer
        if self.size == 0 {
            self.head = 0;
            return;
        }

        if self.head == 0 {
            // Data already starts at 0
            return;
        }

        // Check if we should skip compaction (only when not forcing)
        if !force && self.head + self.size <= self.capacity() {
            // Data is contiguous and we're not forcing, no need to compact
            return;
        }

        if self.head + self.size <= self.capacity() {
            // Data is contiguous but not at the beginning
            self.buff.copy_within(self.head..self.head + self.size, 0);
        } else {
            // Data wraps around: [head..capacity] + [0..end]
            let end = (self.head + self.size) % self.capacity();

            // Create a temporary buffer to hold the data
            let mut temp = Vec::with_capacity(self.size);
            temp.extend_from_slice(&self.buff[self.head..]);
            temp.extend_from_slice(&self.buff[..end]);

            // Copy back to the beginning of the buffer
            self.buff[..self.size].copy_from_slice(&temp);
        }

        self.head = 0;
    }

    /// Clears the buffer.
    pub fn clear(&mut self) {
        self.head = 0;
        self.size = 0;
    }

    /// Reads data from an AsyncRead source into the ring buffer.
    ///
    /// This method attempts to read data from the provided AsyncRead source
    /// and store it in the ring buffer's unfilled space. It returns the number
    /// of bytes actually read.
    ///
    /// # Returns
    ///
    /// Returns `Ok(0)` if the buffer is full (no space available to read into).
    /// Returns `Ok(n)` where n > 0 if n bytes were successfully read.
    /// Returns `Err(UnexpectedEof)` if the source has reached EOF when there was space to read.
    /// Returns `Err(e)` if an I/O error occurred during reading.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rdbinsight::parser::core::ring_buffer::RingBuffer;
    /// # use tokio::io::AsyncRead;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut buffer = RingBuffer::new(1024);
    /// // source must implement AsyncRead + Unpin
    /// # struct MockReader;
    /// # impl AsyncRead for MockReader {
    /// #     fn poll_read(
    /// #         self: std::pin::Pin<&mut Self>,
    /// #         _cx: &mut std::task::Context<'_>,
    /// #         _buf: &mut tokio::io::ReadBuf<'_>,
    /// #     ) -> std::task::Poll<std::io::Result<()>> {
    /// #         std::task::Poll::Ready(Ok(()))
    /// #     }
    /// # }
    /// # let mut source = MockReader;
    ///
    /// let bytes_read = buffer.read_from(&mut source).await?;
    /// println!("Read {} bytes", bytes_read);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_from<R>(&mut self, reader: &mut R) -> std::io::Result<usize>
    where R: AsyncRead + Unpin {
        let unfilled = self.unfilled_mut();
        if unfilled.is_empty() {
            // Buffer is full, return 0 to indicate no bytes were read
            return Ok(0);
        }

        let bytes_read = reader.read(unfilled).await?;
        if bytes_read == 0 {
            // Reader reached EOF when we had space to read - this is unexpected
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading from source",
            ));
        }

        self.commit(bytes_read);
        Ok(bytes_read)
    }
}

impl Debug for RingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.size == 0 {
            return write!(f, "b\"\"");
        }

        write!(f, "b\"")?;

        // Handle the case where data might wrap around
        if self.head + self.size <= self.capacity() {
            // Data is contiguous
            let data = &self.buff[self.head..self.head + self.size];
            format_bytes(f, data)?;
        } else {
            // Data wraps around: [head..capacity] + [0..end]
            let end = (self.head + self.size) % self.capacity();
            let first_part = &self.buff[self.head..];
            let second_part = &self.buff[..end];

            format_bytes(f, first_part)?;
            format_bytes(f, second_part)?;
        }

        write!(f, "\"")?;
        Ok(())
    }
}

/// Format bytes slice similar to bytes::Bytes Debug implementation
fn format_bytes(f: &mut Formatter<'_>, bytes: &[u8]) -> FmtResult {
    for &b in bytes {
        // Based on bytes crate implementation
        // https://doc.rust-lang.org/reference/tokens.html#byte-escapes
        if b == b'\n' {
            write!(f, "\\n")?;
        } else if b == b'\r' {
            write!(f, "\\r")?;
        } else if b == b'\t' {
            write!(f, "\\t")?;
        } else if b == b'\\' || b == b'"' {
            write!(f, "\\{}", b as char)?;
        } else if b == b'\0' {
            write!(f, "\\0")?;
        // ASCII printable
        } else if (0x20..0x7f).contains(&b) {
            write!(f, "{}", b as char)?;
        } else {
            write!(f, "\\x{b:02x}")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ring_buffer() {
        let rb = RingBuffer::new(10);
        assert_eq!(rb.capacity(), 10);
        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
        assert_eq!(rb.remaining_capacity(), 10);
    }

    #[test]
    fn test_unfilled_mut_simple() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        assert_eq!(unfilled.len(), 10);
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.remaining_capacity(), 5);
        assert_eq!(rb.filled(), b"hello");
    }

    #[test]
    fn test_unfilled_mut_multiple() {
        let mut rb = RingBuffer::new(10);

        // First write
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        // Second write
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"world");
        rb.commit(5);

        assert_eq!(rb.size(), 10);
        assert_eq!(rb.remaining_capacity(), 0);
        assert_eq!(rb.filled(), b"helloworld");
    }

    #[test]
    fn test_unfilled_mut_full_buffer() {
        let mut rb = RingBuffer::new(5);
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"hello");
        rb.commit(5);

        // Buffer is full, should return empty slice
        let unfilled = rb.unfilled_mut();
        assert_eq!(unfilled.len(), 0);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");
    }

    #[test]
    fn test_consume() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..10].copy_from_slice(b"helloworld");
        rb.commit(10);

        rb.consume(5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"world");

        rb.consume(3);
        assert_eq!(rb.size(), 2);
        assert_eq!(rb.filled(), b"ld");

        rb.consume(2); // Consume remaining data
        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
    }

    #[test]
    fn test_wraparound_and_compaction() {
        let mut rb = RingBuffer::new(8);

        // Fill the buffer
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"12345678");
        rb.commit(8);
        assert_eq!(rb.filled(), b"12345678");

        // Consume some data
        rb.consume(3);
        assert_eq!(rb.filled(), b"45678");

        // Add more data that would wrap around
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"ABC");
        rb.commit(3);
        assert_eq!(rb.size(), 8);

        // filled should trigger compaction and return contiguous data
        assert_eq!(rb.filled(), b"45678ABC");
    }

    #[test]
    fn test_clear() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);
        rb.clear();
        assert!(rb.is_empty());
        assert_eq!(rb.size(), 0);
        assert_eq!(rb.remaining_capacity(), 10);
    }

    #[test]
    fn test_commit_zero_bytes() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        assert_eq!(unfilled.len(), 10);
        rb.commit(0); // Commit zero bytes
        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
    }

    #[test]
    #[should_panic(expected = "Cannot commit 6 bytes: only 5 bytes remaining in buffer")]
    fn test_commit_too_many_bytes_panics() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        // Now only 5 bytes remaining, trying to commit 6 should panic
        rb.commit(6);
    }

    #[test]
    #[should_panic(expected = "RingBuffer capacity must be greater than 0")]
    fn test_zero_capacity_panics() {
        RingBuffer::new(0);
    }

    #[test]
    #[should_panic(expected = "Cannot consume 10 bytes: only 5 bytes available in buffer")]
    fn test_consume_too_many_bytes_panics() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        // Try to consume more than available, should panic
        rb.consume(10);
    }

    #[test]
    fn test_complex_wraparound_scenario() {
        let mut rb = RingBuffer::new(6);

        // Add initial data
        let unfilled = rb.unfilled_mut();
        unfilled[..4].copy_from_slice(b"ABCD");
        rb.commit(4);
        assert_eq!(rb.filled(), b"ABCD");

        // Consume some
        rb.consume(2);
        assert_eq!(rb.filled(), b"CD");

        // Add more data that wraps
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"EFGH");
        rb.commit(4);
        assert_eq!(rb.size(), 6);

        // Should trigger compaction
        assert_eq!(rb.filled(), b"CDEFGH");

        // Consume and add again
        rb.consume(4);
        assert_eq!(rb.filled(), b"GH");

        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"IJKL");
        rb.commit(4);
        assert_eq!(rb.filled(), b"GHIJKL");
    }

    #[test]
    fn test_truncate() {
        let mut rb = RingBuffer::new(10);

        // Add some data
        let unfilled = rb.unfilled_mut();
        unfilled[..8].copy_from_slice(b"helloworld"[..8].try_into().unwrap());
        rb.commit(8);
        assert_eq!(rb.filled(), b"hellowor");
        assert_eq!(rb.size(), 8);

        // Truncate to smaller size
        rb.truncate(5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");
        assert_eq!(rb.remaining_capacity(), 5);

        // Truncate to same size (should have no effect)
        rb.truncate(5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");

        // Truncate to larger size (should have no effect)
        rb.truncate(10);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");

        // Truncate to zero
        rb.truncate(0);
        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
        assert_eq!(rb.head, 0); // Should reset head to 0
    }

    #[test]
    fn test_truncate_with_wraparound() {
        let mut rb = RingBuffer::new(6);

        // Fill buffer
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"123456");
        rb.commit(6);
        assert_eq!(rb.filled(), b"123456");

        // Consume some to create wraparound scenario
        rb.consume(2);
        assert_eq!(rb.filled(), b"3456");

        // Add more data to wrap around
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"AB");
        rb.commit(2);
        assert_eq!(rb.size(), 6);
        assert_eq!(rb.filled(), b"3456AB");

        // Truncate in wrapped state
        rb.truncate(3);
        assert_eq!(rb.size(), 3);
        assert_eq!(rb.filled(), b"345");
    }

    #[test]
    fn test_consume_to() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..8].copy_from_slice(b"helloworld"[..8].try_into().unwrap());
        rb.commit(8);

        let data = rb.filled();
        assert_eq!(data, b"hellowor");

        // Consume to pointer at position 5 (after "hello")
        let ptr = unsafe { data.as_ptr().add(5) };
        rb.consume_to(ptr);

        assert_eq!(rb.size(), 3);
        assert_eq!(rb.filled(), b"wor");
    }

    #[test]
    fn test_consume_to_beginning() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        let data = rb.filled();
        let ptr = data.as_ptr(); // Consume 0 bytes
        rb.consume_to(ptr);

        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");
    }

    #[test]
    fn test_consume_to_end() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        let data = rb.filled();
        let ptr = unsafe { data.as_ptr().add(data.len()) }; // Consume all bytes
        rb.consume_to(ptr);

        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
    }

    #[test]
    #[should_panic(expected = "pointer is beyond buffer data range")]
    fn test_consume_to_beyond_range() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        let data = rb.filled();
        let ptr = unsafe { data.as_ptr().add(data.len() + 1) }; // Beyond buffer
        rb.consume_to(ptr);
    }

    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::ReadBuf;

    // Simple mock AsyncRead for testing
    struct MockAsyncReader {
        data: Vec<u8>,
        position: usize,
    }

    impl MockAsyncReader {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                position: 0,
            }
        }
    }

    impl AsyncRead for MockAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let remaining = self.data.len() - self.position;
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let to_read = remaining.min(buf.remaining());
            let end_pos = self.position + to_read;
            buf.put_slice(&self.data[self.position..end_pos]);
            self.position = end_pos;

            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_read_from_basic() {
        let mut rb = RingBuffer::new(10);
        let mut source = MockAsyncReader::new(b"hello");

        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"hello");
    }

    #[tokio::test]
    async fn test_read_from_multiple_reads() {
        let mut rb = RingBuffer::new(10);
        let mut source = MockAsyncReader::new(b"hello world");

        // First read
        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 10); // Buffer capacity is 10
        assert_eq!(rb.size(), 10);
        assert_eq!(rb.filled(), b"hello worl");

        // Buffer is full, should return 0
        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 0);
        assert_eq!(rb.size(), 10);
    }

    #[tokio::test]
    async fn test_read_from_with_consume() {
        let mut rb = RingBuffer::new(8);
        let mut source = MockAsyncReader::new(b"hello world test");

        // First read fills buffer
        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(rb.filled(), b"hello wo");

        // Consume some data
        rb.consume(5);
        assert_eq!(rb.filled(), b" wo");

        // Read more data
        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(rb.size(), 8);
        assert_eq!(rb.filled(), b" world t");
    }

    #[tokio::test]
    async fn test_read_from_empty_source() {
        let mut rb = RingBuffer::new(10);
        let mut source = MockAsyncReader::new(b"");

        let result = rb.read_from(&mut source).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(rb.size(), 0);
        assert!(rb.is_empty());
    }

    #[tokio::test]
    async fn test_read_from_partial_read() {
        let mut rb = RingBuffer::new(20);
        let mut source = MockAsyncReader::new(b"short");

        let bytes_read = rb.read_from(&mut source).await.unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(rb.size(), 5);
        assert_eq!(rb.filled(), b"short");
        assert_eq!(rb.remaining_capacity(), 15);
    }

    #[test]
    fn test_debug_empty_buffer() {
        let rb = RingBuffer::new(10);
        let debug_str = format!("{:?}", rb);
        assert_eq!(debug_str, "b\"\"");
    }

    #[test]
    fn test_debug_ascii_data() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..5].copy_from_slice(b"hello");
        rb.commit(5);

        let debug_str = format!("{:?}", rb);
        assert_eq!(debug_str, "b\"hello\"");
    }

    #[test]
    fn test_debug_binary_data() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..4].copy_from_slice(&[0x00, 0x01, 0xff, 0x7f]);
        rb.commit(4);

        let debug_str = format!("{:?}", rb);
        assert_eq!(debug_str, "b\"\\0\\x01\\xff\\x7f\"");
    }

    #[test]
    fn test_debug_mixed_data() {
        let mut rb = RingBuffer::new(10);
        let unfilled = rb.unfilled_mut();
        unfilled[..6].copy_from_slice(b"hi\n\t\0\xff");
        rb.commit(6);

        let debug_str = format!("{:?}", rb);
        assert_eq!(debug_str, "b\"hi\\n\\t\\0\\xff\"");
    }

    #[test]
    fn test_debug_wraparound_data() {
        let mut rb = RingBuffer::new(6);

        // Fill buffer completely
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"123456");
        rb.commit(6);

        // Consume some data to move head
        rb.consume(2);
        assert_eq!(rb.head, 2);
        assert_eq!(rb.size, 4);

        // Add data that will wrap around
        let unfilled = rb.unfilled_mut();
        unfilled.copy_from_slice(b"AB");
        rb.commit(2);

        // Now we have wraparound: head=2, size=6, data should be "3456AB"
        let debug_str = format!("{:?}", rb);
        assert_eq!(debug_str, "b\"3456AB\"");
    }
}
