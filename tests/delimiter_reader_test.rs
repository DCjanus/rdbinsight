#[cfg(test)]
mod delimiter_reader_tests {
    use async_async_io::read::AsyncAsyncRead;
    use rdbinsight::{
        parser::core::ring_buffer::RingBuffer, source::redis_stream::DelimiterReader,
    };
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    };

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter("debug")
            .try_init();
    }

    /// Test that DelimiterReader properly detects EOF without delimiter
    #[tokio::test]
    async fn test_delimiter_reader_eof_without_delimiter() {
        init_tracing();

        // Create a test TCP connection
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn a server that sends data without the delimiter and then closes
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Send some data that looks like RDB content but without the delimiter
            let test_data = b"REDIS0009\xff\x00\x00\x00\x00\x00\x00\x00\x00"; // Mock RDB with EOF
            socket.write_all(test_data).await.unwrap();

            // Close the connection without sending the delimiter
            socket.shutdown().await.unwrap();
        });

        // Connect as client
        let stream = TcpStream::connect(addr).await.unwrap();
        let delimiter = *b"abcdef1234567890abcdef1234567890abcdef12"; // 40 bytes
        let buffer = RingBuffer::default();

        let mut delimiter_reader = DelimiterReader::new(stream, delimiter, buffer);

        // Try to read and expect an error about missing delimiter
        let mut read_buf = vec![0u8; 1024];
        let mut total_read = 0;
        let mut error_encountered = false;

        loop {
            println!("About to call delimiter_reader.read()...");
            match delimiter_reader.read(&mut read_buf[total_read..]).await {
                Ok(0) => {
                    println!("Got EOF (0 bytes read), total read: {}", total_read);
                    break; // EOF
                }
                Ok(n) => {
                    println!("Read {} bytes, total: {}", n, total_read + n);
                    total_read += n;
                }
                Err(e) => {
                    // This is what we expect - an error about missing delimiter
                    let error_msg = e.to_string();
                    println!("Got expected error: {}", error_msg);
                    assert!(
                        error_msg.contains("TCP stream ended without finding expected delimiter")
                            || error_msg.contains(
                                "TCP connection ended (EOF) without finding expected delimiter"
                            )
                            || error_msg.contains("DelimiterReader feed_more failed"),
                        "Expected delimiter error, got: {}",
                        error_msg
                    );
                    error_encountered = true;
                    break;
                }
            }

            // Prevent infinite loop
            if total_read > 1000 {
                break;
            }
        }

        server_handle.await.unwrap();

        if !error_encountered {
            panic!(
                "Expected DelimiterReader to report an error about missing delimiter, but it completed successfully with {} bytes read",
                total_read
            );
        }

        println!("Test passed: DelimiterReader correctly detected missing delimiter");
    }

    /// Test that DelimiterReader works correctly when delimiter is found
    #[tokio::test]
    async fn test_delimiter_reader_with_delimiter() {
        init_tracing();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let delimiter = *b"abcdef1234567890abcdef1234567890abcdef12"; // 40 bytes

        // Spawn a server that sends data with the delimiter
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Send some data, then the delimiter, then more data (which should be ignored)
            let test_data = b"REDIS0009\xff\x00\x00\x00\x00\x00\x00\x00\x00";
            socket.write_all(test_data).await.unwrap();
            socket.write_all(&delimiter).await.unwrap();
            socket.write_all(b"this should not be read").await.unwrap();

            socket.shutdown().await.unwrap();
        });

        // Connect as client
        let stream = TcpStream::connect(addr).await.unwrap();
        let buffer = RingBuffer::default();

        let mut delimiter_reader = DelimiterReader::new(stream, delimiter, buffer);

        // Read all data up to the delimiter
        let mut read_buf = vec![0u8; 1024];
        let mut total_read = 0;

        loop {
            match delimiter_reader.read(&mut read_buf[total_read..]).await {
                Ok(0) => break, // EOF
                Ok(n) => total_read += n,
                Err(e) => panic!("Unexpected error: {}", e),
            }
        }

        server_handle.await.unwrap();

        // Verify we only read the data before the delimiter
        let received_data = &read_buf[..total_read];
        assert_eq!(
            received_data,
            b"REDIS0009\xff\x00\x00\x00\x00\x00\x00\x00\x00"
        );

        // Check that the extra data after delimiter was not included
        let extra_data = b"this should not be read";
        let mut found_extra = false;
        for window in received_data.windows(extra_data.len()) {
            if window == extra_data {
                found_extra = true;
                break;
            }
        }
        assert!(
            !found_extra,
            "Extra data after delimiter should not have been read"
        );
    }
}
