use anyhow::ensure;

use crate::{
    helper::AnyResult,
    parser::core::{
        buffer::Buffer,
        combinators::{read_exact, read_u8},
    },
};

const RING_SIZE: usize = 8 * 1024;

#[derive(Debug, Clone, Default)]
pub struct LzfChunkDecoder {
    buff: Vec<u8>,
    tail: usize,
}

impl LzfChunkDecoder {
    pub fn feed(&mut self, i_buf: &mut Buffer, o_buf: &mut Buffer) -> AnyResult {
        let input = i_buf.as_slice();
        let (input, action) = Self::read_action(input)?;

        ensure!(
            o_buf.remain_capacity() >= action.length as usize,
            "output buffer overflow"
        );

        if action.offset == 0 {
            let (input, literal) = read_exact(input, action.length as usize)?;
            o_buf
                .extend(literal)
                .expect("output buffer overflow when extend");
            literal.iter().copied().for_each(|b| self.push_u8(b));
            i_buf.consume_to(input.as_ptr());
            return Ok(());
        }

        // Byte-by-byte copy to support overlapping buffers
        for _ in 0..action.length {
            let byte = self.read_from_tail(action.offset);
            o_buf
                .push_u8(byte)
                .expect("output buffer overflow when push u8");
            self.push_u8(byte);
        }
        i_buf.consume_to(input.as_ptr());
        Ok(())
    }

    fn read_action(input: &[u8]) -> AnyResult<(&[u8], Action)> {
        let (input, ctrl) = read_u8(input)?;
        if ctrl < 32 {
            // Literal run: ctrl stores length-1
            let length = ctrl as u16 + 1;
            return Ok((input, Action { offset: 0, length }));
        }

        let l_part = ctrl >> 5;

        let (input, length) = if l_part < 7 {
            (input, l_part as u16 + 2)
        } else {
            let (input, next_byte) = read_u8(input)?;
            let length = 7 + 2 + next_byte as u16;
            (input, length)
        };

        let o_part = ctrl & 0b0001_1111;
        let (input, next_byte) = read_u8(input)?;
        let offset = u16::from_be_bytes([o_part, next_byte]) + 1;

        Ok((input, Action { offset, length }))
    }

    fn push_u8(&mut self, byte: u8) {
        if self.tail < self.buff.len() {
            self.buff[self.tail] = byte;
        } else {
            self.buff.push(byte);
        }
        self.tail = (self.tail + 1) % RING_SIZE;
    }

    fn read_from_tail(&self, offset: u16) -> u8 {
        self.buff[(self.tail + RING_SIZE - offset as usize) % RING_SIZE]
    }
}

struct Action {
    offset: u16, // zero means literal
    length: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{core::buffer::Buffer, error::NeedMoreData};

    #[test]
    fn test_lzf_chunk_decoder_roundtrip() {
        let original = b"Hello world, Hello world, Hello world! Hello world!";
        // Compress using external lzf crate
        let compressed = lzf::compress(original).expect("compression failed");
        // Initialize decoder and buffers
        let mut decoder = LzfChunkDecoder::default();
        let mut i_buf = Buffer::new(compressed.len());
        let mut o_buf = Buffer::new(original.len());
        // Feed all compressed data into input buffer
        i_buf.extend(&compressed).expect("buffer extend failed");
        // Decode until input buffer is empty
        while i_buf.len() > 0 {
            decoder
                .feed(&mut i_buf, &mut o_buf)
                .expect("decoding failed");
        }
        // Verify output matches original
        assert_eq!(o_buf.as_slice(), original);
    }

    #[test]
    fn test_lzf_chunk_decoder_feed_partial() {
        let original = b"Repeatable test pattern Repeatable test pattern! Repeatable!";
        let compressed = lzf::compress(original).expect("compression failed");
        let split = compressed.len() / 2;
        let mut decoder = LzfChunkDecoder::default();
        let mut i_buf = Buffer::new(compressed.len());
        let mut o_buf = Buffer::new(original.len());
        // Extend only the first half of the compressed data
        i_buf
            .extend(&compressed[..split])
            .expect("buffer extend failed");
        // Expect a NotFinished error due to incomplete input
        let err = decoder.feed(&mut i_buf, &mut o_buf).unwrap_err();
        assert!(err.is::<NeedMoreData>());
        // Extend the rest of the compressed data
        i_buf
            .extend(&compressed[split..])
            .expect("buffer extend failed");
        // Continue decoding until complete
        while i_buf.len() > 0 {
            decoder
                .feed(&mut i_buf, &mut o_buf)
                .expect("decoding failed");
        }
        assert_eq!(o_buf.as_slice(), original);
    }

    #[test]
    fn test_lzf_chunk_decoder_ctrl_across_feed_boundary() {
        let original = b"Hello world, Hello world, Hello world! Hello world!";
        // Compress using external lzf crate
        let compressed = lzf::compress(original).expect("compression failed");
        let split = 1;
        let mut decoder = LzfChunkDecoder::default();
        let mut i_buf = Buffer::new(compressed.len());
        let mut o_buf = Buffer::new(original.len());
        // Extend only the control byte
        i_buf
            .extend(&compressed[..split])
            .expect("buffer extend failed");
        // Expect a NotFinished error due to incomplete literal data
        let err = decoder.feed(&mut i_buf, &mut o_buf).unwrap_err();
        assert!(err.is::<NeedMoreData>());
        // Extend the rest of the compressed data
        i_buf
            .extend(&compressed[split..])
            .expect("buffer extend failed");
        // Continue decoding until complete
        while i_buf.len() > 0 {
            decoder
                .feed(&mut i_buf, &mut o_buf)
                .expect("decoding failed");
        }
        assert_eq!(o_buf.as_slice(), original);
    }

    #[test]
    fn test_lzf_chunk_decoder_output_buffer_overflow() {
        let original = b"Hello world, Hello world, Hello world! Hello world!";
        let compressed = lzf::compress(original).expect("compression failed");
        let mut decoder = LzfChunkDecoder::default();
        let mut i_buf = Buffer::new(compressed.len());
        // Use a very small output buffer to trigger overflow
        let mut o_buf = Buffer::new(1);
        i_buf.extend(&compressed).expect("buffer extend failed");
        // Expect an output buffer overflow error (not NotFinished)
        let err = decoder.feed(&mut i_buf, &mut o_buf).unwrap_err();
        assert!(!err.is::<NeedMoreData>());
        assert!(err.to_string().contains("output buffer overflow"));
    }

    #[test]
    fn test_lzf_chunk_decoder_feed_empty() {
        let mut decoder = LzfChunkDecoder::default();
        let mut i_buf = Buffer::new(10);
        let mut o_buf = Buffer::new(10);
        // Feeding with empty input should return NotFinished
        let err = decoder.feed(&mut i_buf, &mut o_buf).unwrap_err();
        assert!(err.is::<NeedMoreData>());
    }
}
