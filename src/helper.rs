pub type AnyResult<T = ()> = anyhow::Result<T>;

pub mod proxy_connector;
pub mod sort_merge;

use crc::{CRC_16_XMODEM, CRC_32_ISO_HDLC, Crc};
pub use sort_merge::SortMergeIterator;

pub fn wrapping_to_usize(value: u64) -> usize {
    value.try_into().unwrap_or(usize::MAX)
}

/// Sanitize a URL by removing sensitive information like usernames and passwords.
/// Returns a safe string representation suitable for logging.
pub fn sanitize_url(url_str: &str) -> String {
    let mut url = match url::Url::parse(url_str) {
        Ok(url) => url,
        Err(_) => return "<invalid-url>".to_string(),
    };
    if url.password().is_some() {
        url.set_password(Some("***"))
            .expect("Failed to set password");
    }
    url.to_string()
}

/// Format numbers into human-readable format
/// Uses units K, M, B, T where B represents billion (10^9)
/// Handles special f64 values: NaN, infinity, and negative numbers
pub fn format_number(num: f64) -> String {
    // Handle special f64 values
    if num.is_nan() {
        return "NaN".to_string();
    }
    if num.is_infinite() {
        return if num.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    if num == 0.0 {
        return "0".to_string();
    }

    // Handle negative numbers by formatting absolute value and adding sign back
    let is_negative = num < 0.0;
    let abs_num = num.abs();

    let mut suffix = "";
    let mut divisor = 1.0;

    // Define units, using B as billion (10^9)
    let units = [
        (1_000_000_000_000.0, "T"), // trillion
        (1_000_000_000.0, "B"),     // billion (not G)
        (1_000_000.0, "M"),         // million
        (1_000.0, "K"),             // thousand
    ];

    for (threshold, unit) in &units {
        if abs_num >= *threshold {
            suffix = unit;
            divisor = *threshold;
            break;
        }
    }

    if divisor == 1.0 {
        let result = format!("{}", abs_num as u64);
        return if is_negative {
            format!("-{}", result)
        } else {
            result
        };
    }

    // Use integer arithmetic to avoid floating point precision issues
    let scaled = (abs_num * 100.0) / divisor;
    let integer_part = (scaled / 100.0) as u64;
    let decimal_part = (scaled % 100.0) as u32;

    let formatted_str = format!("{}.{:02}{}", integer_part, decimal_part, suffix);

    if is_negative {
        format!("-{}", formatted_str)
    } else {
        formatted_str
    }
}

/// Format byte sizes into human-readable SI format using 1024 base
/// Units: B, KB, MB, GB, TB, PB
pub fn format_bytesize(bytes: u64) -> String {
    if bytes == 0 {
        return "0B".to_string();
    }

    let mut value = bytes as f64;
    let units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut idx = 0usize;

    while value >= 1024.0 && idx < units.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }

    // Keep two decimal places like format_number
    if idx == 0 {
        // Bytes - show as integer
        format!("{}{}", value as u64, units[idx])
    } else {
        format!("{:.2}{}", value, units[idx])
    }
}

/// Calculate Codis slot for a given key
pub fn codis_slot(key: &[u8]) -> u16 {
    let key = key
        .iter()
        .position(|&c| c == b'{')
        .and_then(|start| {
            let end = key.iter().position(|&c| c == b'}')?;
            Some(&key[start + 1..end])
        })
        .unwrap_or(key);

    let checksum = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(key);
    (checksum % 1024) as u16
}

/// Calculate Redis Cluster slot for a given key
pub fn redis_slot(key: &[u8]) -> u16 {
    let effective_key = key
        .iter()
        .position(|&c| c == b'{')
        .and_then(|start| {
            let end = key.iter().position(|&c| c == b'}')?;
            let tag = &key[start + 1..end];
            // If hashtag is empty, use the whole key for Redis Cluster
            if tag.is_empty() { None } else { Some(tag) }
        })
        .unwrap_or(key);
    let checksum = Crc::<u16>::new(&CRC_16_XMODEM).checksum(effective_key);
    checksum % 16384
}

#[macro_export]
macro_rules! impl_serde_str_conversion {
    ($ty:ty) => {
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.serialize(f)
            }
        }

        impl std::str::FromStr for $ty {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let deserializer =
                    serde::de::IntoDeserializer::<'_, serde::de::value::Error>::into_deserializer(
                        s,
                    );
                let value = <$ty>::deserialize(deserializer)?;
                Ok(value)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number_special_values() {
        // Test NaN
        assert_eq!(format_number(f64::NAN), "NaN");

        // Test positive infinity
        assert_eq!(format_number(f64::INFINITY), "inf");

        // Test negative infinity
        assert_eq!(format_number(f64::NEG_INFINITY), "-inf");

        // Test zero
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(-0.0), "0");
    }

    #[test]
    fn test_format_number_small_numbers() {
        // Test numbers less than 1000
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(1.0), "1");
        assert_eq!(format_number(999.0), "999");

        // Test negative small numbers
        assert_eq!(format_number(-1.0), "-1");
        assert_eq!(format_number(-999.0), "-999");
    }

    #[test]
    fn test_format_number_thousand_unit() {
        // Test K unit (1000 - 999,999)
        assert_eq!(format_number(1000.0), "1.00K");
        assert_eq!(format_number(1500.0), "1.50K");
        assert_eq!(format_number(999999.0), "999.99K");

        // Test negative K unit
        assert_eq!(format_number(-1000.0), "-1.00K");
        assert_eq!(format_number(-1500.0), "-1.50K");

        // Test edge cases
        assert_eq!(format_number(999.0), "999"); // Should not be formatted as K
        assert_eq!(format_number(1001.0), "1.00K"); // Should be formatted as K with 2 decimals
    }

    #[test]
    fn test_format_number_million_unit() {
        // Test M unit (1,000,000 - 999,999,999)
        assert_eq!(format_number(1_000_000.0), "1.00M");
        assert_eq!(format_number(1_500_000.0), "1.50M");
        assert_eq!(format_number(999_999_999.0), "999.99M");

        // Test negative M unit
        assert_eq!(format_number(-1_000_000.0), "-1.00M");
        assert_eq!(format_number(-1_500_000.0), "-1.50M");
    }

    #[test]
    fn test_format_number_billion_unit() {
        // Test B unit (1,000,000,000 - 999,999,999,999)
        assert_eq!(format_number(1_000_000_000.0), "1.00B");
        assert_eq!(format_number(1_500_000_000.0), "1.50B");
        assert_eq!(format_number(999_999_999_999.0), "999.99B");

        // Test negative B unit
        assert_eq!(format_number(-1_000_000_000.0), "-1.00B");
        assert_eq!(format_number(-1_500_000_000.0), "-1.50B");
    }

    #[test]
    fn test_format_number_trillion_unit() {
        // Test T unit (>= 1,000,000,000,000)
        assert_eq!(format_number(1_000_000_000_000.0), "1.00T");
        assert_eq!(format_number(1_500_000_000_000.0), "1.50T");
        assert_eq!(format_number(999_999_999_999_999.0), "999.99T");

        // Test negative T unit
        assert_eq!(format_number(-1_000_000_000_000.0), "-1.00T");
        assert_eq!(format_number(-1_500_000_000_000.0), "-1.50T");
    }

    #[test]
    fn test_format_number_decimal_places() {
        // With the new logic, all formatted numbers now show 2 decimal places
        assert_eq!(format_number(100_000.0), "100.00K");
        assert_eq!(format_number(1_500_000.0), "1.50M");
        assert_eq!(format_number(250_000_000.0), "250.00M");

        assert_eq!(format_number(10_000.0), "10.00K");
        assert_eq!(format_number(15_000.0), "15.00K");
        assert_eq!(format_number(99_999.0), "99.99K");

        assert_eq!(format_number(1_000.0), "1.00K");
        assert_eq!(format_number(1_500.0), "1.50K");
        assert_eq!(format_number(9_999.0), "9.99K");
    }

    #[test]
    fn test_format_number_edge_cases() {
        // Test very large numbers
        assert_eq!(format_number(1_000_000_000_000_000.0), "1000.00T");

        // Test numbers just at unit boundaries
        assert_eq!(format_number(999_999.0), "999.99K");
        assert_eq!(format_number(1_000_000.0), "1.00M");
        assert_eq!(format_number(999_999_999.0), "999.99M");
        assert_eq!(format_number(1_000_000_000.0), "1.00B");
        assert_eq!(format_number(999_999_999_999.0), "999.99B");
        assert_eq!(format_number(1_000_000_000_000.0), "1.00T");

        // Test floating point precision
        assert_eq!(format_number(1_000_000_000.5), "1.00B");
    }

    #[test]
    fn test_sanitize_url() {
        assert_eq!(
            sanitize_url("http://user:pass@proxy.com:8080"),
            "http://user:***@proxy.com:8080/"
        );

        assert_eq!(
            sanitize_url("https://proxy.example.com"),
            "https://proxy.example.com/"
        );

        assert_eq!(
            sanitize_url("http://proxy.example.com"),
            "http://proxy.example.com/"
        );

        assert_eq!(
            sanitize_url("https://proxy.example.com:3128"),
            "https://proxy.example.com:3128/"
        );

        assert_eq!(
            sanitize_url("socks5://user:pass@proxy.com:1080"),
            "socks5://user:***@proxy.com:1080"
        );

        assert_eq!(sanitize_url("invalid-url"), "<invalid-url>");

        assert_eq!(
            sanitize_url("custom://proxy.com:9999"),
            "custom://proxy.com:9999"
        );
    }

    #[test]
    fn test_codis_slot() {
        // Basic key tests
        assert_eq!(codis_slot(b"foo"), 289);
        assert_eq!(codis_slot(b"key1"), 80);
        assert_eq!(codis_slot(b"test"), 524);
        assert_eq!(codis_slot(b"123"), 978);

        // Empty key
        assert_eq!(codis_slot(b""), 0);

        // Single character
        assert_eq!(codis_slot(b"a"), 579);

        // Long key
        assert_eq!(codis_slot(b"very_long_key_name_for_testing"), 29);

        // Special characters
        assert_eq!(codis_slot(b"user:123:profile"), 875);
        assert_eq!(codis_slot(b"key:test:123"), 832);

        // Hashtag tests - valid hashtags
        assert_eq!(codis_slot(b"{foo}"), 289);
        assert_eq!(codis_slot(b"{user}:profile"), 585);
        assert_eq!(codis_slot(b"{user1}:profile"), 341);
        assert_eq!(codis_slot(b"{user1}:settings"), 341); // Same hashtag, same slot
        assert_eq!(codis_slot(b"{123}:data"), 978);

        // Empty hashtag - computes CRC of empty string for Codis
        assert_eq!(codis_slot(b"{}:key"), 0);
        assert_eq!(codis_slot(b"{}:different"), 0); // Empty hashtag always gives slot 0 for Codis
        assert_eq!(codis_slot(b"prefix:{}:suffix"), 0);

        // Invalid hashtag patterns - use whole key
        assert_eq!(codis_slot(b"{foo"), 178); // No closing brace
        assert_eq!(codis_slot(b"foo}"), 311); // No opening brace
        assert_eq!(codis_slot(b"user}:profile"), 904); // No opening brace

        // Multiple hashtags - should use first valid one
        assert_eq!(codis_slot(b"foo{foo}"), 289);
        assert_eq!(codis_slot(b"{user}{group}:data"), 585);

        // Hashtag with special characters
        assert_eq!(codis_slot(b"{user:123}:profile"), 360);

        // Hashtag at different positions
        assert_eq!(codis_slot(b"prefix:{user}"), 585);
        assert_eq!(codis_slot(b"app:cache:{session123}:user:456"), 573);
    }

    #[test]
    fn test_redis_slot() {
        // Basic key tests
        assert_eq!(redis_slot(b"foo"), 12182);
        assert_eq!(redis_slot(b"hello"), 866);
        assert_eq!(redis_slot(b"key1"), 9189);
        assert_eq!(redis_slot(b"123"), 5970);

        // Empty key
        assert_eq!(redis_slot(b""), 0);

        // Single character
        assert_eq!(redis_slot(b"a"), 15495);

        // Special characters
        assert_eq!(redis_slot(b"key:test:123"), 4691);
        assert_eq!(redis_slot(b"user:123:profile"), 8490);

        // Hashtag tests - valid hashtags
        assert_eq!(redis_slot(b"{foo}"), 12182);
        assert_eq!(redis_slot(b"{user}:profile"), 5474);
        assert_eq!(redis_slot(b"{user1}:profile"), 8106);
        assert_eq!(redis_slot(b"{user1}:settings"), 8106); // Same hashtag, same slot
        assert_eq!(redis_slot(b"{123}:data"), 5970);

        // Empty hashtag - uses whole key for Redis Cluster
        assert_eq!(redis_slot(b"{}"), 15257);
        assert_eq!(redis_slot(b"{}:key"), 3707);
        assert_eq!(redis_slot(b"{}:different"), 12361); // Different keys give different slots
        assert_eq!(redis_slot(b"prefix:{}:suffix"), 4527);

        // Invalid hashtag patterns - use whole key
        assert_eq!(redis_slot(b"{foo"), 13308); // No closing brace
        assert_eq!(redis_slot(b"foo}"), 15679); // No opening brace
        assert_eq!(redis_slot(b"{user:profile"), 8348); // No closing brace
        assert_eq!(redis_slot(b"user}:profile"), 7010); // No opening brace

        // Multiple hashtags - should use first valid one
        assert_eq!(redis_slot(b"foo{foo}"), 12182);
        assert_eq!(redis_slot(b"{user}{group}:data"), 5474);

        // Hashtag with special characters
        assert_eq!(redis_slot(b"{user:123}:profile"), 12893);

        // Hashtag at different positions
        assert_eq!(redis_slot(b"prefix:{user}"), 5474);
        assert_eq!(redis_slot(b"app:cache:{session123}:user:456"), 568);
    }

    #[test]
    fn test_format_bytesize() {
        assert_eq!(format_bytesize(0), "0B");
        assert_eq!(format_bytesize(1), "1B");
        assert_eq!(format_bytesize(1023), "1023B");
        assert_eq!(format_bytesize(1024), "1.00KB");
        assert_eq!(format_bytesize(1536), "1.50KB");
        assert_eq!(format_bytesize(1024 * 1024), "1.00MB");
        assert_eq!(format_bytesize(5 * 1024 * 1024 + 512 * 1024), "5.50MB");
        assert_eq!(format_bytesize(1024u64.pow(3)), "1.00GB");
        assert_eq!(format_bytesize(1024u64.pow(4)), "1.00TB");
        assert_eq!(format_bytesize(1024u64.pow(5)), "1.00PB");
    }
}
