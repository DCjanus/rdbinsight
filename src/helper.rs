pub type AnyResult<T = ()> = anyhow::Result<T>;

pub mod proxy_connector;

use crc::{CRC_16_XMODEM, CRC_32_ISO_HDLC, Crc};

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
}
