pub type AnyResult<T = ()> = anyhow::Result<T>;

pub mod proxy_connector;

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
}
