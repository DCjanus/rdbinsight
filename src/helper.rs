pub type AnyResult<T = ()> = anyhow::Result<T>;

pub fn wrapping_to_usize(value: u64) -> usize {
    value.try_into().unwrap_or(usize::MAX)
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
