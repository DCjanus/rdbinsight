pub type AnyResult<T = ()> = anyhow::Result<T>;

pub fn wrapping_to_usize(value: u64) -> usize {
    value.try_into().unwrap_or(usize::MAX)
}
