#[cfg(any(debug_assertions, test))]
#[macro_export]
macro_rules! parser_trace {
    ($tag:expr $(, $field:expr)*) => {
        ::tracing::trace!(target: "parser.trace_point", path = $tag $(, $field)*);
    };
}

#[cfg(not(any(debug_assertions, test)))]
#[macro_export]
macro_rules! parser_trace {
    ($tag:expr $(, $field:expr)*) => {};
}
