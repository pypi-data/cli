use regex::bytes::Regex;
use std::borrow::Cow;

thread_local! {
    pub static NEWLINE_PATTERN: Regex = Regex::new(r"(\r\n|\r)").unwrap();
}
#[inline]
pub fn normalize(item: &[u8]) -> Cow<[u8]> {
    NEWLINE_PATTERN.with(|pattern| pattern.replace_all(item, b"\n"))
}
