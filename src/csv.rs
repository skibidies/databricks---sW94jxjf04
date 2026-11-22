//! The CSV dialect from `rules.md`: comma separated, `\n` line endings, any
//! field may be wrapped in double quotes (the quotes are syntax and are not
//! part of the value), no value ever holds a comma, a quote or a newline, an
//! empty field is null, and spaces are part of a value. A line with the wrong
//! shape is malformed; the caller drops it.

/// Splits `line` into exactly `N` fields, or returns None when the line is
/// malformed: empty, the wrong number of fields, or a quote anywhere except
/// around a whole field.
///
/// `line` must not include its `\n`. A `\r` is an ordinary character and stays
/// in the field it lands in. One SIMD pass finds every comma and quote, so a
/// bare field costs no scanning of its own.
pub fn split_fields_n<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&'a [u8]; N] = [&[]; N];
    let mut n = 0usize;
    let mut start = 0usize;
    // A comma is always a delimiter - no value holds one, quoted or not - so
    // one single-needle SIMD pass finds every boundary. Quotes only ever wrap
    // a whole field and are checked at the edges below.
    for at in memchr::memchr_iter(b',', line) {
        if n + 1 >= N {
            return None; // more fields than the caller asked for
        }
        out[n] = field_value(&line[start..at])?;
        n += 1;
        start = at + 1;
    }
    if n + 1 != N {
        return None;
    }
    out[n] = field_value(&line[start..])?;
    Some(out)
}

/// A field's value: a bare field is itself, a quoted field loses its wrapping
/// quotes. Any other shape of quote usage makes the line malformed.
#[inline]
fn field_value(field: &[u8]) -> Option<&[u8]> {
    let last = field.len().wrapping_sub(1);
    match (field.first(), field.last()) {
        (Some(b'"'), Some(b'"')) if field.len() >= 2 => {
            let inner = &field[1..last];
            if inner.iter().any(|&b| b == b'"') {
                return None;
            }
            Some(inner)
        }
        _ => {
            if field.iter().any(|&b| b == b'"') {
                return None;
            }
            Some(field)
        }
    }
}

/// Splits a file body into chunks that end on a line boundary, for parallel
/// parsing. Every chunk but the last ends with `\n`.
pub fn chunks(body: &[u8], target: usize) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut rest = body;
    while rest.len() > target {
        let mut at = target;
        while at < rest.len() && rest[at] != b'\n' {
            at += 1;
        }
        if at >= rest.len() {
            break;
        }
        out.push(&rest[..=at]);
        rest = &rest[at + 1..];
    }
    if !rest.is_empty() {
        out.push(rest);
    }
    out
}

/// Everything after the first line of a file (its header).
pub fn body_after_header(file: &[u8]) -> &[u8] {
    match file.iter().position(|&b| b == b'\n') {
        Some(at) => &file[at + 1..],
        None => &[],
    }
}