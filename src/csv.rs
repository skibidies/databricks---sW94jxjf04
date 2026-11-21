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
    let mut first_quote = usize::MAX;
    let mut quotes = 0u32;
    for at in memchr::memchr2_iter(b',', b'"', line) {
        if line[at] == b',' {
            if n + 1 >= N {
                return None; // more fields than the caller asked for
            }
            out[n] = field_value(line, start, at, first_quote, quotes)?;
            n += 1;
            start = at + 1;
            first_quote = usize::MAX;
            quotes = 0;
        } else {
            if quotes == 0 {
                first_quote = at;
            }
            quotes += 1;
        }
    }
    if n + 1 != N {
        return None;
    }
    out[n] = field_value(line, start, line.len(), first_quote, quotes)?;
    Some(out)
}

/// A field's value: a bare field is itself, a quoted field loses its wrapping
/// quotes. Any other shape of quote usage makes the line malformed.
#[inline]
fn field_value<'a>(
    line: &'a [u8],
    start: usize,
    end: usize,
    first_quote: usize,
    quotes: u32,
) -> Option<&'a [u8]> {
    match quotes {
        0 => Some(&line[start..end]),
        2 if first_quote == start && end - start >= 2 && line[end - 1] == b'"' => {
            Some(&line[start + 1..end - 1])
        }
        _ => None,
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