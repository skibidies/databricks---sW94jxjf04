//! The CSV dialect from `rules.md`: comma separated, `\n` line endings, any
//! field may be wrapped in double quotes (the quotes are syntax), no value ever
//! holds a comma, a quote or a newline, an empty field is null, and spaces are
//! part of a value. A line with the wrong shape is malformed; the caller drops
//! it.

/// Splits `line` into fields. Returns false when the line is malformed: an
/// empty line, or a quote anywhere except around a whole field.
///
/// `line` must not include its `\n`. A `\r` is an ordinary character and stays
/// in the field it lands in. One pass: commas and quotes are counted as the
/// field is scanned.
pub fn split_fields<'a>(line: &'a [u8], out: &mut Vec<&'a [u8]>) -> bool {
    out.clear();
    if line.is_empty() {
        return false;
    }
    let mut start = 0;
    let mut at = 0;
    loop {
        let mut quotes = 0u32;
        while at < line.len() && line[at] != b',' {
            if line[at] == b'"' {
                quotes += 1;
            }
            at += 1;
        }
        let field = &line[start..at];
        match quotes {
            // Bare: the value is the field.
            0 => out.push(field),
            // Quoted: the quotes wrap the whole field and there are no others.
            2 if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' => {
                out.push(&field[1..field.len() - 1]);
            }
            _ => return false,
        }
        if at >= line.len() {
            break;
        }
        at += 1; // the comma
        start = at;
    }
    true
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