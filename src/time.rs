//! Timestamps: the three valid forms F1, F2, F3 from `rules.md`, and nothing
//! else. Everything is integer milliseconds since the Unix epoch, UTC.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY_MS: i64 = 24 * HOUR;

/// Howard Hinnant's `days_from_civil` (proleptic Gregorian).
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn two(s: &[u8], at: usize) -> Option<i64> {
    let a = *s.get(at)?;
    let b = *s.get(at + 1)?;
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(i64::from(a - b'0') * 10 + i64::from(b - b'0'))
    } else {
        None
    }
}

/// The first 19 characters of F1/F2: `2026-10-03T23:58:10`. Returns the
/// calendar time as milliseconds since the epoch, or None if anything about
/// the shape or the calendar value is wrong.
fn civil_prefix(s: &[u8]) -> Option<i64> {
    if s.len() < 19 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let mut year = 0i64;
    for at in 0..4 {
        let d = *s.get(at)?;
        if !d.is_ascii_digit() {
            return None;
        }
        year = year * 10 + i64::from(d - b'0');
    }
    let month = two(s, 5)?;
    let day = two(s, 8)?;
    let hour = two(s, 11)?;
    let minute = two(s, 14)?;
    let second = two(s, 17)?;
    if month < 1 || month > 12 {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(
        days_from_civil(year, month, day) * DAY_MS
            + hour * HOUR
            + minute * MINUTE
            + second * SECOND,
    )
}

/// F3: digits only, epoch milliseconds. Leading zeros are allowed. A value too
/// large for a 64-bit integer is outside every window, so it is None here; the
/// rules call it "outside the window", which drops the row the same way.
fn epoch_millis(s: &[u8]) -> Option<i64> {
    let digits = match s.iter().position(|&c| c != b'0') {
        None => return Some(0), // all zeros
        Some(at) => &s[at..],
    };
    if digits.is_empty() || digits.len() > 19 {
        return None;
    }
    let mut value: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        value = value * 10 + i64::from(c - b'0');
    }
    Some(value)
}

/// Parses `ts` in any of the three forms. None means a bad timestamp.
pub fn parse_timestamp(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    // F3 is digits only, and neither F1 nor F2 can be.
    if s[0].is_ascii_digit() && s.iter().all(|c| c.is_ascii_digit()) {
        return epoch_millis(s);
    }
    match s.len() {
        // F1: 2026-10-03T23:58:10Z
        20 => {
            if s[19] != b'Z' {
                return None;
            }
            civil_prefix(s)
        }
        // F2: 2026-10-03T19:58:10-04:00
        25 => {
            let sign = match s[19] {
                b'+' => 1i64,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let hours = two(s, 20)?;
            let minutes = two(s, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let local = civil_prefix(s)?;
            Some(local - sign * (hours * HOUR + minutes * MINUTE))
        }
        _ => None,
    }
}

/// `ingest_ts` is always a valid F1, but the same parser accepts it.

/// `ingest_ts` is always a valid F1, so it can skip the form checks.
#[inline]
pub fn parse_f1(s: &[u8]) -> Option<i64> {
    if s.len() != 20 || s[19] != b'Z' {
        return None;
    }
    civil_prefix(s)
}

/// The UTC date, as days since the epoch (what a Parquet DATE holds).
#[inline]
pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(DAY_MS) as i32
}