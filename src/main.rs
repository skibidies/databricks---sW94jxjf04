//! Shopping Sessions: reads the dirty logs in $DATA_DIR and writes the two daily
//! Parquet tables to $OUTPUT_DIR exactly as `rules.md` defines them.
//!
//! The order of operations follows the rules: links first (so every rule that
//! says "person" means the linked group), then events (parse -> dedup on
//! event_id keeping the smallest ingest_ts -> validate -> drop late ->
//! sessionize), then clicks (parse -> dedup -> validate -> credit and assist).

mod csv;
mod output;
mod strmap;
mod time;

use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;

use strmap::{ByteMap, StrMap};

/// A new session starts when the gap from the previous event is over 30 min.
const SESSION_GAP: i64 = 1_800_000;
/// An event is late when it was ingested more than 24 h after its `ts`.
const LATE_MS: i64 = 86_400_000;
/// A click can credit or assist a session up to 7 days later.
const CLICK_REACH: i64 = 604_800_000;
/// Bytes of CSV handed to one parse task.
const CHUNK: usize = 4 << 20;
/// Rows of map work handed over at a time: one lock per shard per batch.
const BATCH: usize = 8 << 10;

const NO_USER: u32 = u32::MAX;
const NO_CAMPAIGN: u32 = u32::MAX;
const NO_CAT: u32 = u32::MAX;

/// A parsed event line, as the event_id dedup keeps it.
#[derive(Clone, Copy)]
struct EvRow {
    ts: i64,     // i64::MIN when the ts is bad
    ingest: i64, // i64::MAX when the ingest_ts is bad
    price: i64,  // -1 unless it is a purchase with a valid price
    user: u32,
    cat: u32,
    kind: u8,  // 0 view, 1 cart, 2 purchase, 255 anything else
    flags: u8, // bit 0: product_id is not empty
}

/// A line parsed off its chunk, waiting for its turn at the shared maps.
struct Pending<'a> {
    id: &'a [u8],
    user: &'a [u8],
    ts: Option<i64>,
    ingest: Option<i64>,
    price: i64,
    cat: u32,
    kind: u8,
    flags: u8,
}

/// A parsed click line, waiting for its turn at the shared maps.
struct ClickPending<'a> {
    id: &'a [u8],
    user: &'a [u8],
    ts: Option<i64>,
    campaign: u32,
}

/// A parsed click line, as the click_id dedup keeps it.
#[derive(Clone, Copy)]
struct CkRow {
    ts: i64,
    user: u32,
    campaign: u32,
}

/// A surviving event. The person is where it sits: events are grouped by
/// person and sorted by ts before anyone looks at them.
#[derive(Clone, Copy)]
struct Ev2 {
    ts: i64,
    price: i64,
    cat: u32,
    kind: u8,
}

/// A surviving click, with its id kept for tie-breaks.
#[derive(Clone, Copy)]
struct Ck2 {
    ts: i64,
    campaign: u32,
    id_off: u32,
    id_len: u32,
}

/// One row of /output/user_daily/.
#[derive(Clone, Copy)]
struct UserRow {
    name: u32,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top_cat: u32,
}

/// products.csv and campaigns.csv: both clean, both small.
struct Catalog {
    product_cat: ByteMap,
    cat_names: Vec<Box<[u8]>>,
    unknown: u32,
    campaign_index: ByteMap,
    campaign_ids: Vec<Box<[u8]>>,
    campaign_channels: Vec<Box<[u8]>>,
    organic: u32,
}

/// Everything a person's processing needs beyond its own events and clicks.
struct Ctx {
    day0: i32,
    n_days: usize,
    organic: u32,
    cat_names: Vec<Box<[u8]>>,
}

/// Per-worker accumulator: user_daily rows plus the (campaign, day) counters.
struct Acc {
    rows: Vec<UserRow>,
    camp: Vec<i64>, // [credited, purchasing, revenue, assisted] per (campaign, day)
}

impl Acc {
    fn new(n_camps: usize, n_days: usize) -> Self {
        Acc {
            rows: Vec::new(),
            camp: vec![0; n_camps * n_days * 4],
        }
    }
    fn merge(&mut self, other: Acc) {
        for (to, from) in self.camp.iter_mut().zip(other.camp) {
            *to += from;
        }
        self.rows.extend(other.rows);
    }
}

fn main() {
    let started = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    // The scorer pins the vCPUs; use exactly those.
    if let Ok(ncpu) = std::env::var("NCPU") {
        if let Ok(threads) = ncpu.parse::<usize>() {
            if threads > 0 {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build_global()
                    .ok();
            }
        }
    }
    let data = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| {
        std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string())
    }));
    let out = PathBuf::from(args.get(2).cloned().unwrap_or_else(|| {
        std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string())
    }));

    let (win_start, win_end) = read_manifest(&data);
    let mut catalog = read_catalog(&data);
    let day0 = time::day_of(win_start);
    let n_days = (time::day_of(win_end - 1) - day0 + 1).max(1) as usize + 1;
    let organic = catalog.organic;
    let n_camps = organic as usize + 1;

    // ---- parse ------------------------------------------------------------

    let users: StrMap<u32> = StrMap::new();
    let ev_map: StrMap<EvRow> = StrMap::new();
    let ck_map: StrMap<CkRow> = StrMap::new();

    parse_events(&data, &catalog, &users, &ev_map);
    parse_clicks(&data, &catalog, &users, &ck_map);
    let links = parse_links(&data, &users);
    eprintln!("[pipeline] parse {:?}", started.elapsed());

    // ---- people: links join ids transitively, the name is the smallest id --

    let mut name_bytes: Vec<u8> = Vec::new();
    let mut name_ranges: Vec<(u32, u32)> = Vec::new();
    users.drain_into(&mut name_bytes, |id, off, len| {
        if name_ranges.len() <= id as usize {
            name_ranges.resize(id as usize + 1, (0, 0));
        }
        name_ranges[id as usize] = (off, len);
    });
    let n_users = name_ranges.len();
    drop(users);
    let user_str = |id: u32| -> &[u8] {
        let (off, len) = name_ranges[id as usize];
        &name_bytes[off as usize..(off + len) as usize]
    };

    let mut parent: Vec<u32> = (0..n_users as u32).collect();
    for &(a, b) in &links {
        union(&mut parent, a, b);
    }
    for u in 0..n_users {
        let root = find(&mut parent, u as u32);
        parent[u] = root;
    }
    let mut name_of: Vec<u32> = vec![NO_USER; n_users];
    for u in 0..n_users as u32 {
        let root = parent[u as usize] as usize;
        if name_of[root] == NO_USER || user_str(u) < user_str(name_of[root]) {
            name_of[root] = u;
        }
    }
    eprintln!("[pipeline] people {:?}", started.elapsed());

    // ---- surviving events, grouped by person and sorted by ts -------------

    let mut ev_count = vec![0usize; n_users];
    ev_map.for_each_value(|row| {
        if event_survives(&row, win_start, win_end) {
            ev_count[parent[row.user as usize] as usize] += 1;
        }
    });
    let mut ev_at = vec![0usize; n_users];
    let mut total = 0usize;
    for p in 0..n_users {
        ev_at[p] = total;
        total += ev_count[p];
    }
    let mut ev_sorted = vec![Ev2 { ts: 0, price: 0, cat: 0, kind: 0 }; total];
    let mut cursor = ev_at.clone();
    // The map is emptied as it is read, so its memory never stacks up with the
    // events it produced.
    ev_map.drain_values(|row| {
        if event_survives(&row, win_start, win_end) {
            let person = parent[row.user as usize] as usize;
            let at = cursor[person];
            cursor[person] += 1;
            ev_sorted[at] = Ev2 {
                ts: row.ts,
                price: row.price,
                cat: row.cat,
                kind: row.kind,
            };
        }
    });
    drop(cursor);
    drop(ev_map);
    {
        // Each person's events sort by ts on their own; the people go in
        // parallel.
        let mut groups: Vec<&mut [Ev2]> = Vec::with_capacity(n_users);
        let mut rest: &mut [Ev2] = &mut ev_sorted;
        for p in 0..n_users {
            if ev_count[p] == 0 {
                continue;
            }
            let (head, tail) = rest.split_at_mut(ev_count[p]);
            groups.push(head);
            rest = tail;
        }
        groups
            .par_iter_mut()
            .for_each(|slice| slice.sort_unstable_by_key(|e| e.ts));
    }
    eprintln!("[pipeline] group events {:?}", started.elapsed());

    // ---- surviving clicks, grouped by person and sorted by ts -------------

    let mut ck_ids: Vec<u8> = Vec::new();
    let mut ck_count = vec![0usize; n_users];
    ck_map.for_each_value(|row| {
        if click_survives(&row, win_start, win_end) {
            ck_count[parent[row.user as usize] as usize] += 1;
        }
    });
    let mut ck_at = vec![0usize; n_users];
    let mut total = 0usize;
    for p in 0..n_users {
        ck_at[p] = total;
        total += ck_count[p];
    }
    let mut ck_sorted = vec![Ck2 { ts: 0, campaign: 0, id_off: 0, id_len: 0 }; total];
    let mut cursor = ck_at.clone();
    ck_map.drain_into(&mut ck_ids, |row, off, len| {
        if click_survives(&row, win_start, win_end) {
            let person = parent[row.user as usize] as usize;
            let at = cursor[person];
            cursor[person] += 1;
            ck_sorted[at] = Ck2 {
                ts: row.ts,
                campaign: row.campaign,
                id_off: off,
                id_len: len,
            };
        }
    });
    drop(cursor);
    drop(ck_map);
    {
        let mut groups: Vec<&mut [Ck2]> = Vec::with_capacity(n_users);
        let mut rest: &mut [Ck2] = &mut ck_sorted;
        for p in 0..n_users {
            if ck_count[p] == 0 {
                continue;
            }
            let (head, tail) = rest.split_at_mut(ck_count[p]);
            groups.push(head);
            rest = tail;
        }
        groups
            .par_iter_mut()
            .for_each(|slice| slice.sort_unstable_by_key(|c| c.ts));
    }
    eprintln!("[pipeline] group clicks {:?}", started.elapsed());

    eprintln!(
        "[pipeline] {} events, {} clicks, {} user ids, {} links in {:?}",
        ev_sorted.len(),
        ck_sorted.len(),
        n_users,
        links.len(),
        started.elapsed()
    );

    // ---- sessions, attribution, assists -----------------------------------

    let ctx = Ctx {
        day0,
        n_days,
        organic,
        cat_names: std::mem::take(&mut catalog.cat_names),
    };
    let persons: Vec<u32> = (0..n_users as u32)
        .filter(|&p| ev_count[p as usize] > 0)
        .collect();

    let acc = persons
        .par_iter()
        .fold(
            || Acc::new(n_camps, n_days),
            |mut acc, &p| {
                let ev = &ev_sorted[ev_at[p as usize]..ev_at[p as usize] + ev_count[p as usize]];
                let ck = &ck_sorted[ck_at[p as usize]..ck_at[p as usize] + ck_count[p as usize]];
                process_person(&ctx, name_of[p as usize], ev, ck, &ck_ids, &mut acc);
                acc
            },
        )
        .reduce(
            || Acc::new(n_camps, n_days),
            |mut a, b| {
                a.merge(b);
                a
            },
        );
    eprintln!("[pipeline] process {:?}", started.elapsed());

    // ---- write ------------------------------------------------------------

    let mut user_rows: Vec<output::UserOutRow> = acc
        .rows
        .par_iter()
        .map(|row| {
            let top = if row.top_cat == NO_CAT {
                None
            } else {
                Some(ctx.cat_names[row.top_cat as usize].as_ref())
            };
            output::UserOutRow {
                user_id: user_str(row.name),
                day: row.day,
                sessions: row.sessions,
                events: row.events,
                revenue_cents: row.revenue,
                top_category: top,
            }
        })
        .collect();
    user_rows.par_sort_unstable_by(|a, b| (a.day, a.user_id).cmp(&(b.day, b.user_id)));

    let mut camp_rows: Vec<output::CampOutRow> = Vec::new();
    for campaign in 0..n_camps {
        for d in 0..n_days {
            let at = (campaign * n_days + d) * 4;
            let (sessions, purchasing, revenue, assisted) =
                (acc.camp[at], acc.camp[at + 1], acc.camp[at + 2], acc.camp[at + 3]);
            if sessions + assisted == 0 {
                continue;
            }
            camp_rows.push(output::CampOutRow {
                campaign_id: &catalog.campaign_ids[campaign],
                channel: &catalog.campaign_channels[campaign],
                day: day0 + d as i32,
                sessions,
                purchasing_sessions: purchasing,
                revenue_cents: revenue,
                assisted_sessions: assisted,
            });
        }
    }
    camp_rows.sort_unstable_by(|a, b| (a.day, a.campaign_id).cmp(&(b.day, b.campaign_id)));

    std::fs::create_dir_all(out.join("user_daily")).expect("cannot create user_daily");
    std::fs::create_dir_all(out.join("campaign_daily")).expect("cannot create campaign_daily");
    // A rerun into the same folder must not leave stale parts behind.
    for table in ["user_daily", "campaign_daily"] {
        if let Ok(entries) = std::fs::read_dir(out.join(table)) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with("part-") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
    output::write_user_daily(&out.join("user_daily"), &user_rows).expect("cannot write user_daily");
    output::write_campaign_daily(&out.join("campaign_daily"), &camp_rows)
        .expect("cannot write campaign_daily");

    eprintln!(
        "[pipeline] wrote {} user_daily and {} campaign_daily rows in {:?}",
        user_rows.len(),
        camp_rows.len(),
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// one person: sessions, then which click credits each one, then assists
// ---------------------------------------------------------------------------

fn process_person(
    ctx: &Ctx,
    name: u32,
    ev: &[Ev2],
    ck: &[Ck2],
    ck_ids: &[u8],
    acc: &mut Acc,
) {
    let n_days = ctx.n_days;
    // Session starts in order, and how many sessions each day holds, so a
    // campaign's assists can be counted per day in one step.
    let mut starts: Vec<i64> = Vec::new();
    let mut day_first = vec![usize::MAX; n_days];
    let mut day_count = vec![0usize; n_days];

    let mut cur_day: usize = usize::MAX;
    let mut day_sessions: i64 = 0;
    let mut day_events: i64 = 0;
    let mut day_revenue: i64 = 0;
    let mut cat_sums: Vec<(u32, i64)> = Vec::new();

    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut at = 0usize;
    while at < ev.len() {
        // A new session when the gap to the previous event is over 30 min.
        let mut end = at + 1;
        while end < ev.len() && ev[end].ts - ev[end - 1].ts <= SESSION_GAP {
            end += 1;
        }
        let session = &ev[at..end];
        let start = session[0].ts;
        let d = (time::day_of(start) - ctx.day0) as usize;
        if d != cur_day {
            if cur_day != usize::MAX {
                flush_day(
                    ctx,
                    acc,
                    name,
                    cur_day,
                    day_sessions,
                    day_events,
                    day_revenue,
                    &cat_sums,
                );
            }
            cur_day = d;
            day_sessions = 0;
            day_events = 0;
            day_revenue = 0;
            cat_sums.clear();
        }
        day_sessions += 1;
        day_events += session.len() as i64;

        let mut revenue: i64 = 0;
        let mut purchasing = false;
        for e in session {
            if e.kind == 2 {
                purchasing = true;
                revenue += e.price;
                match cat_sums.iter_mut().find(|(cat, _)| *cat == e.cat) {
                    Some((_, sum)) => *sum += e.price,
                    None => cat_sums.push((e.cat, e.price)),
                }
            }
        }
        day_revenue += revenue;

        if day_first[d] == usize::MAX {
            day_first[d] = starts.len();
        }
        day_count[d] += 1;
        starts.push(start);

        // Credit: the same person's latest click at or before the start within
        // 7 days, ties going to the smallest click_id.
        while hi < ck.len() && ck[hi].ts <= start {
            hi += 1;
        }
        while lo < hi && ck[lo].ts < start - CLICK_REACH {
            lo += 1;
        }
        let credited = if lo < hi {
            let best_ts = ck[hi - 1].ts;
            let mut best = hi - 1;
            let mut k = hi - 1;
            while k > lo && ck[k - 1].ts == best_ts {
                k -= 1;
                if click_id(ck_ids, &ck[k]) < click_id(ck_ids, &ck[best]) {
                    best = k;
                }
            }
            Some(ck[best].campaign)
        } else {
            None
        };
        let campaign = credited.unwrap_or(ctx.organic) as usize;
        let slot = (campaign * n_days + d) * 4;
        acc.camp[slot] += 1;
        if purchasing {
            acc.camp[slot + 1] += 1;
        }
        acc.camp[slot + 2] += revenue;

        at = end;
    }
    if cur_day != usize::MAX {
        flush_day(
            ctx,
            acc,
            name,
            cur_day,
            day_sessions,
            day_events,
            day_revenue,
            &cat_sums,
        );
    }

    // Assists: a campaign assists a session when the person clicked it in the
    // 7 days up to the start, the crediting click included. One campaign
    // assists a session once however many of its clicks qualify, so each
    // campaign's clicks are merged into their intervals and every session that
    // falls in one is counted.
    if ck.is_empty() || starts.is_empty() {
        return;
    }
    let mut by_campaign: Vec<(u32, i64)> = ck.iter().map(|c| (c.campaign, c.ts)).collect();
    by_campaign.sort_unstable();
    let mut i = 0usize;
    while i < by_campaign.len() {
        let campaign = by_campaign[i].0 as usize;
        let mut from = by_campaign[i].1;
        let mut to = from + CLICK_REACH;
        i += 1;
        while i < by_campaign.len() && by_campaign[i].0 as usize == campaign {
            if by_campaign[i].1 <= to {
                to = to.max(by_campaign[i].1 + CLICK_REACH);
            } else {
                assist_interval(ctx, acc, campaign, from, to, &starts, &day_first, &day_count);
                from = by_campaign[i].1;
                to = from + CLICK_REACH;
            }
            i += 1;
        }
        assist_interval(ctx, acc, campaign, from, to, &starts, &day_first, &day_count);
    }
}

/// Adds one assisted session per session start in [from, to], on each session's
/// own day.
fn assist_interval(
    ctx: &Ctx,
    acc: &mut Acc,
    campaign: usize,
    from: i64,
    to: i64,
    starts: &[i64],
    day_first: &[usize],
    day_count: &[usize],
) {
    let first = starts.partition_point(|&start| start < from);
    let end = starts.partition_point(|&start| start <= to);
    if end <= first {
        return;
    }
    for d in 0..ctx.n_days {
        if day_count[d] == 0 {
            continue;
        }
        let lo = day_first[d].max(first);
        let hi = (day_first[d] + day_count[d]).min(end);
        if hi > lo {
            acc.camp[(campaign * ctx.n_days + d) * 4 + 3] += (hi - lo) as i64;
        }
    }
}

/// Closes a (person, day) and writes its user_daily row. The top category is
/// the one with the highest summed purchase price, ties going to the smallest
/// name, and null when the day had no purchases.
fn flush_day(
    ctx: &Ctx,
    acc: &mut Acc,
    name: u32,
    day: usize,
    sessions: i64,
    events: i64,
    revenue: i64,
    cat_sums: &[(u32, i64)],
) {
    let mut top = NO_CAT;
    let mut best: i64 = 0;
    for &(cat, sum) in cat_sums {
        let better = if top == NO_CAT {
            true
        } else if sum != best {
            sum > best
        } else {
            ctx.cat_names[cat as usize].as_ref() < ctx.cat_names[top as usize].as_ref()
        };
        if better {
            top = cat;
            best = sum;
        }
    }
    acc.rows.push(UserRow {
        name,
        day: ctx.day0 + day as i32,
        sessions,
        events,
        revenue,
        top_cat: top,
    });
}

#[inline]
fn click_id<'a>(ck_ids: &'a [u8], click: &Ck2) -> &'a [u8] {
    &ck_ids[click.id_off as usize..(click.id_off + click.id_len) as usize]
}

// ---------------------------------------------------------------------------
// input
// ---------------------------------------------------------------------------

fn read_manifest(data: &Path) -> (i64, i64) {
    let text =
        std::fs::read_to_string(data.join("manifest.json")).expect("cannot read manifest.json");
    let field = |key: &str| -> i64 {
        let needle = format!("\"{key}\"");
        let at = text
            .find(&needle)
            .unwrap_or_else(|| panic!("manifest.json has no {key}"));
        let rest = &text[at + needle.len()..];
        let open = rest.find('"').expect("window value");
        let rest = &rest[open + 1..];
        let close = rest.find('"').expect("window value");
        time::parse_timestamp(rest[..close].as_bytes())
            .unwrap_or_else(|| panic!("manifest.json {key} is not a timestamp"))
    };
    (field("window_start"), field("window_end"))
}

fn read_catalog(data: &Path) -> Catalog {
    let mut cat_names: Vec<Box<[u8]>> = Vec::new();
    let mut product_cat = ByteMap::new();
    if let Ok(bytes) = std::fs::read(data.join("products.csv")) {
        let body = csv::body_after_header(&bytes);
        let mut fields: Vec<&[u8]> = Vec::with_capacity(4);
        for line in body.split(|&b| b == b'\n') {
            if !csv::split_fields(line, &mut fields) || fields.len() != 2 {
                continue;
            }
            let (product, category) = (fields[0], fields[1]);
            if product.is_empty() {
                continue;
            }
            let id = match cat_names.iter().position(|have| &**have == category) {
                Some(id) => id as u32,
                None => {
                    cat_names.push(category.to_vec().into_boxed_slice());
                    cat_names.len() as u32 - 1
                }
            };
            product_cat.insert(product, id);
        }
    }
    // A product that is not in products.csv has the category `unknown`.
    let unknown = match cat_names.iter().position(|have| &**have == &b"unknown"[..]) {
        Some(id) => id as u32,
        None => {
            cat_names.push(b"unknown".to_vec().into_boxed_slice());
            cat_names.len() as u32 - 1
        }
    };

    let mut campaign_index = ByteMap::new();
    let mut campaign_ids: Vec<Box<[u8]>> = Vec::new();
    let mut campaign_channels: Vec<Box<[u8]>> = Vec::new();
    if let Ok(bytes) = std::fs::read(data.join("campaigns.csv")) {
        let body = csv::body_after_header(&bytes);
        let mut fields: Vec<&[u8]> = Vec::with_capacity(4);
        for line in body.split(|&b| b == b'\n') {
            if !csv::split_fields(line, &mut fields) || fields.len() != 2 {
                continue;
            }
            let (campaign, channel) = (fields[0], fields[1]);
            if campaign.is_empty() || campaign_index.get(campaign).is_some() {
                continue;
            }
            let id = campaign_ids.len() as u32;
            campaign_index.insert(campaign, id);
            campaign_ids.push(campaign.to_vec().into_boxed_slice());
            campaign_channels.push(channel.to_vec().into_boxed_slice());
        }
    }
    // `organic` is a campaign id that never appears in campaigns.csv.
    let organic = campaign_ids.len() as u32;
    campaign_ids.push(b"organic".to_vec().into_boxed_slice());
    campaign_channels.push(b"organic".to_vec().into_boxed_slice());

    Catalog {
        product_cat,
        cat_names,
        unknown,
        campaign_index,
        campaign_ids,
        campaign_channels,
        organic,
    }
}

/// Every *.csv file in `dir`, in name order.
fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(".csv") && path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn parse_events(data: &Path, catalog: &Catalog, users: &StrMap<u32>, events: &StrMap<EvRow>) {
    let files = list_csv(&data.join("events"));
    let bytes_total = total_size(&files);
    events.reserve(bytes_total / 64, bytes_total / 8);
    users.reserve(bytes_total / 384, bytes_total / 192);
    // A few files at a time: their bytes are the biggest thing in memory, and
    // the chunks of one file already fill every core.
    for group in files.chunks(4) {
        group.par_iter().for_each(|path| {
            let Ok(bytes) = std::fs::read(path) else {
                return;
            };
            let body = csv::body_after_header(&bytes);
            csv::chunks(body, CHUNK).par_iter().for_each(|chunk| {
                let mut fields: Vec<&[u8]> = Vec::with_capacity(8);
                let mut pending: Vec<Pending> = Vec::with_capacity(BATCH);
                for line in chunk.split(|&b| b == b'\n') {
                    if !csv::split_fields(line, &mut fields) || fields.len() != 7 {
                        continue;
                    }
                    let id = fields[0];
                    if id.is_empty() {
                        continue;
                    }
                    let (kind, price) = match fields[4] {
                        b"view" => (0u8, -1i64),
                        b"cart" => (1, -1),
                        b"purchase" => (2, parse_price(fields[6])),
                        _ => (255, -1),
                    };
                    let product = fields[5];
                    let flags = if product.is_empty() { 0u8 } else { 1 };
                    pending.push(Pending {
                        id,
                        user: fields[1],
                        ts: time::parse_timestamp(fields[2]),
                        ingest: time::parse_f1(fields[3]),
                        price,
                        cat: catalog.product_cat.get(product).unwrap_or(catalog.unknown),
                        kind,
                        flags,
                    });
                    if pending.len() >= BATCH {
                        flush_events(&pending, users, events);
                        pending.clear();
                    }
                }
                if !pending.is_empty() {
                    flush_events(&pending, users, events);
                }
            });
        });
    }
}

/// Interns the batch's users, then hands the batch to the event_id dedup,
/// which keeps the smallest ingest_ts. Dedup comes before validation.
fn flush_events(pending: &[Pending], users: &StrMap<u32>, events: &StrMap<EvRow>) {
    let keys: Vec<&[u8]> = pending.iter().map(|p| p.user).collect();
    let mut ids = vec![0u32; keys.len()];
    users.intern_batch(&keys, &mut ids);
    let rows: Vec<(&[u8], EvRow)> = pending
        .iter()
        .zip(ids.iter())
        .map(|(p, &user)| {
            (
                p.id,
                EvRow {
                    ts: p.ts.unwrap_or(i64::MIN),
                    ingest: p.ingest.unwrap_or(i64::MAX),
                    price: p.price,
                    user: if p.user.is_empty() { NO_USER } else { user },
                    cat: p.cat,
                    kind: p.kind,
                    flags: p.flags,
                },
            )
        })
        .collect();
    events.upsert_batch(&rows, &|kept, new| {
        if new.ingest < kept.ingest {
            *kept = new;
        }
    });
}

fn parse_clicks(data: &Path, catalog: &Catalog, users: &StrMap<u32>, clicks: &StrMap<CkRow>) {
    let files = list_csv(&data.join("clicks"));
    let bytes_total = total_size(&files);
    clicks.reserve(bytes_total / 48, bytes_total / 8);
    users.reserve(bytes_total / 384, bytes_total / 192);
    for group in files.chunks(4) {
        group.par_iter().for_each(|path| {
            let Ok(bytes) = std::fs::read(path) else {
                return;
            };
            let body = csv::body_after_header(&bytes);
            csv::chunks(body, CHUNK).par_iter().for_each(|chunk| {
                let mut fields: Vec<&[u8]> = Vec::with_capacity(4);
                let mut pending: Vec<ClickPending> = Vec::with_capacity(BATCH);
                for line in chunk.split(|&b| b == b'\n') {
                    if !csv::split_fields(line, &mut fields) || fields.len() != 4 {
                        continue;
                    }
                    let id = fields[0];
                    if id.is_empty() {
                        continue;
                    }
                    pending.push(ClickPending {
                        id,
                        user: fields[1],
                        ts: time::parse_timestamp(fields[3]),
                        campaign: match catalog.campaign_index.get(fields[2]) {
                            Some(id) => id,
                            None => NO_CAMPAIGN,
                        },
                    });
                    if pending.len() >= BATCH {
                        flush_clicks(&pending, users, clicks);
                        pending.clear();
                    }
                }
                if !pending.is_empty() {
                    flush_clicks(&pending, users, clicks);
                }
            });
        });
    }
}

/// Interns the batch's users, then hands the batch to the click_id dedup.
/// Copies of a click hold identical values, so keeping the first is enough.
fn flush_clicks(pending: &[ClickPending], users: &StrMap<u32>, clicks: &StrMap<CkRow>) {
    let keys: Vec<&[u8]> = pending.iter().map(|p| p.user).collect();
    let mut ids = vec![0u32; keys.len()];
    users.intern_batch(&keys, &mut ids);
    let rows: Vec<(&[u8], CkRow)> = pending
        .iter()
        .zip(ids.iter())
        .map(|(p, &user)| {
            (
                p.id,
                CkRow {
                    ts: p.ts.unwrap_or(i64::MIN),
                    user: if p.user.is_empty() { NO_USER } else { user },
                    campaign: p.campaign,
                },
            )
        })
        .collect();
    clicks.upsert_batch(&rows, &|_, _| {});
}

fn total_size(files: &[PathBuf]) -> usize {
    files
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|meta| meta.len() as usize)
        .sum()
}

fn parse_links(data: &Path, users: &StrMap<u32>) -> Vec<(u32, u32)> {
    let files = list_csv(&data.join("links"));
    let parts: Vec<Vec<(u32, u32)>> = files
        .par_iter()
        .map(|path| {
            let mut out = Vec::new();
            let Ok(bytes) = std::fs::read(path) else {
                return out;
            };
            let body = csv::body_after_header(&bytes);
            for chunk in csv::chunks(body, CHUNK) {
                let mut fields: Vec<&[u8]> = Vec::with_capacity(4);
                for line in chunk.split(|&b| b == b'\n') {
                    if !csv::split_fields(line, &mut fields) || fields.len() != 2 {
                        continue;
                    }
                    let (a, b) = (fields[0], fields[1]);
                    if a.is_empty() || b.is_empty() {
                        continue;
                    }
                    out.push((users.intern(a), users.intern(b)));
                }
            }
            out
        })
        .collect();
    parts.into_iter().flatten().collect()
}

/// A purchase's price: ASCII digits only, 1 to 10,000,000 (leading zeros fine).
/// Anything else is -1, which is never a valid price.
fn parse_price(field: &[u8]) -> i64 {
    if field.is_empty() {
        return -1;
    }
    let mut value: i64 = 0;
    for &c in field {
        if !c.is_ascii_digit() {
            return -1;
        }
        value = value * 10 + i64::from(c - b'0');
        if value > 10_000_000 {
            return -1;
        }
    }
    if value < 1 {
        -1
    } else {
        value
    }
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

/// What survives parse, dedup and the rules: a valid user, a ts in the event
/// window, a known type, a non-empty product, a good price for a purchase, and
/// not ingested more than 24 h late.
fn event_survives(row: &EvRow, win_start: i64, win_end: i64) -> bool {
    if row.kind > 2 || row.user == NO_USER || row.flags & 1 == 0 {
        return false;
    }
    if row.ts == i64::MIN || row.ts < win_start || row.ts >= win_end {
        return false;
    }
    if row.kind == 2 && row.price < 1 {
        return false;
    }
    if row.ingest == i64::MAX {
        return false;
    }
    // Exactly 24 h late is kept, and so is an ingest before the ts.
    row.ingest.saturating_sub(row.ts) <= LATE_MS
}

/// A click that can credit or assist: a valid user, a ts in the click window,
/// and a campaign that is in campaigns.csv.
fn click_survives(row: &CkRow, win_start: i64, win_end: i64) -> bool {
    row.user != NO_USER
        && row.campaign != NO_CAMPAIGN
        && row.ts != i64::MIN
        && row.ts >= win_start - CLICK_REACH
        && row.ts < win_end
}

// ---------------------------------------------------------------------------
// linked ids
// ---------------------------------------------------------------------------

fn find(parent: &mut [u32], mut id: u32) -> u32 {
    let mut root = id;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while parent[id as usize] != root {
        let next = parent[id as usize];
        parent[id as usize] = root;
        id = next;
    }
    root
}

fn union(parent: &mut [u32], a: u32, b: u32) {
    let (ra, rb) = (find(parent, a), find(parent, b));
    if ra != rb {
        // Which one becomes the root does not matter.
        parent[rb as usize] = ra;
    }
}