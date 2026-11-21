# Shopping Sessions

Turns the dirty click-stream logs in `/data` into the two daily Parquet tables
`/output/user_daily/` and `/output/campaign_daily/`, exactly as
[`rules.md`](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)
defines them. One right answer, and the clock runs from container start to
container exit.

Engine: **Rust**, one static binary, no runtime dependencies. It reads the CSVs
with its own parser (the dialect rules are exact and no off-the-shelf reader
matches them), keeps the shared state in a sharded byte-string map that takes
one lock per shard per batch instead of one per row, and writes Parquet with the
`parquet` crate.

## What it does

In the order the rules ask for:

1. **Links first.** Every `links/*.csv` line joins two ids into one person,
   transitively, and each person is named by the byte-wise smallest of their
   ids — even an id that appears in no event or click. Everything below says
   "person" and means this group.
2. **Events.** Parse each line against the CSV dialect (quotes are syntax, `\r`
   is an ordinary character, an empty field is null) -> dedup on `event_id`,
   keeping the copy with the smallest `ingest_ts`, before any validation ->
   validate the kept copy (`user_id`, `ts` in the window, `view`/`cart`/
   `purchase`, a non-empty product, a price of 1..10,000,000 for a purchase) ->
   drop what was ingested more than 24 h late -> sessionize each person's
   events, a new session on a gap of over 30 min, the UTC date of the session
   start being its day.
3. **Clicks.** Parse -> dedup on `click_id` -> validate (`user_id`, `ts` in the
   click window that starts 7 days early, a campaign in `campaigns.csv`) ->
   credit each session to the latest qualifying click at or before its start
   within 7 days, ties going to the smallest `click_id`, and `organic` when
   there is none -> count assists: every campaign the person clicked in that
   same 7-day window assists the session once, however many of its clicks
   qualify.
4. **Aggregate** per (person, day) and per (campaign, day) and write the two
   tables as Parquet, `day` as a `DATE`.

## Layout

| Path | What |
| --- | --- |
| `src/main.rs` | the pipeline: parse, dedup, people, sessions, attribution, assists |
| `src/csv.rs` | the CSV dialect: split a line into fields, malformed rules |
| `src/time.rs` | the three timestamp forms F1, F2, F3 and the calendar checks |
| `src/strmap.rs` | the sharded byte-string map: interning and dedup, batched |
| `src/output.rs` | the two Parquet tables |
| `fixtures/` | the 36 edge cases with hand-written expected tables |
| `scripts/` | the test scripts from the starter, unchanged |

## Test

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline   # 36/36
```

Then the 1M-event dev set, with `generate` built from the track page's
`generator.zip` and `dev-answer.zip` unzipped here:

```sh
make dev          # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev      # build the image, run it on dev/ with scorer flags, time it
make check-dev    # exact diff against dev-answer/
```

`make fixtures` runs the same 36 cases inside the image the way the scorer runs
it (linux/amd64, 8 vCPUs, 24 GB, no network, read-only root, uid 1000).

## Container

The image is a two-stage build: `rust:1.100-bookworm` compiles the binary,
`debian:bookworm-slim` runs it. The crate downloads are their own layer, so the
build needs network only to fetch them. The run needs nothing: no network, no
temp files outside `/scratch`, and it writes only `/output`.

Measured on the published generator, one run each, on a 2025 Apple M5 laptop
(4 performance + 6 efficiency cores, so the scorer's 8 vCPUs of Intel Xeon
should land in the same ballpark):

| Dataset | Events | Wall | Peak RSS |
| --- | --- | --- | --- |
| dev (seed 1) | 1M | 0.2 s | 0.3 GB |
| practice (seed 2) | 20M | 2.7 s | 2.8 GB |
| final (seed 3) | 60M | 13.4 s | 4.9 GB |

The dev output matches `dev-answer.zip` exactly, and the larger runs satisfy
the rules' self-checks: summed `sessions` and `revenue_cents` are equal in both
tables, and summed `assisted_sessions` is at least the credited, non-`organic`
`sessions`.

## Notes on the rules

A few places where the rules are exact and this implementation follows them
literally:

- **Malformed is not the same as invalid.** A malformed line (wrong field
  count, a quote that is not around a whole field, an empty line) is dropped
  before dedup and never becomes the kept copy. A bad timestamp or a bad price
  is not malformed: the line still takes part in dedup, and if it wins, the
  event is dropped and a later valid copy does not rescue it.
- **Digits are ASCII.** `٢٥٠` and `２５０` are not digits: prices, F3 timestamps
  and years are read with explicit `0`..= `9` checks.
- **`\r` is data.** Only `\n` ends a line, so a purchase priced `1000\r` has a
  bad price and a click `ts` of `...Z\r` has a bad `ts`.
- **Spaces are data.** ` u1`, `u1 ` and `" u1"` are three different users and
  none of them is `u1`.
- **The windows.** An event's `ts` is in `[window_start, window_end)`; a
  click's `ts` is in `[window_start - 7 days, window_end)`, both ends of that
  one inclusive; a click at exactly the session start counts, 1 ms after it does
  not; a click exactly 7 days before the start counts, 7 days and 1 ms before
  does not.
- **A gap of exactly 30 minutes** keeps one session; 30 min and 1 ms splits.
  Events that share a `ts` all join the session the first of them joins.
- **`organic`** is a campaign id that never appears in `campaigns.csv`, its
  channel is `organic`, and it never assists.
- **Top category** is by summed purchase price over the day's sessions, ties to
  the byte-wise smallest name, `unknown` competing like any other, null with no
  purchases.

The timestamps parse to integer milliseconds with a hand-written reader: F1
`2026-10-03T23:58:10Z`, F2 `2026-10-03T19:58:10-04:00` (hours 00..23, minutes
00..59, with the colon), F3 digits only with leading zeros allowed. Everything
else is a bad timestamp: fractional seconds, a lowercase `z` or `t`, a space
for `T`, no zone, an offset out of range or without its colon, and impossible
calendar values such as February 30 or February 29 outside a leap year.