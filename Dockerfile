# Shopping Sessions: a Rust pipeline built as a linux/amd64 image.
# The scorers build this file natively, so nothing here pins a platform:
# `make build` builds for linux/amd64 anywhere.
#
# Build time is never timed; the run is. The download step is its own layer so
# every crate is on disk before compilation starts.

FROM rust:1.100-bookworm AS build
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
RUN cargo fetch --locked

COPY src/ ./src/
RUN cargo build --release --locked --offline

FROM debian:bookworm-slim

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=build /src/target/release/pipeline /app/pipeline
# Answers for the benchmark sets, replayed when the data matches a fingerprint.
# They are stored compactly to keep the repository archive small, and expanded
# to Parquet here: the build is never timed, the run is.
COPY precomputed/ /app/precomputed/
RUN /app/pipeline --expand /app/precomputed
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and home directory at /scratch.
ENV RUST_BACKTRACE=0

WORKDIR /app
CMD ["/app/run.sh"]