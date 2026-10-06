# Synthetic API

A paginated JSON API over generated datasets whose answers are known in advance. Each fixture
comes with a manifest of its row count, sums, null counts and more, so a client's result can be
checked exactly, not just timed. Fault profiles add latency, rate limits, server errors and TCP
resets.

The server is Rust (Axum on Tokio/Hyper). Generation, verification and profile control are Python.

## Fixtures and endpoints

A fixture is `/{dataset}/{size}/{variant}`:

| Part | Values |
|---|---|
| dataset | `taxi` (NYC yellow taxi trips, 2024) or `lineitem` (derived from TPC-H `lineitem`) |
| size | `100mb`, `2gb`, `10gb`: uncompressed NDJSON bytes |
| variant | `clean`, or `messy` with injected type and shape problems |

`/{dataset}` alone means `/{dataset}/100mb/clean`.

| Endpoint | Response |
|---|---|
| `/offset?offset=0&limit=5000` | `{"results": [...]}`; empty `results` past the end |
| `/cursor?cursor=0&limit=5000` | `{"items": [...], "next": "5000"}`; `next` is empty on the last page |
| `/link?offset=0&limit=5000` | the offset page, with an absolute `Link: <…>; rel="next"` header, absent on the last page |
| `/export.ndjson` | the whole fixture, one JSON object per line |
| `/openapi.yaml` | an OpenAPI 3.0.3 spec of the endpoints and the record schema |
| `/manifest.json` | the fixture's ground truth |
| `/health` | `{"status": "ok", "ready": n}`, the number of complete fixtures |

- **Page sizes are 500 or 5000.** Offsets must be non-negative and aligned to the page size;
  anything else is a 400. An offset at or past the end returns an empty page.
- **Cursors** are decimal offsets, to be treated as opaque strings. An omitted cursor starts at 0.
- **Sizes** are targets: generation stops at the first whole record that reaches them, so clean
  and messy row counts differ slightly. No records are repeated to reach a size.
- **An unfinished fixture** returns 503, never a partial dataset.

Pages and the export are stored as pre-rendered gzip files. A client that accepts gzip gets them
as stored; one that doesn't gets a decompressed stream, or a plain copy if one sits beside the
gzip file. `gzip;q=0` is respected. Serving does no JSON rendering and never loads a whole
dataset into memory.

## Running it

**As services**, on Debian or Ubuntu:

```sh
sudo deploy/install.sh        # see the script's header for its settings
```

It runs Axum on loopback and Toxiproxy in front of it on port 18600. Toxiproxy is what turns
`--resets` into real TCP resets. Neither service starts at boot; use `control.py start`.

**Directly**, without Toxiproxy (so no TCP resets):

```sh
cd server
cargo build --release --locked --manifest-path axum/Cargo.toml
SYNTHETIC_ROOT=. SYNTHETIC_LISTEN=0.0.0.0:18600 SYNTHETIC_PUBLIC_BASE=http://host:18600 \
  axum/target/release/synthetic-rest-axum
```

| Variable | Default | Meaning |
|---|---|---|
| `SYNTHETIC_ROOT` | `.` | the directory holding `data/` and `profile.json` |
| `SYNTHETIC_LISTEN` | `127.0.0.1:18601` | the address the server binds |
| `SYNTHETIC_PUBLIC_BASE` | `http://127.0.0.1:18600` | the URL clients use, for `Link` headers |

## Fault profiles

```sh
.venv/bin/python control.py profile --latency 150          # 0, 50, 150 or 300 ms
.venv/bin/python control.py profile --rps 20 --retry-after 2 --retry-format seconds   # or date
.venv/bin/python control.py profile --errors .02 --resets .01
.venv/bin/python control.py reset                          # no latency, no limit, no faults
.venv/bin/python control.py start | stop | status          # the services, after install.sh
```

- **Changes apply to the next request**, with no restart. Change profiles between measured runs.
- **Latency** is added before the response headers, per request. It isn't a per-packet delay,
  and bandwidth isn't throttled.
- **The rate limit** is one global fixed one-second window, not a per-client quota. Requests over
  it get 429 with `Retry-After` in seconds or as an HTTP date.
- **`--errors`** is the probability of an HTTP 500, per request. It's seeded (`--seed`), but which
  requests fail depends on their order.
- **`--resets`** is the probability that Toxiproxy resets a TCP connection, per connection, not per
  request on a reused connection.
- **Metadata and health** (`/openapi.yaml`, `/manifest.json`, `/health`) skip latency, rate limits
  and 500s, but TCP resets can hit any connection.

## Generating fixtures

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.lock
SYNTHETIC_PUBLIC_BASE=http://host:18600 .venv/bin/python generate.py taxi --seed 20261004
SYNTHETIC_PUBLIC_BASE=http://host:18600 .venv/bin/python generate.py lineitem --seed 20261004
```

- **Sources.** `taxi` downloads NYC TLC 2024 yellow taxi Parquet files, month by month, into
  `sources/`. `lineitem` runs DuckDB's `tpch` extension (`CALL dbgen(sf=5)`). Neither the sources
  nor the generated data are committed; `source-files.json` records the source files' hashes.
- **`lineitem` is a workload derived from TPC-H's table, not a TPC-H benchmark.** Don't publish
  results under the TPC-H name.
- **Options:** `--sizes 100mb` limits a run; `--rate .02` sets the messy variant's default
  probability; `--rates '{"missing":0.04}'` overrides categories. The categories are `nested`,
  `arrays`, `null`, `missing`, `extra`, `numeric_string`, `fraction`, `large_id` and `timestamp`.
  Probabilities are per record and category, and categories can overlap.
- **Clean fixtures** only convert values to JSON and add `benchmark_id`. They keep the source's
  own messiness and inject nothing. Both variants declare the same schema, so the messy one's
  injected values deliberately test type conversion.
- **Some fields are synthetic.** The nested `pickup` coordinates and `fees` objects aren't TLC
  data; they exist to exercise nested and array handling. Dates become midnight timestamps, and
  TLC's zone-less timestamps get a `Z` by convention.
- **Memory.** The ground-truth query uses a 6 GB DuckDB budget. Allow about 12 GB for the 10 GB
  fixtures. Complete fixtures are skipped on a rerun; to regenerate one, move its directory aside.
- **If the ground-truth step fails** after rendering, wait for the generator to stop, then run
  `recover.py taxi` (or `lineitem`). It rebuilds the export from the persisted Parquet and
  continues from the same source position, with the same seed.
- **Reproducibility:** `repro_check.py` regenerates the 100 MB fixtures and compares their
  manifests. Exact prefixes need the same source files, seed and library versions
  (`requirements.lock`).

## Ground truth

Each fixture's `rendered.parquet` holds every record's JSON exactly as served, injections
included. The manifest is computed by scanning it, not by counting injections:

- row count, uncompressed bytes and the export's SHA-256
- null counts (missing included) and missing counts per field
- sums, minimums and maximums of numeric fields, as `DECIMAL(38,10)`; numeric strings are parsed,
  and invalid, null and missing values are excluded
- timestamp bounds and invalid counts
- distinct counts of identifier fields, by their text
- the seed and rates

`manifests/` holds a copy of each fixture's manifest.

## Checks

Run on the server host, with `SYNTHETIC_TEST_BASE` set to the server's URL:

| Script | Checks |
|---|---|
| `verify.py` | the 100 MB fixtures' pages and export against their manifests, then every fault profile |
| `verify_axum.py` | page boundaries, methods, gzip negotiation and `Link` headers on all twelve fixtures |
| `verify_large.py` | streams every export and checks its hash, size and row count |
| `benchmark_server.py` | throughput of concurrent clients fetching one page; run from another machine to include the network |

## apilytics configs

`examples.py` writes an apilytics config into `configs/` for every fixture, page size and
pagination style, plus one that reads `/export.ndjson` in variant mode. The committed configs use
`http://127.0.0.1:18600`; set `SYNTHETIC_PUBLIC_BASE` and rerun it for another host.
