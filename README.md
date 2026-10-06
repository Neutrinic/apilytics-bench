# apilytics-bench

Benchmarks for [apilytics](https://github.com/Neutrinic/apilytics), the Spark connector for
REST APIs: a synthetic paginated JSON API with known answers, and scripts that read it with
apilytics, DuckDB and a naive client at different link speeds.

| Folder | What's in it |
|---|---|
| [`server/`](server/README.md) | The synthetic API: an Axum server, the fixture generator, fault profiles, manifests of the ground truth, and apilytics configs for every fixture |
| [`deploy/`](deploy/install.sh) | Installs the API as systemd services on a Debian or Ubuntu host |
| [`gcp/`](gcp/README.md) | Provisions the API and Spark clients on Google Compute Engine, caps the link speed, and runs the comparison |
| [`results/`](results/gcp/README.md) | Measured results, with the raw rows |

Every result is checked against the dataset's manifest, so a run that's fast but wrong doesn't
count.

## Quick start

To read the API from your own Spark setup:

```sh
sudo deploy/install.sh
cd server
SYNTHETIC_PUBLIC_BASE=http://host:18600 .venv/bin/python generate.py taxi --sizes 100mb
```

Then point apilytics at `configs/taxi-100mb-clean-offset-5000.conf`, after rerunning
`examples.py` with the same `SYNTHETIC_PUBLIC_BASE`, and compare
`SELECT count(*) FROM api.default.records` with `row_count` in the fixture's manifest.

For the link-speed comparison on GCP, see [`gcp/README.md`](gcp/README.md).

## License

Apache 2.0. The datasets are generated from public sources when you run the generator and aren't
part of this repository: NYC TLC trip records, and TPC-H data from DuckDB's `tpch` extension.
