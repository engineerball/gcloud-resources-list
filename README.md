# GCP Resource Inventory Tool

Inventories Google Cloud resources across multiple projects and writes them to per-run CSV directories.
Each run produces a timestamped output directory with one CSV per resource type plus a JSON summary, and a `latest` symlink that always points at the most recent successful run.

## Install

```bash
uv sync
```

Authenticate with Application Default Credentials before running anything:

```bash
gcloud auth application-default login
```

## Usage

Generate a `config.yaml` from the active projects under a GCP folder.
Nested sub-folders are walked too, so every project in the subtree is included:

```bash
uv run gcp-inventory config --folder-id <FOLDER_ID> [--location <region> ...]
```

`--location` is repeatable; if omitted it defaults to `asia-southeast1`.

Run the inventory:

```bash
uv run gcp-inventory run [--config config.yaml] [--output output] [--workers 8] [--collector NAME ...] [--list-collectors] [-v]
```

Repeat `--collector NAME` to run only those collectors.
Collector selection uses the registered collector name, preserves registry order, and defaults to all collectors when the flag is omitted.
Use `--list-collectors` to print every valid name with its GLOBAL or REGIONAL scope without requiring credentials or creating output.

Service-account tools (operate on a single project, independent of `config.yaml`):

```bash
uv run gcp-inventory sa roles --source-project <PROJECT>
```

`sa roles` dumps service-account to project-role bindings to a CSV.

## Output layout

Each `run` writes to `output/<run-timestamp>/`, for example `output/20260719T120000Z/`.
The directory contains:

- `cloud_bigquery_datapolicies.csv`
- `cloud_bigquery_datasets.csv`
- `cloud_bigquery_datasets_tables.csv`
- `cloud_compute_engine_instances.csv`
- `cloud_dataform.csv`
- `cloud_functions.csv`
- `cloud_kms_keys.csv`
- `cloud_run_services.csv`
- `cloud_scheduler.csv`
- `cloud_storage_buckets.csv`
- `cloud_workflows.csv`
- `service_accounts.csv`
- `service_account_roles.csv`
- `run_summary.json`, a machine-readable summary of the run: projects, locations, selected collectors, whether the run was filtered, task counts, rows per collector, and any failures.

Most collectors are constrained to the configured locations.
Compute Engine instances are the exception: they are inventoried project-wide across all zones via `aggregatedList`, with the zone recorded per row, so stray instances outside your expected regions still show up.

`output/latest` is a symlink to the most recent run directory.
The staging directory is only renamed into place once every collector has finished, so a crashed run never corrupts previous output.

## Exit codes

- `0`: every collector task succeeded.
- `2`: partial failure; some collector/project (or collector/project/location) tasks failed.
  Output is still written for the tasks that succeeded.
  Check `run_summary.json` in the run directory for details.
- `1`: fatal error, such as a missing or invalid config file, or missing/invalid credentials.

## Development

```bash
uv run pytest
uv run ruff check src tests
uv run pyright src
```
