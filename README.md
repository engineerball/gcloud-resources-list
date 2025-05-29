# GCP Resource Inventory Tool

This tool helps you inventory and document various Google Cloud Platform (GCP) resources across multiple projects. It collects information about different GCP services and exports the data to CSV files for easy analysis and documentation.

## Features

The tool collects information about the following GCP resources:

- BigQuery
  - Data Policies
  - Data Transfer Configurations
  - Datasets and Tables
  - Jobs
- Cloud Functions
- Cloud KMS Keys
- Cloud Run Services
- Cloud Scheduler Jobs
- Cloud Storage Buckets
- Cloud Workflows
- Compute Engine Instances
- Dataform Repositories and Workspaces
- IAM Roles and Service Accounts

## Prerequisites

- Python 3.6 or higher
- Google Cloud SDK installed and configured
- Appropriate GCP permissions to access the resources you want to inventory

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install the required Python packages:
```bash
pip install -r requirements.txt
```

## Usage

1. Ensure you have the correct GCP credentials:
```bash
gcloud auth application-default login
```

2. Run the script for generate config:
```bash
python build_config.py --fid=<GCP_FOLDER_ID>
```

3. Run the script:
```bash
python main.py
```

The script will:
- Create an `output` directory if it doesn't exist
- Generate CSV files for each resource type
- Show progress in the console
- Handle errors gracefully and continue with other resources

## Output Files

The tool generates the following CSV files in the `output` directory:

- `cloud_bigquery_datapolicies.csv`: BigQuery data policies
- `cloud_bigquery_datatransfer.csv`: BigQuery data transfer configurations
- `cloud_bigquery_datasets.csv`: BigQuery datasets
- `cloud_bigquery_datasets_tables.csv`: BigQuery tables
- `cloud_bigquery_jobs.csv`: BigQuery jobs
- `cloud_dataform.csv`: Dataform repositories and workspaces
- `cloud_functions.csv`: Cloud Functions
- `cloud_kms_keys.csv`: KMS key rings and crypto keys
- `cloud_run_services.csv`: Cloud Run services
- `cloud_scheduler.csv`: Cloud Scheduler jobs
- `cloud_storage_buckets.csv`: Cloud Storage buckets
- `cloud_workflows.csv`: Cloud Workflows
- `service_accounts.csv`: Service accounts
- `service_account_roles.csv`: IAM roles assigned to service accounts
- `cloud_compute_engine_instances.csv`: Compute Engine instances

## Error Handling

The tool includes error handling for:
- API rate limits
- Permission issues
- Network problems
- Missing resources

Errors are logged to the console but won't stop the entire process. The tool will continue with other resources even if some fail.

## Contributing

Feel free to submit issues and enhancement requests!

## License

[Your chosen license]
