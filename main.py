from functions import (
    clean_output_files,
    get_config,
    get_credentials,
    get_bigquery_datapolicies,
    get_bigquery_datasets,
)
from functions import (
    get_dataform,
    get_cloud_function,
    get_cloud_kms,
    get_cloud_run,
    get_cloud_scheduler,
    get_cloud_workflow,
)
from functions import get_cloud_service_account

# Get the project names and location from the config file
projects, location = get_config()

# Authenticate with the service account
credentials, _ = get_credentials()


# Function to get the GCP resource information
def get_gcp_resource_info(projects, location):
    get_bigquery_datapolicies(projects, location)
    get_bigquery_datasets(projects, location)
    get_dataform(projects, location)
    get_cloud_function(projects, location)
    get_cloud_kms(projects, location)
    get_cloud_run(projects, location)
    get_cloud_scheduler(projects, location)
    get_cloud_workflow(projects, location)
    get_cloud_service_account(projects, credentials, location)


if __name__ == "__main__":
    # Clean the output files
    clean_output_files()
    # Get the GCP resource information
    get_gcp_resource_info(projects, location)
