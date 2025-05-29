import yaml


# Function to clean up output files
def clean_output_files():
    import os

    # Remove all csv files in the output folder
    for file in os.listdir("output"):
        if file.endswith(".csv"):
            os.remove(f"output/{file}")


def get_config(filname="config.yaml"):
    # Load project names from YAML file
    with open(filname, "r") as file:
        config = yaml.safe_load(file)

    projects = config["projects"]
    location = config["location"]
    return projects, location


def get_credentials():
    from google.auth import default

    return default()


def get_bigquery_datapolicies(projects, location):
    from google.cloud import bigquery_datapolicies_v1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting BigQuery Data Policies for project: {project}")
            # Create the client.
            client = bigquery_datapolicies_v1.DataPolicyServiceClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = bigquery_datapolicies_v1.ListDataPoliciesRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_data_policies(request=request)
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open(
                "output/cloud_bigquery_datapolicies.csv", "a", newline=""
            ) as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Policies"])

                for service in page_result:
                    print(f"Service: {service.name}")
                    writer.writerow([project, service.name])
        except Exception as e:
            print(
                f"Failed to get Cloud BigQuery Data Policies for project: {project}. Error: {str(e)}"
            )
            continue


def get_bigquery_datatransfer(projects, location):
    from google.cloud import bigquery_datatransfer_v1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting Cloud Run services for project: {project}")
            # Create the client.
            client = bigquery_datatransfer_v1.DataTransferServiceClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = bigquery_datatransfer_v1.ListTransferConfigsRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_transfer_configs(request=request)
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open(
                "output/cloud_bigquery_datatransfer.csv", "a", newline=""
            ) as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Service"])

                for service in page_result:
                    print(f"Service: {service.display_name}")
                    writer.writerow([project, service.display_name])
        except Exception as e:
            print(
                f"Failed to get Cloud BigQuery Data Transfer services for project: {project}. Error: {str(e)}"
            )
            continue


def get_bigquery_datasets(projects, location):
    from google.cloud import bigquery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting BigQuery Datasets for project: {project}")
            # Create the client.
            client = bigquery.Client(project=project)

            # Make the request
            page_result = client.list_datasets()
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_bigquery_datasets.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Dataset"])

                for service in page_result:
                    print(f"Service: {service.dataset_id}")
                    writer.writerow([project, service.dataset_id])
        except Exception as e:
            print(
                f"Failed to get Cloud BigQuery Datasets for project: {project}. Error: {str(e)}"
            )
            continue

def get_bigquery_datasets_tables(projects, location):
    from google.cloud import bigquery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting BigQuery Datasets for project: {project}")
            # Create the client.
            client = bigquery.Client(project=project)

            # Make the request
            page_result = client.list_datasets()
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_bigquery_datasets_tables.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Dataset", "Table"])

                for service in page_result:
                    print(f"Service: {service.dataset_id}")
                    # for tables in client.list_tables(service.data)
                    tables = client.list_tables(service.dataset_id)
                    for table in tables:
                        writer.writerow([table.project, table.dataset_id, table.table_id])

        except Exception as e:
            print(
                f"Failed to get Cloud BigQuery Datasets for project: {project}. Error: {str(e)}"
            )
            continue


def get_bigquery_jobs(projects, location):
    from google.cloud import bigquery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting BigQuery Jobs for project: {project}")
            # Create the client.
            client = bigquery.Client(project=project)

            # Make the request
            page_result = client.list_jobs()
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_bigquery_jobs.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Job"])

                for service in page_result:
                    print(f"Service: {service.job_id}")
                    writer.writerow([project, service.job_id])
        except Exception as e:
            print(
                f"Failed to get Cloud BigQuery Jobs for project: {project}. Error: {str(e)}"
            )
            continue


def get_dataform(projects, location):
    from google.cloud import dataform_v1beta1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting Cloud Run services for project: {project}")
            # Create the client.
            client = dataform_v1beta1.DataformClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = dataform_v1beta1.ListRepositoriesRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_repositories(request=request)

            # Write to csv file with project and service
            with open("output/cloud_dataform.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Repository", "Workspace"])

                for repository in page_result:
                    print(f"Repository: {repository.name}")
                    workspace_list = client.list_workspaces(parent=repository.name)
                    for workspace in workspace_list:
                        print(f"Workspace: {workspace.name}")
                        writer.writerow(
                            [
                                project,
                                repository.name.split("/")[-1],
                                workspace.name.split("/")[-1],
                            ]
                        )
        except Exception as e:
            print(
                f"Failed to get Cloud Dataform  for project: {project}. Error: {str(e)}"
            )
            continue


def get_cloud_function(projects, location):
    from google.cloud import functions_v2
    import csv

    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Functions for the project
            print(f"Getting Cloud Functions for project: {project}")
            # Create the client.
            client = functions_v2.FunctionServiceClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = functions_v2.ListFunctionsRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_functions(request=request)

            # Write to csv file with project and function
            with open("output/cloud_functions.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Function"])

                for function in page_result:
                    print(f"Function: {function.name}")
                    writer.writerow([project, function.name.split("/")[-1]])
        except Exception as e:
            print(
                f"Failed to get Cloud Functions for project: {project}. Error: {str(e)}"
            )
            continue


def get_cloud_kms(projects, location):
    from google.cloud import kms
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get KMS keys for the project
            print(f"Getting KMS keys for project: {project}")
            # Create the client.
            client = kms.KeyManagementServiceClient()

            # Build the parent location name.
            location_name = f"projects/{project}/locations/{location}"

            # Call the API.
            key_rings = client.list_key_rings(request={"parent": location_name})

            for key_ring in key_rings:
                # Get crypto key from keyring
                cryptoKeys = client.list_crypto_keys(request={"parent": key_ring.name})

                # Write to csv file with project, keyring and crypto key
                with open("output/cloud_kms_keys.csv", "a", newline="") as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:
                        writer.writerow(["Project", "KeyRing", "CryptoKey"])

                    for cryptoKey in cryptoKeys:
                        print(f"KeyRing: {key_ring.name}, CryptoKey: {cryptoKey.name}")
                        writer.writerow(
                            [
                                project,
                                key_ring.name.split("/")[-1],
                                cryptoKey.name.split("/")[-1],
                            ]
                        )
        except Exception as e:
            print(f"Failed to get KMS keys for project: {project}. Error: {str(e)}")
            continue


def get_cloud_run(projects, location):
    from google.cloud import run_v2
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Run services for the project
            print(f"Getting Cloud Run services for project: {project}")
            # Create the client.
            client = run_v2.ServicesClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = run_v2.ListServicesRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_services(request=request)
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_run_services.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Service"])

                for service in page_result:
                    print(f"Service: {service.name}")
                    writer.writerow([project, service.name.split("/")[-1]])
        except Exception as e:
            print(
                f"Failed to get Cloud Run services for project: {project}. Error: {str(e)}"
            )
            continue


def get_cloud_scheduler(projects, location):
    from google.cloud import scheduler_v1beta1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting Cloud Run services for project: {project}")
            # Create the client.
            client = scheduler_v1beta1.CloudSchedulerClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = scheduler_v1beta1.ListJobsRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_jobs(request=request)
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_scheduler.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Scheduler"])

                for service in page_result:
                    print(f"Service: {service.name}")
                    writer.writerow([project, service.name.split("/")[-1]])
        except Exception as e:
            print(
                f"Failed to get Cloud Scheduler services for project: {project}. Error: {str(e)}"
            )
            continue


def get_cloud_storage(projects, location, credentials):
    from google.cloud import storage
    from googleapiclient import discovery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        # Get buckets for the project
        print(f"Buckets for project: {project}")
        service = discovery.build("storage", "v1", credentials=credentials)
        request = service.buckets().list(project=project)
        response = request.execute()

        if "items" in response:
            # Open CSV file for writing
            with open("output/cloud_storage_buckets.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(
                        ["Project", "Bucket_Name"]
                    )  # Write header only if file is empty

                # Write the name of each bucket to the CSV file
                for bucket in response["items"]:
                    print(f"Bucket Name: {bucket['name']}")
                    writer.writerow([project, bucket["name"]])
        else:
            print(f"No buckets found for project: {project}")


def get_cloud_workflow(projects, location):
    from google.cloud import workflows_v1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting Cloud Run services for project: {project}")
            # Create the client.
            client = workflows_v1.WorkflowsClient()

            parent = f"projects/{project}/locations/{location}"

            # Initialize request argument(s)
            request = workflows_v1.ListWorkflowsRequest(
                parent=parent,
            )

            # Make the request
            page_result = client.list_workflows(request=request)
            # for response in page_result:
            #     print(response)

            # Write to csv file with project and service
            with open("output/cloud_workflows.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(["Project", "Service"])

                for service in page_result:
                    print(f"Service: {service.name}")
                    writer.writerow([project, service.name.split("/")[-1]])
        except Exception as e:
            print(
                f"Failed to get Cloud Run services for project: {project}. Error: {str(e)}"
            )
            continue


def get_cloud_service_account(projects, location, credentials):
    from googleapiclient import discovery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        # Get service accounts for the project
        print(f"Service accounts for project: {project}")
        service = discovery.build("iam", "v1", credentials=credentials)
        request = service.projects().serviceAccounts().list(name=f"projects/{project}")
        response = request.execute()

        if "accounts" in response:
            # Open CSV file for writing
            with open("output/service_accounts.csv", "a", newline="") as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(
                        ["Project", "Email"]
                    )  # Write header only if file is empty

                # Write the name and email of each service account to the CSV file
                for service_account in response["accounts"]:
                    print(
                        f"Name: {service_account['name']}, Email: {service_account['email']}"
                    )
                    writer.writerow([project, service_account["email"]])
        else:
            print(f"No service accounts found for project: {project}")


def get_compute_engine_instance(projects, location):
    from google.cloud import compute_v1
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get Cloud Workflows for the project
            print(f"Getting Compute Engine instance list for project: {project}")
            # Create the client.
            client = compute_v1.InstancesClient()

            parent = f"projects/{project}/locations/{location}"

            for zone in ["a", "b", "c"]:
                # Initialize request argument(s)
                request = compute_v1.ListInstancesRequest(
                    project=project,
                    zone=f"{location}-{zone}"
                )

                # Make the request
                page_result = client.list(request=request)
                # for response in page_result:
                #     print(response)

                # Write to csv file with project and service
                with open(
                    "output/cloud_compute_engine_instances.csv", "a", newline=""
                ) as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:
                        writer.writerow(["Project", "Instances", "Zone"])

                    for service in page_result:
                        print(f"Service: {service.name}")
                        writer.writerow([project, service.name, service.zone.split("/")[-1]])
        except Exception as e:
            print(
                f"Failed to get Cloud Compute Engine instances for project: {project}. Error: {str(e)}"
            )
            continue
        
        
def get_cloud_iam_roles(projects, location, credentials):
    from googleapiclient import discovery
    import csv

    # Loop through each project
    for project in projects:
        print(f"Authenticating with project: {project}")

        try:
            # Get IAM roles for the project
            print(f"Getting IAM roles for project: {project}")
            service = discovery.build("iam", "v1", credentials=credentials)
            resource_manager = discovery.build("cloudresourcemanager", "v1", credentials=credentials)
            
            # Get all service accounts first
            service_accounts = service.projects().serviceAccounts().list(name=f"projects/{project}").execute()
            
            if "accounts" in service_accounts:
                # Open CSV file for writing
                with open("output/service_account_roles.csv", "a", newline="") as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:
                        writer.writerow(["Project", "Service Account", "Role", "Binding Type"])
                    
                    # For each service account, get its roles
                    for service_account in service_accounts["accounts"]:
                        service_account_email = service_account["email"]
                        print(f"Getting roles for service account: {service_account_email}")
                        
                        # 1. Get direct IAM policy for the service account
                        try:
                            policy = service.projects().serviceAccounts().getIamPolicy(
                                resource=service_account["name"]
                            ).execute()
                            
                            if "bindings" in policy:
                                for binding in policy["bindings"]:
                                    role = binding["role"]
                                    print(f"Direct Role: {role}")
                                    writer.writerow([project, service_account_email, role, "Direct"])
                        except Exception as e:
                            print(f"Error getting direct IAM policy: {str(e)}")
                        
                        # 2. Get project-level IAM policy
                        try:
                            project_policy = resource_manager.projects().getIamPolicy(
                                resource=project,
                                body={}
                            ).execute()
                            
                            if "bindings" in project_policy:
                                for binding in project_policy["bindings"]:
                                    role = binding["role"]
                                    members = binding.get("members", [])
                                    if f"serviceAccount:{service_account_email}" in members:
                                        print(f"Project Role: {role}")
                                        writer.writerow([project, service_account_email, role, "Project"])
                        except Exception as e:
                            print(f"Error getting project IAM policy: {str(e)}")
                        
                        # 3. Get organization-level IAM policy if available
                        try:
                            # Get organization ID from project
                            project_info = resource_manager.projects().get(projectId=project).execute()
                            if "parent" in project_info and project_info["parent"]["type"] == "organization":
                                org_id = project_info["parent"]["id"]
                                org_policy = resource_manager.organizations().getIamPolicy(
                                    resource=f"organizations/{org_id}",
                                    body={}
                                ).execute()
                                
                                if "bindings" in org_policy:
                                    for binding in org_policy["bindings"]:
                                        role = binding["role"]
                                        members = binding.get("members", [])
                                        if f"serviceAccount:{service_account_email}" in members:
                                            print(f"Organization Role: {role}")
                                            writer.writerow([project, service_account_email, role, "Organization"])
                        except Exception as e:
                            print(f"Error getting organization IAM policy: {str(e)}")
            else:
                print(f"No service accounts found for project: {project}")
                
        except Exception as e:
            print(f"Failed to get IAM roles for project: {project}. Error: {str(e)}")
            continue