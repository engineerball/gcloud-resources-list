from google.cloud import resourcemanager_v3
import os
import yaml
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-fid", "--folderid",)

def list_projects(folder_id):
    client = resourcemanager_v3.ProjectsClient()
    query = f'parent.type:folder parent.id:{folder_id}'
    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    response = client.search_projects(request=request)

    projects = []
    for project in response:
        if project.state == resourcemanager_v3.Project.State.ACTIVE:
            # projects.append(project.display_name)
            projects.append(project.project_id)

    return projects

def main(folder_id):
    # call the function
    # projects = list_projects("294120713570")
    projects = list_projects(folder_id)
    fname = "config_template.yaml"

    with open(fname, "r") as file:
            config = yaml.safe_load(file)

    config['gcp_folder_id'] = folder_id
    config['projects'] = projects       

    with open('config.yaml', 'w') as yaml_file:
        yaml_file.write( yaml.dump(config, default_flow_style=False, sort_keys=False))
    
    return config


if __name__ == "__main__":
     args = parser.parse_args()
     folder_id = args.folderid
     config = main(folder_id)
     print(config)