import os
import requests
import json
import pandas as pd
import subprocess
from subprocess import run
import argparse



snyk_token = os.environ['snyk_token']
snyk_org = os.environ['snyk_org']
github_token = os.environ['github_token']
organization = os.environ['repo_owner']
repo_name = os.environ['repo_name']
pull_req = os.environ['pr_number']
req_files = os.environ['req_files']
snyk_org_name = os.environ['snyk_org_name']


AUTH_HEADER = {
  'Authorization': snyk_token,
  'Content-Type': 'application/vnd.api+json'
};

#github authorization
headers = {
  'Accept': 'application/vnd.github+json',
  'Accept': 'application/vnd.github.html+json',
  'Authorization': f'Bearer {github_token}',
  'X-GitHub-Api-Version': '2022-11-28',
}


def get_project_ids(snyk_org):
    """
    Get project IDs for a given organization from the Snyk API.

    Args:
        snyk_org (str): The Snyk organization identifier.

    Returns:
        list: A list of project IDs.
    """
    try:
        # Get all project IDs for a given organization
        response = requests.get(
            f'https://api.snyk.io/rest/orgs/{snyk_org}/projects?version=2023-08-04',
            headers=AUTH_HEADER
        )
        response.raise_for_status()  # Check for HTTP errors
        project_data = response.json()['data']
        project_ids = [project['id'] for project in project_data]
        print(f"Successfully retrieved project IDs: {project_ids}")
        return project_ids
    except Exception as e:
        print(f"Error in get_project_ids: {str(e)}")
        return []

def get_issue_counts(snyk_org, project_id):
    """
    Get issue counts for a specific project from the Snyk API.

    Args:
        snyk_org (str): The Snyk organization identifier.
        project_id (str): The identifier of the Snyk project.

    Returns:
        dict or None: A dictionary containing issue counts or None if there's an error.
    """
    try:
        # Get issue counts for a project
        response = requests.get(
            f'https://api.snyk.io/rest/orgs/{snyk_org}/projects/{project_id}?meta.latest_issue_counts=true&version=2023-08-21',
            headers=AUTH_HEADER
        )
        response.raise_for_status()  # Check for HTTP errors
        issue_counts = response.json()['data']['meta']['latest_issue_counts']
        print(f"Successfully retrieved issue counts: {issue_counts}")
        return issue_counts
    except Exception as e:
        print(f"Error in get_issue_counts: {str(e)}")
        return None

def get_aggregated_issues(snyk_org, project_id):
    """
    Get aggregated issues for a specific project from the Snyk API.

    Args:
        snyk_org (str): The Snyk organization identifier.
        project_id (str): The identifier of the Snyk project.

    Returns:
        list: A list of aggregated issues or an empty list if there's an error.
    """
    try:
        # Get aggregated issues for a project
        data = {
            "includeDescription": False,
            "includeIntroducedThrough": False,
        }
        response = requests.post(
            f'https://api.snyk.io/v1/org/{snyk_org}/project/{project_id}/aggregated-issues',
            headers=AUTH_HEADER, data=data
        )
        response.raise_for_status()  # Check for HTTP errors
        aggregated_issues = json.loads(response.text)['issues']
        print(f"Successfully retrieved aggregated issues: {len(aggregated_issues)}")
        return aggregated_issues
    except Exception as e:
        print(f"Error in get_aggregated_issues: {str(e)}")
        return []

def create_issue_list_dataframe(issues):
    """
    Create a pandas DataFrame from a list of issues.

    Args:
        issues (list): A list of issue data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the issue data.
    """
    try:
        # Create a pandas DataFrame from the list of issues
        fixable = [
            [
                x['id'], x['issueType'], x['pkgName'], str(x['pkgVersions']),
                x['priorityScore'], x['issueData']['title'], x['issueData']['severity'],
                x['issueData']['language'], x['fixInfo']['isFixable'],
                str(x['fixInfo']['fixedIn'])
            ]
            for x in issues if x['fixInfo']['isFixable'] == True
        ]

        non_fixable = [
            [
                x['id'], x['issueType'], x['pkgName'], str(x['pkgVersions']),
                x['priorityScore'], x['issueData']['title'], x['issueData']['severity'],
                x['issueData']['language'], x['fixInfo']['isFixable'], 'NA'
            ]
            for x in issues if x['fixInfo']['isFixable'] == False
        ]

        total_issues = fixable + non_fixable

        df = pd.DataFrame(
            total_issues,
            columns=['id', 'issueType', 'pkgName', 'pkgVersions', 'priorityScore',
                     'issue_title', 'issue_severity', 'language', 'fixInfo_isFixable', 'fixInfo_fixedIn']
        )
        print("Successfully created DataFrame from issues.")
        return df
    except Exception as e:
        print(f"Error in create_issue_list_dataframe: {str(e)}")
        return pd.DataFrame()

def create_issue_list_html(df):
    """
    Convert a pandas DataFrame to an HTML representation.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be converted.

    Returns:
        str: The HTML representation of the DataFrame.
    """
    try:
        # Convert the DataFrame to HTML
        issue_list = df.to_html()
        print("Successfully converted DataFrame to HTML.")
        return issue_list
    except Exception as e:
        print(f"Error in create_issue_list_html: {str(e)}")
        return ""


def get_latest_snapshot_url(snyk_org, project_id):
    """
    Get the URL for the latest snapshot of a Snyk project.

    Args:
        snyk_org (str): The Snyk organization identifier.
        project_id (str): The identifier of the Snyk project.

    Returns:
        str or None: The URL for the latest snapshot or None if there's an error.
    """
    try:
        # Get the URL for the latest snapshot.
        response = requests.post(
            f'https://api.snyk.io/v1/org/{snyk_org}/project/{project_id}/history?perPage=1&page=1',
            headers=AUTH_HEADER
        )
        response.raise_for_status()  # Check for HTTP errors.
        snapshot_id = response.json()['snapshots'][0]['id']

        # Get the name of the Snyk organization.
        #snyk_org_name = get_snyk_org_name_by_id(snyk_org)

        if snyk_org_name is not None:
            # Construct the URL for the latest snapshot.
            snapshot_url = f'https://app.snyk.io/org/{snyk_org_name}/project/{project_id}/history/{snapshot_id}'

            # Print the URL for the latest snapshot.
            print(f"Successfully retrieved latest snapshot URL: {snapshot_url}")

            # Return the URL for the latest snapshot.
            return snapshot_url
        else:
            print("Could not get snyk org name...")

    except Exception as e:
        print(f"Error in get_latest_snapshot_url: {str(e)}")

    return None



def get_project_name(snyk_org, snyk_token, project_id):
    """
    Get the name of a Snyk project.

    Args:
        snyk_org (str): The Snyk organization identifier.
        snyk_token (str): The Snyk API token for authorization.
        project_id (str): The identifier of the Snyk project.

    Returns:
        str or None: The name of the project or None if there's an error.
    """
    try:
        # Set the headers for the Snyk API request.
        headers = {
            'Authorization': f'Bearer {snyk_token}',
        }

        # Make a GET request to the Snyk API to get project details.
        response = requests.get(
            f'https://api.snyk.io/rest/orgs/{snyk_org}/projects/{project_id}version=2023-08-04',
            headers=headers
        )

        # Check if the response was successful.
        if response.status_code == 200:
            project_data = response.json()['data']
            print("project data ", project_data)
            project_name = project_data['attributes']['name']
            print("project name ", project_name)
            return project_name
        else:
            return None
    except Exception as e:
        print(f"Error in get_project_name: {str(e)}")
        return None


def get_project_name_by_id(project_id):
    """
    Get the name of a Snyk project by its ID.

    Args:
        snyk_org (str): The Snyk organization identifier.
        project_id (str): The identifier of the Snyk project.

    Returns:
        str or None: The name of the project or None if it's not found or there's an error.
    """
    try:
        # Get all project IDs for a given organization
        response = requests.get(
            f'https://api.snyk.io/rest/orgs/{snyk_org}/projects?version=2023-08-04',
            headers=AUTH_HEADER
        )
        response.raise_for_status()  # Check for HTTP errors
        project_data = response.json()['data']
        for project in project_data:
            if project["id"] == project_id:
                print("Project Name Found : ", project["attributes"]["name"])
                return project["attributes"]["name"]
    except Exception as Error:
        print("Could not fetch project names ", Error)


def get_snyk_org_name_by_id(snyk_org):
    """
    Get the name of a Snyk organization by its ID.

    Args:
        snyk_org (str): The Snyk organization identifier.

    Returns:
        str or None: The name of the organization or None if it's not found or there's an error.
    """
    url = f"https://api.snyk.io/rest/orgs/{snyk_org}"
    headers=AUTH_HEADER

    try:
        response = requests.get(url, headers=headers)
    except requests.exceptions.RequestException as e:
        print(f"Error getting Snyk organization name: {e}")
        return None

    if response.status_code == 200:
        org_data = response.json()
        print("org data looks like this...", org_data)
        return org_data["name"]
    elif response.status_code == 404:
        print(f"Snyk organization {snyk_org} does not exist.")
        return None
    else:
        print(f"Unexpected error getting Snyk organization name: {response.status_code}")
    return None


def main():
    # Get the list of req files and split them
    env_file=os.getenv('GITHUB_ENV')
    req_files = os.environ.get('req_files', '')
    repo_name = os.environ.get('repo_name', '')
    print("Changed env files are :", req_files)

    # Split the req_files string into a list of strings
    req_files_list = req_files.split()

    if not req_files_list:
        print("No requirement files found in the environment variable.")
        return
    snyk_project_name_list = []
    matching_project_ids = {}

    for req_filename in req_files_list:
        project_name = req_filename[:-4]  # Remove the ".txt" extension
        snyk_project_name = f'{repo_name}/{project_name}'
        print(f"SNYK_PROJECT_NAME for {req_filename} is {snyk_project_name}")
        snyk_project_name_list.append(snyk_project_name)

    org_name = get_snyk_org_name_by_id(snyk_org)
    print("org name is ", org_name)
    project_ids = get_project_ids(snyk_org)
    if not project_ids:
        return
    critical = 0
    high = 0
    """
    for project_id in project_ids:
        get_project_name = get_project_name_by_id(project_id)
        print("Project Name for ", project_id, " is as follows ", get_project_name)
    for project_id in project_ids:
        issue_counts = get_issue_counts(snyk_org, project_id)
        if issue_counts is None:
            continue
    """
        # Create a dictionary to store the matching project IDs and names.
    for project_id in project_ids:
        project_name = get_project_name_by_id(project_id)

        # If the project name matches a project name in your code, add the project ID and name to the dictionary.
        if project_name in snyk_project_name_list:
            matching_project_ids[project_id] = project_name

    # Iterate over the dictionary and call the get_issue_count() function for each project ID.
    for project_id, project_name in matching_project_ids.items():
        issue_counts = get_issue_counts(snyk_org, project_id)
        if issue_counts is None:
            continue
    
        issues = get_aggregated_issues(snyk_org, project_id)
        df = create_issue_list_dataframe(issues)

        issue_list = create_issue_list_html(df)
        final_url = get_latest_snapshot_url(snyk_org, project_id)
        print("Latest snapshot URL ", final_url)

        #if issue_counts['critical'] > 0 or issue_counts['high'] > 0:
        critical = issue_counts['critical'] + critical
        high = issue_counts['high'] + high
        with open(env_file, "a") as myfile:
            myfile.write(f"SNYK_CRITICAL={critical} \n")
        with open(env_file, "a") as myfile:
            myfile.write(f"SNYK_HIGH={high} \n")
        print(f'There are {issue_counts["critical"]} critical and {issue_counts["high"]} high vulnerabilities in project')

        project_name = get_project_name_by_id(project_id)
        # Create the comment body
        comment_body = f"Snapshot URL for project name < {project_name} >: {final_url}\n\n<details><summary>Click to expand the Snyk report for < {project_name} > </summary>\n\n{issue_list}\n\n</details>"

        # Make the POST request to create the comment
        response = requests.post(
                f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_req}/comments',
                headers=headers,  # Assuming you have 'headers' defined
                json={"body": comment_body}
        )

        if response.status_code == 201:
            print("Comment successfully posted to GitHub pull request.")
        else:
            print(f"Failed to post comment to GitHub pull request. Status code: {response.status_code}")

if __name__ == "__main__":
    main()