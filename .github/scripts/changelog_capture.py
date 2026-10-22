# Import packages
import os
import pyodbc
from datetime import datetime, date
import re
import requests
import json
import base64

repo_name = os.environ['repo_name']
organization = os.environ['repo_owner']
pull_request_number = os.environ['pr_number']
pull_request_link = os.environ['pr_url']
pull_request_title = os.environ['pr_title']
pr_author = os.environ['pr_author']
github_token = os.environ['GH_TOKEN']

clientId = os.environ['CLINET_ID']
clientSecret = os.environ['CLINET_SECRET']
tenantID = os.environ['TENANT_ID']
key_vault_name = os.environ['key_vault_name']
sqldb_secret_name = os.environ['sqldb_secret_name']
pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']
github_api_version = os.environ['github_api_version']

headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {github_token}',
    'X-GitHub-Api-Version': github_api_version,
}
headers1 = ''

def generate_az_access_token(clientId,clientSecret,tenantID):
    #connection details to login to azure
    url = f"https://login.microsoftonline.com/{tenantID}/oauth2/v2.0/token"
    headers = {"Content-Type":"application/x-www-form-urlencoded"}

    data = {
        "grant_type": 'client_credentials',
        "client_id": clientId,
        "client_secret": clientSecret,
        "scope": "https://vault.azure.net/.default"
    }
    try:
        #post request to generate access_token
        response = requests.post(url,headers = headers, data = data)
        if response.status_code == 200:
            access_token = response.json()["access_token"]
            print("Azure Access Token Generated")
            return access_token 
        else:
            print(f"Error: {response.status_code},{response.text}")
    except:
        raise Exception (response.text)


#TO GENERATE GITHUB AUDIT TABLE SQL SERVER DETAILS
def get_secrets_from_kv(key_vault_name,secret_name,az_access_token):
    '''This function is to get secret details from key vault'''
    #connection details
    var = {
        "Authorization": f"Bearer {az_access_token}",
        "Content-Type": "application/json"
    }

    #connection url to get a secret name from specified key vault
    base_url=f"https://{key_vault_name}.vault.azure.net/secrets/{secret_name}?api-version=7.4"

    try:
        response = requests.get(base_url,headers=var)

        if response.status_code == 200 :
            result = json.loads(response.text)
            return result['value']
        else:
            print(f"Error: {response.status_code},{response.text}")
    except:
        raise Exception (response.text)


## Step - 2 Use Azure Key vault secrets to fetch the details from DB for respective CRQ Number 
# Updated get_data_from_db function
def get_data_from_db(crq_number,server,database,username,password):
    '''This function is to fetch the details from DB for respective CRQ Number'''
    conn = None
    try:
        driver = '{ODBC Driver 17 for SQL Server}'

        # Establish connection to SQL Server DB
        conn_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_string)

        # Create a cursor from the connection
        cursor = conn.cursor()

        # Query to check if CRQ number exists
        check_query = f"SELECT COUNT(*) FROM [ReleaseManagement].[release_management_detail] WHERE CRQ_Number = ?"
        cursor.execute(check_query, crq_number)
        crq_count = cursor.fetchone()[0]

        if crq_count == 0:
            raise ValueError(f"No data found for CRQ number: {crq_number}")

        # Query to fetch specific column information based on CRQ number
        fetch_query = """
            SELECT
                CRQ_Number,
                Change_Type AS change_type,
                Domain_Name AS domain_name,
                Feature_Name AS feature_name,
                Change_Description AS change_description,
                Business_Owner AS business_owner,
                Tech_Lead AS tech_lead,
                Cab_Approval_By AS cab_approval_by,
                Business_Go_Live_Date AS business_go_live_date,
                Expected_Go_Live_Date AS expected_go_live_date,
                Design_Link as design_link
            FROM
                [ReleaseManagement].[release_management_detail]
            WHERE
                CRQ_Number = ?
        """
        cursor.execute(fetch_query, crq_number)

        # Fetch the first row
        row = cursor.fetchone()

        if row is not None:
            # Map the column names to the dictionary keys
            column_names = [column[0] for column in cursor.description]
            result = dict(zip(column_names, row))
            return result

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # Close the database connection
        if conn:
            conn.close()


## Step - 4 Get Approvers List
def get_approvers_list(organization, repo_name, pull_request_number):
    '''This function is to get the list of approvers for the pull request'''
    global headers
    # Get the list of reviews for the pull request
    review_list_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_number}/reviews'
    review_list = requests.get(review_list_url, headers=headers, timeout=60)
    
    # Check if the request was successful
    if review_list.status_code != 200:
        raise Exception(f"Failed to retrieve reviews: {review_list.status_code}")
    
    # Parse the JSON response and extract the list of approvers
    reviews_data = json.loads(review_list.text)
    approvers_list = [review['user']['login'] for review in reviews_data if review['state'].upper() == 'APPROVED']
    
    # Print the list of approvers
    print("Approvers List:")
    for approver in approvers_list:
        print(approver)
    
    # Return the list of approvers
    return approvers_list



## Step - 3 Get Existing Changelog md file from the main branch 
def get_changelog_content(organization, repo_name, branch_name):
    '''This function is to retrieve the changelog file content from the main branch'''
    global headers
    # Construct the URL to get the changelog file content
    changelog_url = f"https://api.github.com/repos/{organization}/{repo_name}/contents/CHANGELOG.md?ref={branch_name}"

    # Send the GET request to retrieve the changelog file content
    response = requests.get(changelog_url, headers=headers, timeout=60)

    # Check the response status code
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve changelog file: {response.status_code}")

    # Parse the JSON response and extract the changelog content
    response_data = json.loads(response.text)
    changelog_content = response_data['content']

    # Decode the base64 encoded changelog content
    changelog_decoded = base64.b64decode(changelog_content).decode('utf-8')
    changelog_file = str(changelog_decoded)
    # Print the changelog content
    return(changelog_file)


## Step - 6 Generate Changelog Entry and return new template
def generate_changelog_entry(version, date, change_type, crq_number, domain_name, process_name,
    change_description, business_owner, tech_lead, approver_name, business_go_live_date,
    it_go_live_date, authors, documentation_link, detailed_commit_info):
    '''This function is to generate a new changelog entry based on the input data'''
    color_symbol = {'new': '+', 'fix': '-', 'enhancement': '!'}

    # Generate the dynamic HTML table
    html_table = f"""
    <table>
        <tr>
            <td>CRQ Number</td>
            <td>{crq_number}</td>
        </tr>
        <tr>
            <td>Domain Name</td>
            <td>{domain_name}</td>
        </tr>
        <tr>
            <td>Process Name</td>
            <td>{process_name}</td>
        </tr>
        <tr>
            <td>Change Description</td>
            <td>{change_description}</td>
        </tr>
        <tr>
            <td>Business Owner</td>
            <td>{business_owner}</td>
        </tr>
        <tr>
            <td>Tech Lead</td>
            <td>{tech_lead}</td>
        </tr>
        <tr>
            <td>Approver Name</td>
            <td>{approver_name}</td>
        </tr>
        <tr>
            <td>Business Go Live Date</td>
            <td>{business_go_live_date}</td>
        </tr>
        <tr>
            <td>IT Go Live Date</td>
            <td>{it_go_live_date}</td>
        </tr>
    </table>
    """

    # Enclose HTML code in <pre> tags for left-aligned formatting
    html_table = f"<pre>{html_table}</pre>"

    pattern = r"(^\s+)"
    replacement = r""
    html_table = re.sub(pattern, replacement, html_table, flags=re.MULTILINE)
    # Concatenate the HTML table with the existing template
    template = f"# v{version} ({date})\n______________________________________________________\n## {'💡 New' if change_type.lower() == 'new' else '🚀 Enhancement' if change_type.lower() == 'enhancement' else '🐛 Bug Fix'}\n\n\n" \
        f"\n\n{html_table}\n\n" \
        f"🔰 Authors:\n\n {color_symbol.get(change_type.lower(), '')} {authors} \n\n" \
        f"📝 Documentation:\n\n {color_symbol.get(change_type.lower(), '-')} For more information please refer to design documentation 🔗{documentation_link}\n\n" \
        f"🔔 Detailed Commit Information: \n\n"
    for files in detailed_commit_info:
        template += f"{color_symbol.get(change_type.lower(), '')} {files} \n"
    template += "\n<br>\n"

    return template


def increment_version(existing_template, new_data):
    '''This function is to increment the version based on the change type'''
    #pattern = r"\[v(\d+\.\d+\.\d+)\]\(.*?\)"
    #pattern = r"\[v(\d+\.\d+\.\d+)\]" --> This default pattern not working for spaces or exceptions
    pattern = r"\[v?(\d+\.\d+\.\d+)\]"   # --> Therefore, trying this one

    # Find all matches
    version_list = re.findall(pattern, existing_template)
    # Print the list of versions
    print(version_list)
    parts = list(map(int, version_list[-1].split('.')))
    print(parts)

    if new_data['change_type'].lower() == 'new':
        parts[0] += 1
        parts[1] = 0
        parts[2] = 0
    elif new_data['change_type'].lower() == 'enhancement':
        parts[1] += 1
        parts[2] = 0
    elif 'fix' in new_data['change_type'].lower():
        parts[2] += 1
    else:
        raise ValueError("Invalid change type")
    print('.'.join(map(str, parts)))
    return '.'.join(map(str, parts))


## Step - 5 Generate Changelog Entry
def generate_new_changelog(existing_template, new_data, approvers_list_str,changed_files_list,pr_author):
    '''This function is to generate a new changelog entry based on the input data'''
    # Increment the version based on the change type
    new_version = increment_version(existing_template, new_data)
    business_go_live_date_final = new_data['business_go_live_date'].strftime("%d-%b-%Y")
    it_go_live_date_final = new_data['expected_go_live_date'].strftime("%d-%b-%Y")
    # Generate new changelog entry
    new_changelog_entry = generate_changelog_entry(
        version=f"[{new_version}]({pull_request_link})",
        date=date.today().strftime("%d-%b-%Y"),
        change_type=new_data['change_type'],
        crq_number=new_data['CRQ_Number'],
        domain_name=new_data['domain_name'],
        process_name=new_data['feature_name'],
        change_description=new_data['change_description'],
        business_owner=new_data['business_owner'],
        tech_lead=new_data['tech_lead'],
        approver_name=approvers_list_str,
        business_go_live_date = business_go_live_date_final,
        it_go_live_date = it_go_live_date_final,
        authors=pr_author,
        documentation_link=new_data['design_link'],
        detailed_commit_info=changed_files_list
    )
    # Append new changelog entry to existing template
    new_changelog = existing_template + '\n\n' + new_changelog_entry
    #print(new_changelog)
    return new_changelog


## Step - 7 Push and update the new changelog md file in the main branch
def update_changelog(organization, repo_name, branch_name, new_changelog):
    '''This function is to update the changelog file in the main branch'''
    global headers1, headers
    # Construct the URL to get the changelog file content
    changelog_url = f"https://api.github.com/repos/{organization}/{repo_name}/contents/CHANGELOG.md?ref={branch_name}"

    # Retrieve the existing changelog template and SHA value
    response = requests.get(changelog_url, headers=headers)

    # Check the response status code
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve changelog file: {response.status_code}")

    # Parse the JSON response and extract the changelog content and SHA value
    response_data = json.loads(response.text)
    #changelog_content = response_data['content']
    sha = response_data['sha']

    # Encode the updated changelog back to base64
    updated_changelog_encoded = base64.b64encode(new_changelog.encode('utf-8')).decode('utf-8')

    # Prepare the commit message
    commit_message = f"Update changelog.md"

    # Construct the PUT request body with the updated changelog and SHA value
    put_data = {
        "message": commit_message,
        "content": updated_changelog_encoded,
        "sha": sha,
    }

    # Send the PUT request to update the changelog file
    put_response = requests.put(changelog_url, headers=headers1, data=json.dumps(put_data))

    # Check the PUT response status code
    if put_response.status_code != 200:
        raise Exception(f"Failed to update changelog file: {put_response.status_code}")

    print("Changelog updated successfully!")


def main():
    '''This is the main function'''
    global headers, headers1
    az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)
    sqldb_conn = eval(get_secrets_from_kv(key_vault_name,sqldb_secret_name,az_access_token))
    #Audit Table details
    server = sqldb_conn['audit_server']
    database = sqldb_conn['audit_database']
    username = sqldb_conn['user_name']
    password = sqldb_conn['password']

    github_pat = get_secrets_from_kv(key_vault_name,pat_github_secret_key,az_access_token)
    headers1 = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {github_pat}',
        'X-GitHub-Api-Version': github_api_version,
    }

    if '-' in pull_request_title:
        pr_title_arr = pull_request_title.lower().split('-')
    elif '_' in pull_request_title:
        pr_title_arr = pull_request_title.lower().split('_')
    CRQ_Number = ''
    
    if pr_title_arr[0] == 'revert':              
        CRQ_Number = pr_title_arr[1]
    elif pr_title_arr[2] == 'hf':
        CRQ_Number = pr_title_arr[0]
    elif pr_title_arr[0][:3] == 'crq' or pr_title_arr[0][:4] == 'dtrm':
        CRQ_Number = pr_title_arr[0]

    print(f'CRQ number: {CRQ_Number}')
    
    changed_files_list = [x['filename'] for x in requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_number}/files', headers=headers, timeout=60).json()]
    branch_name = "main"
    existing_template = get_changelog_content(organization, repo_name, branch_name)
    new_data = get_data_from_db(CRQ_Number, server,database,username,password)
    approvers_list = get_approvers_list(organization, repo_name, pull_request_number)
    approvers_list_str = (',').join(approvers_list)
    new_changelog = generate_new_changelog(existing_template, new_data,approvers_list_str,changed_files_list,pr_author)
    update_changelog(organization, repo_name, branch_name, new_changelog)


if __name__ == '__main__':
    main()
