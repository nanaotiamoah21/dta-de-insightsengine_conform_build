import os
import requests
import json
import re
from jira import JIRA
import pyodbc
import time

github_token = os.environ['github_token']
repo_name = os.environ['repo_name']
organization = os.environ['repo_owner']
run_id = os.environ['run_id']
pull_number = os.environ['pr_number']

#Connection Details to login to azure,generate access token, get info from key vault
clientId = os.environ['CLINET_ID']
clientSecret = os.environ['CLINET_SECRET']
tenantID = os.environ['TENANT_ID']
key_vault_name = os.environ['key_vault_name']
sqldb_secret_name = os.environ['sqldb_secret_name']
jira_host_secret_name = os.environ['jira_host_secret_name']
jira_token_secret_name = os.environ['jira_token_secret_name']
metadatatabname = os.environ['github_team_metadata_table']
workflow_name = os.environ['workflow_name']
head_branch_name = os.environ['source_branch']

source_branch = os.environ['source_branch']
pull_request_title = os.environ['pr_title']
pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']

github_api_version = os.environ['github_api_version']

headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {github_token}',
    'X-GitHub-Api-Version': github_api_version,
}
headers1 = ''
fail_counter = 0
soft_fail_counter = 0
domain_list = []

def generate_az_access_token(clientId,clientSecret,tenantID):
    '''This function is used to generate access token for azure'''
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
    '''This function is used to get the secret details from azure key vault'''
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
            #print("AKV Secret details collected")
            return result['value']
        else:
            print(f"Error: {response.status_code},{response.text}")
    except:
        raise Exception (response.text)
  

def get_pull_request_info(organization, repo_name, pull_request_id):
    '''This function is used to get the pull request details'''
    global headers
    pull_request_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_id}'
    changed_files_list = []
    head_commit_title = ''
    try:
        pull_request_data = requests.get(pull_request_url, headers=headers, timeout=60)
        if pull_request_data.status_code == 200:
            pull_request_info = pull_request_data.json()
            changed_files_list = [x['filename'] for x in requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_id}/files', headers=headers, timeout=60).json()]
            pull_request_body = pull_request_info['body']
            last_commits_id = [x['sha'] for x in requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_id}/commits', headers=headers, timeout=60).json()][0]
            head_commit_title = (requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/commits/{last_commits_id}', headers=headers, timeout=60).json())['commit']['message']
        else:
            raise ValueError(f"Invalid pull request number: {pull_request_id}")
    except Exception as e:
        raise ValueError(f"Error: Unable to retrieve pull request '{pull_request_id}' - {e}")    
    
    return changed_files_list, pull_request_body, head_commit_title


def validate_folder_details(changed_files_list): 
    '''This function is used to validate the folder details of the changed files in the pull request. It checks for the valid folders and file extensions.'''
    print(f'Changed file list: \n {changed_files_list}')
    global fail_counter
    global soft_fail_counter
    folder_validation_text = ''
    ext_validation_text = ''
    if changed_files_list:
        valid_folders_extensions = 'configs:yaml|jms:yaml|notebooks:py|schemas:json,xsd|sql:py,sql'
        for file_list in changed_files_list:
            file = file_list
            folder_name_initial = file.split('/')[0]
            if folder_name_initial == '.github':
                folder_name = file.split('/')[1]
                if domain_name != 'shared':
                    fail_counter += 1
                    print(f'Failure - Unauthorized Pull request raised under .github folders by domain -> {domain_name}')
            else:
                folder_name = folder_name_initial
                file_name = file.split('/')[-1]
                file_extension = file_name.split('.')[-1]
                if folder_name.lower() in valid_folders_extensions.lower() or folder_name.lower() == 'prophecy':
                    folder_validation_text = f'File -> {file} is under a valid folder -> ' + folder_name
                else:
                    folder_validation_text = f'Failure - Github Folder Validation: File -> {file} is under a invalid folder -> ' + folder_name
                    soft_fail_counter += 1
                    print(folder_validation_text)
                if folder_name.lower() == "configs" and file_extension.lower() in valid_folders_extensions.split('|')[0].lower():
                    ext_validation_text = f'Config file -> {file} is with a valid extension -> ' + file_extension 
                elif folder_name.lower() == "jms" and file_extension in valid_folders_extensions.split('|')[1].lower():
                    ext_validation_text = f'JMS file -> {file} is with a valid extension -> ' + file_extension 
                elif folder_name.lower() == "notebooks" and file_extension in valid_folders_extensions.split('|')[2].lower():
                    ext_validation_text = f'Notebooks file -> {file} is with a valid extension -> ' + file_extension 
                elif folder_name.lower() == "schemas" and file_extension in valid_folders_extensions.split('|')[3].lower():
                    ext_validation_text = f'Schemas file -> {file} is with a valid extension -> ' + file_extension 
                elif folder_name.lower() == "sql" and file_extension in valid_folders_extensions.split('|')[4].lower():
                    ext_validation_text = f'SQL file -> {file} is with a valid extension -> ' + file_extension 
                elif folder_name.lower() == "prophecy":
                    ext_validation_text = f' file -> {file} is with a valid extension inside prophecy-> ' + file_extension  
                else:
                    ext_validation_text = f'Failure - File Extension Validation: File -> {file} is with an invalid extension -> ' + file_extension
                    soft_fail_counter += 1
                    print(ext_validation_text)


def validate_branch_name(source_branch, head_commit_title):
    '''This function is used to validate the branch name of the pull request. It checks for the valid branch naming convention.'''
    global fail_counter
    source_branchArr = source_branch.lower().split('-')
    branch_flag = 0
    branch_format = ''
    if head_commit_title[:6] == 'Revert':    
        if not(source_branchArr[0] == 'revert' and source_branchArr[2] == 'feature'):
            branch_flag = 1
            branch_format = 'revert-prnumber-feature-*'
    else:
        if not(source_branchArr[0] == 'feature' or source_branchArr[0] == 'hf'):
            branch_flag = 1
            branch_format = 'feature-*,hf-crqnumber-ops-*'
        
    if branch_flag == 1:
        fail_counter += 1
        print(f'Failure - Branch Name Validation: Branch naming convention not in the expected format {branch_format}. Branch Name -> {source_branch}')


def get_release_details(CRQ_Number,server,database,username,password):
    '''This function is used to get the release details from the release management table. It checks for the valid CRQ number and release status.'''
    driver = '{ODBC Driver 17 for SQL Server}'
    rm_check = 0
    try:
        conn_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * from ReleaseManagement.release_management_detail where crq_number = '{CRQ_Number}' and release_status not IN ('rejected', 'closed')")
        record_count = len(cursor.fetchall())
        if record_count > 0:
            rm_check = 1
        else:
            print('CRQ Number or status incorrect')
        cursor.close()
        conn.close()
    except pyodbc.Error as ex:
        print("Error fetching release details:", str(ex))

    return rm_check


def get_github_team_name(domain,server,database,username,password,istl=int):
    '''This function is used to get the github team name from the metadata table based on provided domain as input'''
    driver = '{ODBC Driver 17 for SQL Server}'
    github_team_name = ''
    try:
        conn_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT GithubTeamSlugName from {metadatatabname} where DomainNameShort like '%{domain}%' and [IsTLGroup] = {istl}")
        github_team_name = cursor.fetchall()[0][0]
        cursor.close()
        conn.close()
    except pyodbc.Error as ex:
        print("Error fetching github team details:", str(ex))

    return github_team_name


def get_domain_list(server,database,username,password):
    '''This function is used to get the domain list from the metadata table'''
    global domain_list
    driver = '{ODBC Driver 17 for SQL Server}'
    try:
        conn_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT distinct DomainNameShort as DomainNameShort from {metadatatabname}")
        domain_list = [x[0] for x in cursor.fetchall()]
        cursor.close()
        conn.close()
    except pyodbc.Error as ex:
        print("Error fetching domain list:", str(ex))


def setup_collaborations(server,database,username,password):
    '''This function is used to setup the collaborations for the pull request. It sets the environmental approvers for SIT and PPD environments.'''
    global domain_name, headers, headers1

    open_pull_req_list = sorted([(x['number'],x['title']) for x in json.loads((requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls?state=open', headers=headers, timeout=60)).text) if x['head']['ref'].startswith('feature-')],key=lambda x: x[0])
    reviewer_slug_SIT = ''
    reviewer_slug_PPD = ''

    # Sets the environmental approvers for SIT and PPD if this is first open pull request
    if int(open_pull_req_list[0][0]) == int(pull_number):
        github_team_members_SIT, reviewer_slug_SIT = get_team_details_from_pr_title(open_pull_req_list[0][1],server,database,username,password,0)
        reviewer_id_SIT = json.loads(requests.get(f'https://api.github.com/orgs/{organization}/teams/{reviewer_slug_SIT}', headers=headers1, timeout=60).text)['id']
        github_team_members_PPD, reviewer_slug_PPD = get_team_details_from_pr_title(open_pull_req_list[0][1],server,database,username,password,1)
        reviewer_id_PPD = json.loads(requests.get(f'https://api.github.com/orgs/{organization}/teams/{reviewer_slug_PPD}', headers=headers1, timeout=60).text)['id']

        environment_name = 'SIT'
        requests.put(f'https://api.github.com/repos/{organization}/{repo_name}/environments/{environment_name}', headers=headers1, json={"reviewers": [{"type": "Team", "id": reviewer_id_SIT}]}, timeout=60)

        environment_name = 'PPD'
        requests.put(f'https://api.github.com/repos/{organization}/{repo_name}/environments/{environment_name}', headers=headers1, json={"reviewers": [{"type": "Team", "id": reviewer_id_PPD}]}, timeout=60)
    
    else:
        environment_name = 'SIT'
        resp = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/environments/{environment_name}', headers=headers1, timeout=60)
        reviewer_slug_SIT = resp.json()['protection_rules'][0]['reviewers'][0]['reviewer']['slug']

        environment_name = 'PPD'
        resp = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/environments/{environment_name}', headers=headers1, timeout=60)
        reviewer_slug_PPD = resp.json()['protection_rules'][0]['reviewers'][0]['reviewer']['slug']

    return reviewer_slug_SIT, reviewer_slug_PPD


def validate_pr_title(pull_request_title, source_branch,server,database,username,password):
    '''This function is used to validate the pull request title. It checks for the valid pull request title naming convention.'''
    global fail_counter, domain_name, domain_list
    if '-' in pull_request_title:
        pr_title_arr = pull_request_title.lower().split('-')
    elif '_' in pull_request_title:
        pr_title_arr = pull_request_title.lower().split('_')
    domains = domain_list
    firstname_flag = 0
    CRQ_flag = 0
    domain_flag = 0
    pr_title_format = ''
    CRQ_Number = ''
    
    if pr_title_arr[0] == 'revert':              
        CRQ_Number = pr_title_arr[1]
        domain_name = pr_title_arr[2]
        pr_title_format = 'revert-crqnumber-domain-*'
        if source_branch.lower().split('-')[0] == 'revert':
            firstname_flag = 1 
        if domain_name in domains:
            domain_flag = 1
    elif pr_title_arr[2] == 'hf':
        firstname_flag = 1
        CRQ_Number = pr_title_arr[0]
        domain_name = pr_title_arr[1]
        pr_title_format = 'crqnumber-ops-*'
        if domain_name in domains:
            domain_flag = 1
    elif pr_title_arr[0][:3] == 'crq' or pr_title_arr[0][:4] == 'dtrm':
        firstname_flag = 1
        CRQ_Number = pr_title_arr[0]
        domain_name = pr_title_arr[1]
        pr_title_format = 'crqnumber-domain-*'
        if domain_name in domains:
            domain_flag = 1

    if CRQ_Number[:3] == 'crq' or CRQ_Number[:4] == 'dtrm':
        rm_check_res = get_release_details(CRQ_Number,server,database,username,password)
        if rm_check_res ==1:
            CRQ_flag = 1
        else:
            CRQ_flag = 0
    else:
        CRQ_flag = 0

    if domain_flag == 0:
        print(f'Domain name validation failed. Domain name from pull request: {domain_name}')
    
    if firstname_flag == 1 and CRQ_flag == 1 and domain_flag == 1:        
        pass_text = f'Title naming convention validation successful. PR title -> {pull_request_title}'
        print(pass_text)
    else:
        fail_counter += 1
        print(f'Failure - PR Title Validation: Title naming convention not in the expected format {pr_title_format}. PR title -> {pull_request_title} || Branch name -> {source_branch}')


def identify_tickets(jira, ticket_list):
    '''This function is used to identify the valid JIRA tickets from the provided list. It checks for the valid JIRA story tickets.'''
    if not ticket_list:
        print("Ticket list is empty. Please provide ticket numbers.")
    valid_tickets = []
    labels_list = []
    for ticket in ticket_list:
        try:
            issue = jira.issue(ticket.strip("[]'"))
            issue_type = issue.fields.issuetype.name
            if issue_type == "Epic":
                valid_tickets.append(ticket)
                labels = issue.fields.labels
                for label in labels:
                    if label not in labels_list:
                        labels_list.append(label)
            else:
                jira_text = f"{ticket} is a {issue_type}, so will be ignord as it's not a  epic."
                print(jira_text)
        except Exception as e:
            print(f"JIRA Ticket {ticket} not found or error occurred: {str(e)}")

    return valid_tickets


def validate_jira_ticket(pull_request_body,jira_host_url,jira_auth_pat):
    '''This function is used to validate the JIRA tickets from the pull request body'''
    global fail_counter
    DE_Tickets = []
    other_all_comments = []
    links = []
    jira_validation = 0
    valid_jira_ticket = []
    
    try:
        comment_links = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', pull_request_body)
        if comment_links:
            links.extend(comment_links)
        #de_ticket_matches = re.findall(r'(DE-\d+|DTRM-\d+|DPS-\d+)', pull_request_body)
        de_ticket_matches = re.findall(r'DE-\d+', pull_request_body)
        if de_ticket_matches:
            DE_Tickets.append(de_ticket_matches)
        else:
            other_all_comments.append(pull_request_body)
        if not DE_Tickets:
            print("No 'DE-' tickets found in comments")
            #raise ValueError("No 'DE-' tickets found in comments")
        else:
            flattened = [val for sublist in DE_Tickets for val in sublist]
            DE_Tickets = flattened
            unique_set = set(DE_Tickets)
            DE_Tickets = list(unique_set)
            if 'DE-1234' in DE_Tickets:
                DE_Tickets.remove('DE-1234')
            headers = JIRA.DEFAULT_OPTIONS["headers"].copy()
            headers["Authorization"] = f"Bearer {jira_auth_pat}"
            jira = JIRA(server=jira_host_url, options={"headers": headers})
            valid_jira_ticket = identify_tickets(jira, DE_Tickets)
            if valid_jira_ticket:
                jira_validation = 1
                jira_pass_text = f"Valid Jira  epic tickets found in the provided list. Ticket -> {valid_jira_ticket}"
                print(jira_pass_text)
            else:
                print("Failure - JIRA Ticket Validation: The Jira epic id provided in the Pull Request is found be an invalid ticket .")
                #raise ValueError("Failure - JIRA Ticket Validation: The Jira epic provided in the Pull Request is found be an invalid ticket .")

    except Exception as e:
        print(f"Error - JIRA Ticket validation: No DE JIRA ticket provided in the body section of the Pull Request - {e}")
        #raise ValueError(f"Error - JIRA Ticket validation: No DE JIRA ticket provided in the body section of the Pull Request - {e}")
    #return jira_validation,valid_jira_ticket
    return valid_jira_ticket


def cancel_workflow_runs(workflow_name):
    '''This function is used to cancel the workflow runs for the provided workflow name'''
    global headers
    #get runnig and inprogress workflow list
    workflow_runs_waiting = []
    workflow_runs_inprogress = []
    workflow_runs_waiting_stg = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/actions/workflows/{workflow_name}/runs?event=pull_request&status=waiting', headers=headers, timeout=60).json()
    if workflow_runs_waiting_stg['total_count'] > 0:
        workflow_runs_waiting = [[x['id'],x['head_sha'],x['head_branch']] for x in workflow_runs_waiting_stg['workflow_runs']]
    workflow_runs_inprogress_stg = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/actions/workflows/{workflow_name}/runs?event=pull_request&status=in_progress', headers=headers, timeout=60).json()
    if workflow_runs_inprogress_stg['total_count'] > 0:
        workflow_runs_inprogress = [[x['id'],x['head_sha'],x['head_branch']] for x in workflow_runs_inprogress_stg['workflow_runs']]
    tobe_cancelled_run = workflow_runs_waiting + workflow_runs_inprogress

    #get open head branches from workflow runs
    distinct_branches = list(set([x[2] for x in tobe_cancelled_run]))

    #for every branch keep latest run and cancel rest
    for branch in distinct_branches:
        if branch == head_branch_name:
            runids = sorted([x[0] for x in tobe_cancelled_run if x[2]==branch])
            print(f'List of runids for head branch {branch} - {runids}')
            if len(runids) > 0:
                print(f'To be cancelled runids for head branch {branch} - {sorted(runids)}')
                for runid in runids:
                    print(f'Cancelling run {runid} of head branch {branch}')
                    requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/actions/runs/{runid}/cancel', headers=headers, timeout=60)


def get_team_details_from_pr_title(pr_title,server,database,username,password,isTechLeads):
    '''This function is used to get the tech lead team details from the pull request title'''
    global headers1, domain_name
    domain_name = ''
    if '-' in pr_title:
        pr_title_arr = pr_title.lower().split('-')
    elif '_' in pr_title:
        pr_title_arr = pr_title.lower().split('_')
    if pr_title_arr[0] == 'revert':              
        domain_name = pr_title_arr[2]
    elif pr_title_arr[2] == 'hf':
        domain_name = pr_title_arr[1]
    elif pr_title_arr[0][:3] == 'crq' or pr_title_arr[0][:4] == 'dtrm':
        domain_name = pr_title_arr[1]
    
    github_team = get_github_team_name(domain_name,server,database,username,password,isTechLeads)
    github_team_members = ['@'+x['login'] for x in json.loads((requests.get(f'https://api.github.com/orgs/{organization}/teams/{github_team}/members', headers=headers1, timeout=60)).text)]

    return github_team_members, github_team


def main():
    global fail_counter, headers, headers1
    github_teams = []
    github_teams_1 = []
    reviewer_slug_SIT = ''
    reviewer_slug_PPD = ''
    message = '\nDeployment approval required from below:\n'
    az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)
    sqldb_conn = eval(get_secrets_from_kv(key_vault_name,sqldb_secret_name,az_access_token))
    #Audit Table details
    server = sqldb_conn['audit_server']
    database = sqldb_conn['audit_database']
    username = sqldb_conn['user_name']
    password = sqldb_conn['password']

    jira_host_url = get_secrets_from_kv(key_vault_name,jira_host_secret_name,az_access_token)
    jira_auth_pat = get_secrets_from_kv(key_vault_name,jira_token_secret_name,az_access_token)

    github_pat = get_secrets_from_kv(key_vault_name,pat_github_secret_key,az_access_token)
    headers1 = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {github_pat}',
        'X-GitHub-Api-Version': github_api_version,
    }

    get_domain_list(server,database,username,password)
    
    changed_files_list, pull_request_body, head_commit_title = get_pull_request_info(organization, repo_name, pull_number)

    validate_pr_title(pull_request_title, source_branch,server,database,username,password)

    validate_branch_name(source_branch, head_commit_title)

    validate_folder_details(changed_files_list)
    
    # if source_branch[:7] == 'revert-':
    #     validate_jira_ticket(pull_request_body,jira_host_url,jira_auth_pat)
    print(pull_request_body)
    valid_jira_ticket_list = validate_jira_ticket(pull_request_body,jira_host_url,jira_auth_pat)
    
    if fail_counter > 0:
        pr_comment = 'PR validation failed. Please check the workflow run for detailed explanation.'
        print(pr_comment)
        requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_number}/comments', headers=headers, json={"body": pr_comment}, timeout=60)
        raise RuntimeError('PR validation failed')
    elif soft_fail_counter > 0:
        pr_comment = 'PR Validation completed with soft validation issues. Please check the workflow run for detailed explanation.'
        print(pr_comment)
        requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_number}/comments', headers=headers, json={"body": pr_comment}, timeout=60)
        if source_branch.startswith('feature-'):
            reviewer_slug_SIT, reviewer_slug_PPD = setup_collaborations(server,database,username,password)
    else: 
        print('PR Validation successful.')
        if source_branch.startswith('feature-'):
            reviewer_slug_SIT, reviewer_slug_PPD = setup_collaborations(server,database,username,password)

    open_pull_req_list = [[x['title'],x['number']] for x in json.loads((requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls?state=open', headers=headers, timeout=60)).text)]

    for x, y in open_pull_req_list:
        if int(y) <= int(pull_number):
            github_team_members, github_team = get_team_details_from_pr_title(x,server,database,username,password,1)
            message += f'''Pull Request Number #{int(y)}: Please wait for atleast one review from {github_team_members} of team {github_team} from domain {domain_name}.\n'''
            github_teams.append(github_team)
            github_teams_1.append([domain_name,github_team_members,y])

    #setting up reviewers
    github_ops_team = get_github_team_name('ops',server,database,username,password,1)
    github_ops_team_members = ['@'+x['login'] for x in json.loads((requests.get(f'https://api.github.com/orgs/{organization}/teams/{github_ops_team}/members', headers=headers1, timeout=60)).text)]
    message += f'''Pull Request Number #{int(pull_number)}: Please wait for atleast one review from {github_ops_team_members} of team {github_ops_team}.\n'''
    github_teams.append(github_ops_team)
    github_teams_1.append(['ops',github_ops_team_members,pull_number])

    print(github_teams)
    github_teams = list(dict.fromkeys(github_teams))

    message1 = f"""
    <h2>Approver details</h2>
    <p>Please find below Environmental approvers : </p>
    <table>
        <tr>
            <td>Environment</td>
            <td>Team Name</td>
        </tr>
        <tr>
            <td>SIT</td>
            <td>{reviewer_slug_SIT}</td>
        </tr>
        <tr>
            <td>PPD</td>
            <td>{reviewer_slug_PPD}</td>
        </tr>
    </table>
    <p>Please find below Prod Deployment approvers because of open pull requets : </p>
    <table>
        <tr>
            <td>Domain</td>
            <td>Approver Names</td>
            <td>Pull Number</td>
        </tr>
    """
    for x in github_teams_1:
        message1 += f"""
        <tr>
            <td>{x[0]}</td>
            <td>{x[1]}</td>
            <td>#{str(x[2])}</td>
        </tr>
        """
    message1 += """</table><p>Please get atleast one approval for each domain mentioned above.</p>"""

    pattern = r"(^\s+)"
    replacement = r""
    message2 = re.sub(pattern, replacement, message1, flags=re.MULTILINE)

    requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}/requested_reviewers', headers=headers1, json={"team_reviewers":github_teams}, timeout=60)
    requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_number}/comments', headers=headers, json={"body": message2}, timeout=60)


if __name__ == '__main__':
    main()
