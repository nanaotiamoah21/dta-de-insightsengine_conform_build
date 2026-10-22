# import libraries
import os
import requests
import json
import pyodbc
import re

repo_name = os.environ['repo_name']
organization = os.environ['repo_owner']
pull_number = os.environ['pr_number']
pull_request_title = os.environ['pr_title']

#Connection Details to login to azure,generate access token, get info from key vault
clientId = os.environ['CLINET_ID']
clientSecret = os.environ['CLINET_SECRET']
tenantID = os.environ['TENANT_ID']
key_vault_name = os.environ['key_vault_name']
sqldb_secret_name = os.environ['sqldb_secret_name']
metadatatabname = os.environ['metadatatabname']
pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']
github_token = os.environ['github_token']
github_api_version = os.environ['github_api_version']

headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {github_token}',
    'X-GitHub-Api-Version': github_api_version,
}
headers1 = ''

def generate_az_access_token(clientId,clientSecret,tenantID):
    '''This function is to generate access token using azure client id, client secret and tenant id'''
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


def get_secrets_from_kv(key_vault_name,secret_name,az_access_token):
    '''This function is to get the secret from key vault using secret name'''
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
            print("Github Audit Table Connection details collected")
            return result['value']
        else:
            print(f"Error: {response.status_code},{response.text}")
    except:
        raise Exception (response.text)
    

def get_github_team_name(domain,server,database,username,password,istl=int):
    '''This function is to get the github team name from the metadata table'''
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


def get_team_details_from_pr_title(pr_title,server,database,username,password,istl=int):
    '''This function is to get the team details from the pull request title'''
    global domain_name, headers, headers1
    if '-' in pr_title:
        pr_title_arr = pr_title.lower().split('-')
    elif '_' in pr_title:
        pr_title_arr = pr_title.lower().split('_')

    if pr_title_arr[0][:3] == 'crq' or pr_title_arr[0][:4] == 'dtrm':
        domain_name = pr_title_arr[1]
    else:
        raise Exception("Invalid PR title, Title should be in format crqnumber-domain-*")    
    
    github_team = get_github_team_name(domain_name,server,database,username,password,istl)
    github_team_members = ['@'+x['login'] for x in json.loads((requests.get(f'https://api.github.com/orgs/{organization}/teams/{github_team}/members', headers=headers1, timeout=60)).text)]

    return github_team_members, github_team


def main():
    global headers, headers1
    message = 'Deployment approval required from below:\n'
    own_pr_github_teams = []
    github_teams_1 = []
    invalid_flag = 0
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

    # Get the list of approved reviewers
    review_list = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}/reviews', headers=headers, timeout=60)
    approved_review_list = ['@'+x['user']['login'] for x in json.loads(review_list.text) if x['state'].upper() == 'APPROVED']
    print(f'folllowing members have approved the req: {approved_review_list}')

    # Get the list of current reviewers
    current_pr_reviewer_list = [x['slug'] for x in (requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}/requested_reviewers', headers=headers1, timeout=60).json())['teams']]

    # validate how many approvals received from the ops team
    github_ops_team_members, github_ops_team = get_team_details_from_pr_title(pull_request_title,server,database,username,password,0)
    approved_ops_reviewer_count = len([x for x in github_ops_team_members if x in approved_review_list])
    if approved_ops_reviewer_count == 0:
        invalid_flag = 1
        own_pr_github_teams.append(github_ops_team)
        message += f'''Pull Request Number #{int(pull_number)}: Please wait for atleast one review from {github_ops_team_members} of team {github_ops_team} from domain {domain_name}.\n'''
        github_teams_1.append([domain_name+" members",github_ops_team_members,pull_number])

    # validate how many approvals received from the ops techlead team
    github_ops_techlead_members, github_ops_techlead_team = get_team_details_from_pr_title(pull_request_title,server,database,username,password,1)
    approved_ops_techlead_reviewer_count = len([x for x in github_ops_techlead_members if x in approved_review_list])
    if approved_ops_techlead_reviewer_count == 0:
        invalid_flag = 1
        own_pr_github_teams.append(github_ops_techlead_team)
        message += f'''Pull Request Number #{int(pull_number)}: Please wait for atleast one review from {github_ops_techlead_members} of team {github_ops_techlead_team} from domain {domain_name}.\n'''
        github_teams_1.append([domain_name+ "Techlead",github_ops_techlead_members,pull_number])

    
    # To handle approval if a member is present in both ops team and ops techlead team and got one approval   but as our branch protection rule we need 2 approvals 
    #get github ops techlead members details who approved pr 
    github_ops_techlead_members_in_review = [member for member in github_ops_techlead_members if member in approved_review_list]
    print(github_ops_techlead_members_in_review)  
    #get github ops members details who approved pr
    github_ops_members_in_review = [member for member in github_ops_team_members if member in approved_review_list]
    print(github_ops_members_in_review)
    approvercount = len(list(set(github_ops_techlead_members_in_review + github_ops_members_in_review)))
    if approvercount < 2 and invalid_flag == 0:
        github_teams_1.append([domain_name+ "Techlead",github_ops_techlead_members,pull_number])
        github_teams_1.append([domain_name+" members",github_ops_team_members,pull_number])
        message = f'''Pull Request Number #{int(pull_number)}: Please wait for atleast one review from {github_ops_techlead_members} of team {github_ops_techlead_team} and {github_ops_team_members} of team {github_ops_team} from domain {domain_name} .\n'''
        invalid_flag = 1


    message1 = f"""
    <h2>Approver details</h2>
    <p>Please find below Prod Deployment approvers because of open pull requets : </p>
    <table>
        <tr>
            <td>Domain</td>
            <td>Approver Names</td>
        </tr>
    """
    for x in github_teams_1:
        message1 += f"""
        <tr>
            <td>{x[0]}</td>
            <td>{x[1]}</td>
        </tr>
        """
    message1 += """</table><p>Please get atleast one approval for each domain mentioned above.</p>"""

    pattern = r"(^\s+)"
    replacement = r""
    message2 = re.sub(pattern, replacement, message1, flags=re.MULTILINE)

    to_be_removed_reviewers = [x for x in current_pr_reviewer_list if x not in own_pr_github_teams]
    requests.delete(f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}/requested_reviewers', headers=headers1, json={"team_reviewers":to_be_removed_reviewers,"reviewers":[]}, timeout=60)

    if invalid_flag == 1:
        requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_number}/comments', headers=headers, json={"body": message2}, timeout=60)
        raise Exception(message)


if __name__ == '__main__':
    main()
