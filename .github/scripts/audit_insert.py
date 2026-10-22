import os
import datetime
import pyodbc
import requests
import re
import json
from jira import JIRA
import base64

event_tms=str(datetime.datetime.now())
print(event_tms)
repo_name=os.getenv('repo_name')
repo_owner=os.getenv('repo_owner')
repo_url=os.getenv('repo_url')
run_id=os.getenv('run_id')
pr_number=os.getenv('pr_number')
pr_title=os.getenv('pr_title')
pr_url=os.getenv('pr_url')
pr_status=os.getenv('pr_status')
pr_action=os.getenv('pr_action')
pr_author=os.getenv('pr_author')
event_owner=os.getenv('pr_owner')
pr_merged_status=os.getenv('pr_merged_status')
pr_merged_by=os.getenv('pr_merged_by')
pr_merged_tms=os.getenv('pr_merged_tms')
pr_merged_commit_id=os.getenv('pr_merged_commit_id')
source_branch=os.getenv('source_branch')
target_branch=os.getenv('target_branch')
event_name=os.getenv('event_name')

#valid jira tickets
jira_host_secret_name = os.environ['jira_host_secret_name']
jira_token_secret_name = os.environ['jira_token_secret_name']
prophecy_pipelines=os.getenv('prophecy_pipelines')
new_files=os.getenv('new_files')
updated_files=os.getenv('updated_files')
deleted_files=os.getenv('deleted_files')
filedetails = {}
# filedetails['Newly_Added']=new_files.split()
# filedetails['Updated']=updated_files.split()
# filedetails['Deleted']=deleted_files.split()


def prophecy_filter(input_list):
    # find changed files of prophecy projects and fetch corresponding wheel files of it, replace changed files with corresponding wheel file name	
    prophecy_pipelines = list(set(['prophecy/pipelines/' + item.split('/')[2] + '/code/dist/' for item in input_list if item.startswith('prophecy/pipelines') and len(item.split('/')) > 2]))
    print(prophecy_pipelines) 
    non_prophecy = [item for item in input_list if not item.startswith('prophecy')]
    wheel_files = []
    for folder in prophecy_pipelines:
        folder_path = os.path.join(os.environ['GITHUB_WORKSPACE'], folder)
        if os.path.exists(folder_path):
          for filename in os.listdir(folder_path):
              if filename.endswith('.whl'):
                  wheel_files.append('prophecy_executables/'+filename)
    
    if prophecy_pipelines:
        non_prophecy.extend(wheel_files)
    return non_prophecy
	
filedetails['Newly_Added'] = prophecy_filter(new_files.split())
filedetails['Updated'] = prophecy_filter(updated_files.split())
filedetails['Deleted'] = prophecy_filter(deleted_files.split())
#if some prophecy pipelines have updated /deleted and newly added files, those pipeline wheel file is considered as updated wheel file in audit table
filedetails['Newly_Added'] = [item for item in filedetails['Newly_Added'] if item not in filedetails['Updated']]
filedetails['Deleted'] = [item for item in filedetails['Deleted'] if item not in filedetails['Updated']]

#Connection Details to login to azure,generate access token, get info from key vault
clientId = os.getenv('CLINET_ID')#applicationId
clientSecret = os.getenv('CLINET_SECRET') #password
tenantID = os.getenv('TENANT_ID') #tenantID
key_vault_name = os.getenv('key_vault_name')
secret_name = os.getenv('secret_name')
pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']
github_token = os.environ['github_token']
github_api_version = os.environ['github_api_version']
headers = {
  'Accept': 'application/vnd.github+json',
  'Authorization': f'Bearer {github_token}',
  'X-GitHub-Api-Version': github_api_version,
}


#GET PULL REQUEST BODY
def get_pull_request_info(organization, repo_name, pull_request_id,headers1):
    '''This function is used to get the pull request details'''    
    pull_request_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_request_id}'
    changed_files_list = []
    head_commit_title = ''
    try:
        pull_request_data = requests.get(pull_request_url, headers=headers1, timeout=60)
        if pull_request_data.status_code == 200:
            pull_request_info = pull_request_data.json()
            pull_request_body = pull_request_info['body']
        else:
            raise ValueError(f"Invalid pull request number: {pull_request_id}")
    except Exception as e:
        raise ValueError(f"Error: Unable to retrieve pull request '{pull_request_id}' - {e}")    
    
    return pull_request_body

#Check Ticket is Valid
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
        de_ticket_matches = re.findall(r'DE-\d+', pull_request_body) #re.findall(r'(DE-\d+|DTRM-\d+|DPS-\d+)', pull_request_body)
    
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
                print("Failure - JIRA Ticket Validation: The Jira story provided in the Pull Request is found be an invalid ticket .")
                #raise ValueError("Failure - JIRA Ticket Validation: The Jira story provided in the Pull Request is found be an invalid ticket .")

    except Exception as e:
        print(f"Error - JIRA Ticket validation: No DE JIRA ticket provided in the body section of the Pull Request - {e}")
        #raise ValueError(f"Error - JIRA Ticket validation: No DE JIRA ticket provided in the body section of the Pull Request - {e}")
    #return jira_validation,valid_jira_ticket
    return valid_jira_ticket

#fetch semgrepscan details from pr comment
def get_semgrep_scan_details(ORGANIZATION, repo, pull_number):
    try:
        # Construct the URL for the GitHub API request
        url = f'https://api.github.com/repos/{ORGANIZATION}/{repo}/pulls/{pull_number}/comments'
        
        # Make the GET request to the GitHub API
        response = requests.get(url, headers=headers, timeout=60)
        
        # Check if the response status code is 200 (OK)
        if response.status_code == 200:
            data = response.json()
            semgrep_scan_list = []
            
            # Iterate through the comments in the response data
            for d in data:
                # Check if the comment is made by the semgrep bot
                if d['user']['login'] == 'semgrep-prod-digitalinnovation[bot]':
                    # Search for the findings ID and status in the comment body
                    findings_id_match = re.search(r'Finding id: (\d+)', d['body'])
                    #findings_id_match = re.search(r'https://semgrep\.dev/orgs/mns/findings/(\d+)', d['body'])
                    status_id_match = re.search(r'https://img\.shields\.io/badge/status-([a-zA-Z0-9]+)', d['body'])
                    #print(d['body'])
                    # If both findings ID and status are found, extract and store them
                    if findings_id_match:
                        findings_id = findings_id_match.group(1)
                        findings_id = f'"https://semgrep.dev/orgs/mns/findings/{findings_id}'
                        if status_id_match:
                            status = status_id_match.group(1)
                        else:
                            pattern = r"Fixed in commit ([a-f0-9]{40})"
                            match = re.search(pattern, d['body'])
                            if match:
                                status = match.group(1)
                                status = 'Fixed in commit ' + status
                            else:    
                                status = 'NA'
                        semgrep_scan = {'findings_id': findings_id, 'status': status, 'path': d['path']}
                        semgrep_scan_list.append(semgrep_scan)
            
            # Remove duplicate entries from the list
            if semgrep_scan_list:
                semgrep_scan_list = [dict(t) for t in {tuple(d.items()) for d in semgrep_scan_list}]
                semgrep_scan = str(semgrep_scan_list).replace("'",'"')
            else:
                semgrep_scan = ""
            
            return semgrep_scan    
    except Exception as e:
        # Print the error message if an exception occurs
        print(f"An error while fetching semgrep scan details: {e}")
        semgrep_scan = ""
        return semgrep_scan



#TO GENERATE AZURE ACCESS TOKEN
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

#TO GENERATE GITHUB AUDIT TABLE SQL SERVER DETAILS
def generate_audit_sql_cred(key_vault_name,secret_name,az_access_token):
  '''This function is to get the  connection details from key vault'''

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
      print("Connection details collected")
      return result['value']
    else:
      print(f"Error: {response.status_code},{response.text}")
        
  except:
    raise Exception (response.text)

#TO INSERT AUDIT TABLE
def audit_insert(audit_columns,audit_column_values,github_audit_table,cursor):
  '''This function is to insert the audit details into the audit table'''
  
  insert_query = f"INSERT INTO {github_audit_table} ({audit_columns}) VALUES ({audit_column_values})"
  cursor.execute(insert_query)
  cursor.commit() 

#function to update audit table for pbt test status
# function to update approver details
def updateaudit(cursor,repo_name, source_branch, pr_number, run_id, github_audit_table):
  '''This function is to update the audit table with the approver details'''
  try:
    wheel_files_test_status_encoded = os.environ['wheel_files_test_status_encoded']
    wheel_files_test_status_decoded = base64.b64decode(wheel_files_test_status_encoded).decode()
    wheel_files_test_status_decoded = json.loads(wheel_files_test_status_decoded)
    print(wheel_files_test_status_decoded)
    print(type(wheel_files_test_status_decoded))

    # update pbt test status for prophecy pipelines
    for file in wheel_files_test_status_decoded:
      update_query = f'''update {github_audit_table} set unit_test_status = '{file["wheelfile_test_status"]}'
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file["wheel_file_full_path"]}';'''
      cursor.execute(update_query)
      cursor.commit()
  except Exception as e:
    print(f"There is a issue occured when updating records in audit table Error: {str(e)}")
    print("Exiting!!")
    exit(0)


#invoke function to generate azure bearer token
az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)

#invoke function to get jira id list
#generate_audit_sql_cred - function will be reused to get akv secret details
print("fetching jira host url")
jira_host_url = generate_audit_sql_cred(key_vault_name,jira_host_secret_name,az_access_token)
print("fetching jira auth pat token")
jira_auth_pat = generate_audit_sql_cred(key_vault_name,jira_token_secret_name,az_access_token)
print("fetching github pat token")
github_pat = generate_audit_sql_cred(key_vault_name,pat_github_secret_key,az_access_token)
headers1 = {
  'Accept': 'application/vnd.github+json',
  'Authorization': f'Bearer {github_pat}',
  'X-GitHub-Api-Version': github_api_version,
}
pull_request_body= get_pull_request_info(repo_owner, repo_name, pr_number,headers1)

print(pull_request_body)
valid_jira_ticket_list = str(validate_jira_ticket(pull_request_body,jira_host_url,jira_auth_pat))
valid_jira_ticket_list = valid_jira_ticket_list.replace("'", '"')

# collected sql server db connection details by invoking "generate_audit_sql_cred" function
#output got in string format, converted to dictionary using eval
print("fetching audit conn details")
audit_conn = eval(generate_audit_sql_cred(key_vault_name,secret_name,az_access_token))
#Audit Table details
server = audit_conn['audit_server']
database = audit_conn['audit_database']
username = audit_conn['user_name']
password = audit_conn['password']

github_audit_table = os.getenv('github_audit_table')
driver= '{ODBC Driver 17 for SQL Server}'  # Adjust the driver as needed

#GITHUB AUDIT TABLE COLUMN NAMES
audit_columns = 'repo_name, repo_owner, repo_url,event_name ,event_owner,run_id,pr_number, pr_title, pr_url, pr_status, pr_action, pr_author,pr_merged_status, pr_merged_by, pr_merged_tms, pr_merged_commit_id,source_branch, target_branch , changed_file,changed_file_status ,event_tms,sit_approver,sit_approved_status,ppd_approver,ppd_approved_status, prod_approver, sit_deployment_status, ppd_deployment_status, prod_deployment_status, snyk_report_link, semgrep_report_link,comment,jira_id'

if not new_files and not updated_files and not deleted_files:
	print ("No new changes in PR with respect to Main branch,No records inserted")
else:      
  try:
    conn = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};DRIVER={driver}')
    cursor = conn.cursor()                        
  except Exception as e:
    print(f"There is an issue occured when connection established with Audit Table audit table, Error: {str(e)}")
    print("Exiting!!")
    exit(0)

  dedup_list = ''
  if pr_action == 'closed' and (pr_merged_by != '' or pr_merged_by is not None):
    try:
      review_list = requests.get(f'https://api.github.com/repos/{repo_owner}/{repo_name}/pulls/{pr_number}/reviews', headers=headers, timeout=60)
      approved_review_list = [x['user']['login'] for x in json.loads(review_list.text) if x['state'].upper() == 'APPROVED']
      dedup_list = (',').join(list(set(approved_review_list)))
      print(f'folllowing members have approved the req: {dedup_list}')
    except Exception as e:
      print(f"There is an issue occured when getting PR approver details, Error: {str(e)}")
      print("Exiting!!")
      exit(0)

  try:
    #insert newly added files info
    #semgrep_report_link = f'https://semgrep.dev/orgs/mns/findings?repo={repo_owner}/{repo_name}&ref=refs/pull/{pr_number}/merge'
    #semgrep_scan = get_semgrep_scan_details(repo_owner,repo_name,pr_number)    
    semgrep_report_link = ''
    semgrep_scan = ''
    #insert newly added files info
    for changed_file_status,files in filedetails.items():
      for file in files:
        #values to be passed to audit table
        audit_column_values = f"'{repo_name}','{repo_owner}','{repo_url}','{event_name}','{event_owner}','{run_id}','{pr_number}','{pr_title}','{pr_url}','{pr_status}','{pr_action}','{pr_author}','{pr_merged_status}','{pr_merged_by}',NULLIF('{pr_merged_tms}',''),'{pr_merged_commit_id}', '{source_branch}', '{target_branch}', '{file}' ,'{changed_file_status}','{event_tms}','','','','','{dedup_list}','','','','','{semgrep_report_link}','{semgrep_scan}','{valid_jira_ticket_list}'"
        audit_insert(audit_columns,audit_column_values,github_audit_table,cursor)
        print(f"'{audit_column_values}' details inserted successfully")
    #if changes have prophecy pipelines,then update pbt test status as well  
    if prophecy_pipelines == 'true':
      updateaudit(cursor,repo_name, source_branch, pr_number, run_id, github_audit_table) 
    cursor.close()
    conn.close()

  except Exception as e:
    print(f"There is an issue occured when inserting audit table, Error: {str(e)}")
    print("Exiting!!")
    exit(0)
