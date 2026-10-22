import os
import requests
import json
import re
import pyodbc
import time
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,From, Personalization, Bcc, To

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

#workflow_name = os.environ['workflow_name']
head_branch_name = os.environ['source_branch']

source_branch = os.environ['source_branch']
pull_request_title = os.environ['pr_title']
pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']

sendgrid_secretname = os.environ['sendgrid_secret_name']

recieveremail = json.loads(os.environ['reciever_mail'])
techlead = recieveremail["techleads"]
releasemanagement = recieveremail["releasemanagement"]
ops = recieveremail["ops"]
toemailArr = [techlead,releasemanagement,ops]
print(toemailArr)

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
            print("AKV Secret details collected")
            return result['value']
        else:
            print(f"Error: {response.status_code},{response.text}")
    except:
        raise Exception (response.text)
  
def get_pull_request_info(organization, repo_name, pull_request_id):
    '''This function is to get the pull request details'''
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
    

def sendhotfixdeploymentnotification(repo_name,organization,pull_number,sender_email,sender_group,sendgrid_api_key,toemailArr,changed_files_list):
    '''This function is to send email notification to techleads and release management team'''
    #Compose email subject and content

    subject = 'GitHub Hot Fix Pull Request  Deployment Successful'
    filelist = '<table border="1" cellspacing="8" cellpadding="8"><tr><th> Changed Files List </th></tr>'
    for i in changed_files_list:
        filelist = filelist + '<tr><td align = "center">' +i+'</td></tr>'
    filelist = filelist + '</table>'
    print(filelist)
    subject = 'Hot Fix Pull Request  Deployment Successful'
    content = f'<p> Hi Team,<br>\n<br>\nThe  Hot Fix Pull Request  Deployment is successful for repo {repo_name}. \n<br> <br> These are list of changed files <br> {filelist}  <br> <br> \n \n  Please refer PR for more details <a href=\"https://github.com/{organization}/{repo_name}/pull/{pull_number}">Pull Request</a>.<br><br>\n Release Management Team is requested to retrospectively perform the CAB process for the hotfix.<br> <br> \n \n Tech Leads are requested to kindly synchronize these code changes in all the lower environments.  \n \n <br> <br>\n <br> Thanks & Regards,<br>Data Tech SST Team<br>'
    #Create email object with below composed variables
    message = Mail(
        from_email=From(sender_email,sender_group),
        to_emails=[To(toemailArr[0]),To(toemailArr[1]),To(toemailArr[2])],
        subject=subject,
        html_content=content)

    #Call SendGrid API to send email
    send_grid = SendGridAPIClient(sendgrid_api_key)
    response = send_grid.send(message)
    if response.status_code == 202 or response.status_code == 201 or response.status_code == 200:
        print("Mail Send Successfully to Techleads and Release Management Team")
     

def main():
    global headers, headers1

    az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)
    sendgriddetails = eval(get_secrets_from_kv(key_vault_name,sendgrid_secretname,az_access_token))
    #Sendgrid details
    sender_email = sendgriddetails['sender-email-id']
    sendgrid_api_key = sendgriddetails['sendgrid-api-key']
    sender_group = sendgriddetails['sender-groupid'] 
    
    github_pat = get_secrets_from_kv(key_vault_name,pat_github_secret_key,az_access_token)
    headers1 = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {github_pat}',
        'X-GitHub-Api-Version': github_api_version,
    }
    
    changed_files_list, pull_request_body, head_commit_title = get_pull_request_info(organization, repo_name, pull_number)
    print(changed_files_list, pull_request_body, head_commit_title)
    sendhotfixdeploymentnotification(repo_name,organization,pull_number,sender_email,sender_group,sendgrid_api_key,toemailArr,changed_files_list)

if __name__ == '__main__':
    main()
