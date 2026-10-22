import os
import requests
import json
import xml.etree.ElementTree as ET
import time
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,From, Personalization, Bcc, To
import sys
import re
import pyodbc
import base64
#from pyspark.sql import functions as F
from io import StringIO
import pandas as pd

# Retrieve the list of pipelines from the environment variable
p_pipelines = os.getenv('p_pipelines')
#Connection Details to login to azure,generate access token, get info from key vault
clientId = os.environ['CLINET_ID']
clientSecret = os.environ['CLINET_SECRET']
tenantID = os.environ['TENANT_ID']
key_vault_name = os.environ['key_vault_name']
repo_name = os.environ['repo_name']
organization = os.environ['repo_owner']
run_id = os.environ['run_id']
pull_number = os.environ['pr_number']
#find crq number from pr_title
pr_title = os.environ['pr_title']
crq_match = re.search(r'[cC][rR][qQ]\d+', pr_title)
if crq_match:
    crq_number = crq_match.group(0)
else:
    crq_number = ''  
print(crq_number)
#finding pbt test happened on whch phase during prod/non prod deployment
pr_merged_status = os.environ['pr_merged_status']
if pr_merged_status =='false':
    test_time = '(TESTING)'
else:
    test_time = '(PR MERGE FOR PRODUCTION)' 


pat_github_secret_key = os.environ['PAT_GITHUB_SECRET_KEY']
sendgrid_secretname = os.environ['sendgrid_secret_name']
github_actor = os.environ['github_actor']
print("github actor:",github_actor)
github_api_version = os.environ['github_api_version']

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
  

   

# Function to fetch PBT test output for a list of pipelines
def fetch_pbt_test_output(pipelinelist):
    pbttestdata_list = []
    pbttestfailed_data = []
    # Iterate over each pipeline in the list
    for p in pipelinelist: 
        # Construct the folder path for each pipeline
        folder_path = f'{workspacepath}{p}/code/'
        # Define the paths for the report and coverage XML files
        report_path = f'{folder_path}report.xml'
        coverage_path = f'{folder_path}coverage.xml'
        # Check if the folder exists
        if os.path.exists(folder_path):
            pipeline_data = {'pipeline': p}
            # If the report file exists, parse it to extract test data
            if os.path.exists(report_path):
                with open(report_path, 'r') as file:
                    xml_content = file.read()
                    root = ET.fromstring(xml_content)
                    testsuite = root.find('testsuite')
                    if testsuite is not None:
                        errors = int(testsuite.get('errors'))
                        failures = int(testsuite.get('failures'))
                        skipped = int(testsuite.get('skipped'))
                        tests = int(testsuite.get('tests'))
                        succeeded = tests - (failures + skipped+errors)
                        if succeeded == tests and tests > 0:
                            status = "Passed"
                        else:
                            status = "Failed"    
                        pipeline_data.update({
                            'suite_name': testsuite.get('name'),
                            'errors': errors,
                            'failures': failures,
                            'skipped': skipped,
                            'tests': tests,
                            'time': testsuite.get('time'),
                            'timestamp': testsuite.get('timestamp'),
                            'testcase_available': [testcase.get('name') for testcase in testsuite.findall('testcase')],
                            'succeeded': succeeded,
                            'status': status
                        })
                    # Extracting details from each testcase
                    for testcase in testsuite.findall('testcase'):
                        case_name = testcase.get('name')
                        case_classname = testcase.get('classname')
                        case_time = testcase.get('time')
                        
                        print(f"\nTest Case Name: {case_name}")
                        print(f"Class Name: {case_classname}")
                        print(f"Time: {case_time}")

                        # Check for failure details
                        failure = testcase.find('failure')
                        if failure is not None:
                            failure_message = failure.get('message')
                            pbttestfailed_data.append({
                                "pipeline": p,
                                "unittestcase":case_name,
                                "failure_message": failure_message
                            })    

            # If the coverage file exists, parse it to extract coverage data
            if os.path.exists(coverage_path):
                with open(coverage_path, 'r') as file:
                    xml_content = file.read()
                    root = ET.fromstring(xml_content)
                    pipeline_data.update({
                        'lines_valid': root.get('lines-valid'),
                        'lines_covered': root.get('lines-covered'),
                        'line_rate': root.get('line-rate')
                    })
            # Add the pipeline data to the list
            pbttestdata_list.append(pipeline_data)

    return pbttestdata_list,pbttestfailed_data

def get_file_content_from_github():


    # Authorization header for GitHub API call
    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {github_pat}',
        'X-GitHub-Api-Version': '2022-11-28',
    }
    # Constants
    GITHUB_API_URL = "https://api.github.com/repos"
    OWNER = "DigitalInnovation"  # Replace with the repo owner's username
    REPO = "people"    # Replace with the repository name
    FILE_PATH = "users.csv"  # Path to the file in the repo
    BRANCH = "master"  # Replace with the branch name if necessary

    # Construct the API URL
    url = f"{GITHUB_API_URL}/{OWNER}/{REPO}/contents/{FILE_PATH}?ref={BRANCH}"
    print(url)
    # Send the GET request
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Get the file content
        file_info = response.json()
        file_content = base64.b64decode(file_info['content']).decode('utf-8')

        print("File Content:")
        return(file_content)
    else:
        print(f"Error: {response.status_code}")
        print(response.json())


def sendpbtteststatus(repo_name,organization,pull_number,sender_email,sender_group,sendgrid_api_key,toemail,pbttestdatalist,pbttestfailed_data,status):
    '''This function is to send pbt test status email'''
    #Compose email subject and content
    pull_request_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}'
    subject = f"Unit Test Status Report: PR{pull_number}-{crq_number}-{run_id}"
    #filelist = '<table border="1" cellspacing="8" cellpadding="8"><tr><th> Pipeline </th><th> Unit Test Cases </th><th> Tests </th><th> Succeeded </th><th> Errors </th><th> Failures </th><th> Skipped </th><th> Time Taken </th><th> Tested Timestamp </th><th> Lines Valid </th><th> Lines Covered </th><th> Line Rate </th><th> Test Status </th></tr>'
    filelist = '<table border="1" cellspacing="8" cellpadding="8"><tr><th> Pipeline </th><th> Unit Test Cases </th><th> Tests </th><th> Succeeded </th><th> Errors </th><th> Failures </th><th> Skipped </th><th> Time Taken </th><th> Tested Timestamp </th><th> Test Status </th></tr>'
  
    for data in pbttestdatalist:
        #filelist += '<tr><td align = "center">' + data['pipeline'] + '</td><td align = "center">' + str(data['testcase_available']) + '</td><td align = "center">' + str(data['tests']) + '</td><td align = "center">' + str(data['succeeded']) + '</td><td align = "center">' + str(data['errors']) + '</td><td align = "center">' + str(data['failures']) + '</td><td align = "center">' + str(data['skipped']) + '</td><td align = "center">' + data['time'] + '</td><td align = "center">' + data['timestamp'] + '</td><td align = "center">' + data['lines_valid'] + '</td><td align = "center">' + data['lines_covered'] + '</td><td align = "center">' + data['line_rate'] + '</td><td align = "center">' + data['status'] + '</td></tr>'
        filelist += '<tr><td align = "center">' + data['pipeline'] + '</td><td align = "center">' + str(data['testcase_available']) + '</td><td align = "center">' + str(data['tests']) + '</td><td align = "center">' + str(data['succeeded']) + '</td><td align = "center">' + str(data['errors']) + '</td><td align = "center">' + str(data['failures']) + '</td><td align = "center">' + str(data['skipped']) + '</td><td align = "center">' + data['time'] + '</td><td align = "center">' + data['timestamp'] + '</td><td align = "center">' + data['status'] + '</td></tr>'

    filelist += '</table>'
    print(filelist)
    failedlist = ''
    if len(pbttestfailed_data) > 0:
        failedlist = f'<h2> <b> FAILED UNIT TEST REPORT {test_time} </b> </h2><br><br> <table border="1" cellspacing="8" cellpadding="8"><tr><th> Pipeline </th><th> Failed Unit Test Case</th><th> Failure Message </th></tr>'
        for data in pbttestfailed_data:
            failedlist += '<tr><td align = "center">' + data['pipeline'] + '</td><td align = "center">' + str(data['unittestcase']) + '</td><td align = "center">' + str(data['failure_message']) + '</td></tr>'

        failedlist += '</table>'      
    
    content = f'<p> Hi Team,<br>\n<br>\nThis is an automated notification to confirm that Unit testing has been done on the {repo_name} repository for the recent pull request . <br> <br> <h2> UNIT TEST STATUS REPORT {test_time} </h2> <br> {filelist}  <br> <br> {failedlist} \n \n \n<br> <br> <b> {status} </b>  <br> <br> \n \n  Please refer PR for more details <a href=\"https://github.com/{organization}/{repo_name}/pull/{pull_number}">Pull Request</a>.<br><br>\n \n \n <br> <br>\n <br> Thanks & Regards,<br>Data Tech SST Team<br>'
    pattern = r"(^\s+)"
    replacement = r""
    message2 = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/issues/{pull_number}/comments', headers=headers, json={"body": message2}, timeout=60)
        
    #Create email object with below composed variables
    message = Mail(
        from_email=From(sender_email,sender_group),
        to_emails=[To(toemail)],
        subject=subject,
        html_content=content)
    if toemail is not None:
        #Call SendGrid API to send email
        send_grid = SendGridAPIClient(sendgrid_api_key)
        response = send_grid.send(message)
        if response.status_code == 202 or response.status_code == 201 or response.status_code == 200:
            print("Mail Send Successfully to GITHUB ACTOR")
     




#get accesstoken
az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)
sendgriddetails = eval(get_secrets_from_kv(key_vault_name,sendgrid_secretname,az_access_token))
#Sendgrid details
sender_email = sendgriddetails['sender-email-id']
sendgrid_api_key = sendgriddetails['sendgrid-api-key']
sender_group = sendgriddetails['sender-groupid'] 
    
github_pat = get_secrets_from_kv(key_vault_name,pat_github_secret_key,az_access_token)
headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {github_pat}',
    'X-GitHub-Api-Version': github_api_version,
    }

# Split the string into a list and strip whitespace
pipelinelist = [item.strip() for item in p_pipelines.split(",")]

# Construct the base path to the pipeline code within the GitHub workspace
workspacepath = os.path.join(os.environ['GITHUB_WORKSPACE'], 'prophecy/pipelines/') 

# Fetch the PBT test data for the list of pipelines
pbttestdatalist,pbttestfailed_data = fetch_pbt_test_output(pipelinelist)
# Print the list of pipeline test data
print(pbttestdatalist)
pull_request_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls/{pull_number}'
#to get overall pbt test status
for data in pbttestdatalist:
    if data['succeeded'] == data['tests'] and data['tests'] > 0:
        status = "Test Status : UNIT TESTING PASSED"
        status_flag = 0
    else:
        
        status = 'Test Status : Unit Testing Failed due to test cases are not defined for pipelines or some of the test cases failed'
        status_flag = 1
        break

#get wheel file and corresponding pbt test report
def wheelfilestatusreport(pbttestdatalist, pr_merged_status):
    wheel_files_test = []
    for data in pbttestdatalist:
        # find wheelfile path for corresponding prophecy pipelines
        wheelfile_path = f'prophecy/pipelines/{data["pipeline"]}/code/dist'
        wheelfile_folder_path = os.path.join(os.environ['GITHUB_WORKSPACE'], wheelfile_path)
        if os.path.exists(wheelfile_folder_path):
            for filename in os.listdir(wheelfile_folder_path):
                if filename.endswith('.whl'):
                    wheel_file_full_path = f'prophecy_executables/{filename}'
                    if pr_merged_status == "false":
                        wheelfile_test_status = f'Prophecy Unit Test {data["status"]}'
                    else:
                        wheelfile_test_status = f'Prophecy Unit Test {data["status"]} at PR Merge'
                    wheel_files_test.append({"wheel_file_full_path": wheel_file_full_path, "wheelfile_test_status": wheelfile_test_status})
    return wheel_files_test     
#get github user corresponding m & s mail id        
#user_details = get_file_content_from_github() 
#pdf = pd.read_csv(StringIO(user_details))

# Filter the DataFrame for the specific username and select the email column
#filtered_df = pdf[pdf['Username'] == github_actor]['Email']

#toemail = filtered_df.iloc[0] if len(filtered_df) > 0 else None
      
#sendpbtteststatus(repo_name,organization,pull_number,sender_email,sender_group,sendgrid_api_key,toemail,pbttestdatalist,pbttestfailed_data,status)

# if status_flag == 1:
#     raise SystemExit(status)

wheel_files_test = wheelfilestatusreport(pbttestdatalist, pr_merged_status)
print(wheel_files_test)
wheel_files_test = json.dumps(wheel_files_test)
wheel_files_test_status_encoded = base64.b64encode(wheel_files_test.encode()).decode()
env_file = os.getenv('GITHUB_ENV')
with open(env_file, "a") as myfile:
    myfile.write(f"wheel_files_test_status_encoded={wheel_files_test_status_encoded} \n")
