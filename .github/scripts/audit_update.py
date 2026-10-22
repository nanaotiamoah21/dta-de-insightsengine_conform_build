import requests
import os
import json
import base64
import pyodbc
#use the pyodbc library to establish a connection and execute SQL queries

# Define your GitHub personal access token and repository details
authorization_pat = os.getenv('github_token')
repo_name = os.getenv('repo_name')
org_name= os.getenv('repo_owner')
run_id = os.getenv('run_id')
envname = os.environ['envname']

#Connection Details to login to azure,generate access token, get info from key vault
clientId = os.getenv('CLINET_ID')#applicationId
clientSecret = os.getenv('CLINET_SECRET') #password
tenantID = os.getenv('TENANT_ID') #tenantID
key_vault_name = os.getenv('key_vault_name')
secret_name = os.getenv('secret_name')

azsql_deployment = os.getenv('azsql_deployment')
synapse_deployment = os.getenv('synapse_deployment')
dbx_deployment = os.getenv('dbx_deployment')

github_audit_table = os.getenv('github_audit_table')
driver= '{ODBC Driver 17 for SQL Server}'  # Adjust the driver as needed
source_branch = os.getenv('source_branch')
pr_number = os.getenv('pr_number')

headers = {
	'Accept': 'application/vnd.github+json',
	'Authorization': f'Bearer {authorization_pat}',
	'X-GitHub-Api-Version': '2022-11-28',
}

#TO GENERATE ACCESS TOKEN
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
  '''This function is to get the sql server connection details from key vault'''
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

# function to update approver details
def updateaudit(cursor,col_names,col_values,repo_name,source_branch,pr_number,run_id,github_audit_table):
  '''This function is to update the audit table with the approver details'''
  try:
    select_query = f"select distinct changed_file as changed_file from {github_audit_table} where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}'"
    cursor.execute(select_query)
    file_names = cursor.fetchall()
    print(file_names)

    # update deployment status based on the file changes
    for file in file_names:
      if file[0].startswith('.github/'):
        update_query = f'''update {github_audit_table} set {col_names[0]} = '{col_values[0]}', {col_names[1]} ='{col_values[1]}', {col_names[2]} = 'NA' 
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file[0]}';'''
        cursor.execute(update_query)
        cursor.commit()
      if file[0].startswith('configs/') or file[0].startswith('jms/') or file[0].startswith('notebooks/') or file[0].startswith('schemas/')or file[0].startswith('prophecy_executables/'):
        update_query = f'''update {github_audit_table} set {col_names[0]} = '{col_values[0]}', {col_names[1]} ='{col_values[1]}', {col_names[2]} = 'Successful' 
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file[0]}';'''
        cursor.execute(update_query)
        cursor.commit()
      if file[0].startswith('sql/azsqldb/'):
        if azsql_deployment == 'false':
          sql_deployment_status = 'Failed'
        else:
          sql_deployment_status = 'Successful'
        update_query = f'''update {github_audit_table} set {col_names[0]} = '{col_values[0]}', {col_names[1]} ='{col_values[1]}', {col_names[2]} = '{sql_deployment_status}' 
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file[0]}';'''
        cursor.execute(update_query)
        cursor.commit()
      if file[0].startswith('sql/synapse/'):
        if synapse_deployment == 'false':
          synapse_deployment_status = 'Failed'
        else:
          synapse_deployment_status = 'Successful'
        update_query = f'''update {github_audit_table} set {col_names[0]} = '{col_values[0]}', {col_names[1]} ='{col_values[1]}', {col_names[2]} = '{synapse_deployment_status}' 
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file[0]}';'''
        cursor.execute(update_query)
        cursor.commit()
      if file[0].startswith('sql/databricks/'):
        if dbx_deployment == 'false':
          dbx_deployment_status = 'Failed'
        else:
          dbx_deployment_status = 'Successful'
        update_query = f'''update {github_audit_table} set {col_names[0]} = '{col_values[0]}', {col_names[1]} ='{col_values[1]}', {col_names[2]} = '{dbx_deployment_status}' 
                        where repo_name='{repo_name}' and source_branch='{source_branch}' and pr_number = '{pr_number}' and run_id='{run_id}' and changed_file = '{file[0]}';'''
        cursor.execute(update_query)
        cursor.commit()
  except Exception as e:
    print(f"There is a issue occured when updating records in  audit table Error: {str(e)}")
    print("Exiting!!")
    exit(0)

#invoke function to generate azure bearer token
az_access_token = generate_az_access_token(clientId,clientSecret,tenantID)
# collected sql server db connection details by invoking "generate_audit_sql_cred" function
#output got in string format, converted to dictionary using eval
audit_conn = eval(generate_audit_sql_cred(key_vault_name,secret_name,az_access_token))
#Audit Table details
server = audit_conn['audit_server']
database = audit_conn['audit_database']
username = audit_conn['user_name']
password = audit_conn['password']

api_url = f"https://api.github.com/repos/{org_name}/{repo_name}/actions/runs/{run_id}/approvals"

response = requests.get(f'{api_url}',headers=headers)
reviewdetails = response.json()

try:
	conn = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};DRIVER={driver}')
	cursor = conn.cursor()
except Exception as e:
	print(f"There is a issue occured when connection established with audit table Error: {str(e)}")
	print("Exiting!!")
	exit(0)

try:
	# Update approved status of SIT ENV /PPD ENV
	for rd in reviewdetails:
		for e in rd['environments']:
			#Calling function to update SIT approver details
			if e['name'] == 'SIT' and envname == 'SIT':
				col_names = ['sit_approver','sit_approved_status','sit_deployment_status']
				col_values = [rd['user']['login'],rd['state']]  
			#Calling function to update PPD approver details
			if e['name'] == 'PPD' and envname == 'PPD':
				col_names = ['ppd_approver','ppd_approved_status','ppd_deployment_status']
				col_values = [rd['user']['login'],rd['state']]
	if envname == 'PROD':
		col_names = ['source_branch','pr_number','prod_deployment_status']
		col_values = [source_branch,pr_number]

	updateaudit(cursor,col_names,col_values,repo_name,source_branch,pr_number,run_id,github_audit_table)  
	
	cursor.close()
	conn.close()
except Exception as e:
	print(f"There is a issue occured when updating records in  audit table Error: {str(e)}")
	print("Exiting!!")
	exit(0)