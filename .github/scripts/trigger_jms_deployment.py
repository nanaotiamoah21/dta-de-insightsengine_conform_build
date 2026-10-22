import os
import yaml
import requests

authorization_pat = os.environ['authorization_pat']
repo_name = os.environ['repo_name']
k8s_url = os.environ['K8S_URL']
k8s_secret = os.environ['K8S_SECRET']
jms_config_list = str(os.environ["JMS_CONFIG"]).split(',')
mq_host_list = str(os.environ["MQ_HOST"]).split(',')
ref_branch = os.environ['ref_branch']

ORGANIZATION = 'DigitalInnovation'
WORKFLOW_NAME = 'github_deploy_jms.yml'
DEFAULT_BRANCH = ref_branch

headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {authorization_pat}',
    'X-GitHub-Api-Version': '2022-11-28',
}

def submit_job_run(workflow_name: str, repo_name: str, input_param, k8s_url):
    """Submit job run for input workflow in provided repo"""

    data = {
        "ref": DEFAULT_BRANCH,
        "inputs": {
            "jms_configs": str(input_param),
            "k8s_url": k8s_url,
            "aks_creds_secret_name": k8s_secret
        }
    }
    response = requests.post(f'https://api.github.com/repos/{ORGANIZATION}/{repo_name}/actions/workflows/{workflow_name}/dispatches', headers=headers, json=data, timeout=60)
    if response.status_code == 204:
        print(f'Triggered jms deployment workflow in repo {repo_name} for {input_param["release_name"]}')
    else:
        raise RuntimeError(f'Not able to despatch workflow due to: {response.text}')


for config_file in jms_config_list:
    for mq_host in mq_host_list:
        mq_host_name = mq_host.split(':')[0]
        mq_qm_name = mq_host.split(':')[1]
        
        with open(config_file) as f:
            config = yaml.safe_load(f)
        if mq_qm_name == 'A2BDTA01':
            value_file_name = f"./jms/values_{config['job_name']}_qm1.yaml"
            release_name = f"{config['job_name']}-qm1"
        else:
            value_file_name = f"./jms/values_{config['job_name']}_qm2.yaml"
            release_name = f"{config['job_name']}-qm2"
        
        jms_configs = {
            "config_file" : config_file,
            "value_file_name" : value_file_name, 
            "release_name" : release_name,
            "mq_host" : mq_host_name,
            "mq_qm" : mq_qm_name,
            "mq_channel" : os.environ['MQ_CHANNEL'],
            "kafka_servers_confluent" : os.environ['KAFKA_SERVERS_CONFLUENT'],
            "kafka_servers_enterprise" : os.environ['KAFKA_SERVERS_ENTERPRISE'],
            "landing_account" : os.environ['LANDING_ACCOUNT'],
            "k8s_namespace" : os.environ['K8S_NAMESPACE'],
            "k8s_cluster" : os.environ['K8S_CLUSTER'],
            "environment_short" : os.environ['ENVIRONMENT_SHORT'],
            "subscription_id" : os.environ['SUBSCRIPTION_ID'],
            "tenant_id" : os.environ['TENANT_ID']
        }

        submit_job_run(WORKFLOW_NAME, repo_name, jms_configs, k8s_url)

        print(f'Triggered workflow run for {value_file_name}')