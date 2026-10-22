import os
import yaml
import json

configs = os.environ['CONFIGS']
json_config = json.loads(configs.replace("'",'"'))

config_file = json_config['config_file']
value_file_name = json_config['value_file_name']
release_name = json_config['release_name']
mq_host = json_config['mq_host']
mq_qm = json_config['mq_qm']
mq_channel = json_config['mq_channel']
kafka_servers_confluent = json_config['kafka_servers_confluent']
kafka_servers_enterprise = json_config['kafka_servers_enterprise']
landing_account = json_config['landing_account']
k8s_namespace = json_config['k8s_namespace']
k8s_cluster = json_config['k8s_cluster']
environment_short = json_config['environment_short']
subscription_id = json_config['subscription_id']
tenant_id = json_config['tenant_id']

jms_default_config = json.loads(os.environ['JMS_DEFAULT_CONFIGS'].replace("'",'"'))
container_JAVA_OPTS_Xmx = jms_default_config['container_JAVA_OPTS_Xmx']
container_JAVA_OPTS_Xms = jms_default_config['container_JAVA_OPTS_Xms']
resource_limits_cpu = jms_default_config['resource_limits_cpu']
resource_limits_memory = jms_default_config['resource_limits_memory']
resource_requests_cpu = jms_default_config['resource_requests_cpu']
resource_requests_memory = jms_default_config['resource_requests_memory']


env_file = os.getenv('GITHUB_ENV')
with open(env_file, "a") as envfile:
    envfile.write(f"value_file_name={value_file_name}\n")
    envfile.write(f"release_name={release_name}\n")
    envfile.write(f"k8s_namespace={k8s_namespace}")

def merge_options(*options):
    all_opts = []
    for x in options:
        all_opts.extend(x.split())
    return " ".join(all_opts)

with open(config_file) as f:
    config = yaml.safe_load(f)

with open("./.github/jms/values-template.yaml") as f:
    data = yaml.safe_load(f)

if 'container_JAVA_OPTS_Xmx' in config.keys():
    print('mod data')
    container_JAVA_OPTS_Xmx = config['container_JAVA_OPTS_Xmx']
if 'container_JAVA_OPTS_Xms' in config.keys():
    print('mod data')
    container_JAVA_OPTS_Xms = config['container_JAVA_OPTS_Xms']
if 'resource_limits_cpu' in config.keys():
    print('mod data')
    resource_limits_cpu = config['resource_limits_cpu']
if 'resource_limits_memory' in config.keys():
    print('mod data')
    resource_limits_memory = config['resource_limits_memory']
if 'resource_requests_cpu' in config.keys():
    print('mod data')
    resource_requests_cpu = config['resource_requests_cpu']
if 'resource_requests_memory' in config.keys():
    print('mod data')
    resource_requests_memory = config['resource_requests_memory']


main_options = f"""
    {container_JAVA_OPTS_Xmx} {container_JAVA_OPTS_Xms} -Dcom.sun.management.jmxremote
    -Dspring.config.location=/etc/config/ -Dspring.profiles.active=credentials,dev
    -Dsink.type={config['sink_type']}
"""

mq_options = f"""
    -Dmq.host={mq_host} -Dmq.port=9414
    -Dmq.queue-manager={mq_qm} -Dmq.channel={mq_channel}
    -Dmq.queue={config['input_queue']}
"""

if config["sink_type"] == "confluent":
    kafka_servers = kafka_servers_confluent
else:
    kafka_servers = kafka_servers_enterprise

env = environment_short
if config["sink_type"] in ["kafka", "confluent"]:
    kafka_topic = config["kafka_topic"]
    if env.lower() in ['sit','ndev'] and '-a2061-' not in kafka_topic:
        kafka_topic=kafka_topic.replace('cust-beam-dataengr-','cust-beam-dataengr-a2061-').lower()
    else:
        pass
else:
    kafka_topic = "DUMMY_TOPIC"

kafka_options = f"""
    -Dkafka.bootstrap-server={kafka_servers}
    -Dkafka.topic={kafka_topic}
    -Dkafka.buffer-memory={config.get('kafka_buffer', 1000000)}
"""

blob_options = f"""
    -Dblob.accountName={landing_account} -Dblob.containerName=landing
    -Dblob.folderPrefix={config.get('blob_folder_prefix', 'inbound/unknown')}
    -Dblob.batchTime={config.get('blob_batch_time', '1D')}
"""

secret_options = f"""
    -Dsecrets.confluentUser=confluent-access-user
    -Dsecrets.confluentPass=confluent-access-pass
    -Dsecrets.blobAccessKey=accesskey-blb-landdir01
"""

data["namespace"] = k8s_namespace
data["cluster"] = k8s_cluster
# data["dockerTag"] = config["docker_tag"]
data["ENV"]["JAVA_OPTS"] = merge_options(
    main_options, mq_options, kafka_options, blob_options, secret_options
)
env = environment_short.lower()
data["keyvault"] = {
    "name": f"dta-eun-{env}-kv-dbk-01",
    "resourceGroup": f"dta-eun-{env}-rg-kv-01",
    "subscriptionId": subscription_id,
    "tenantId": tenant_id,
    "credentials": f"dta-eun-{env}-sp-akstokv-01",
}

data["resources"] = {
    "limits": {
        "cpu": f"{resource_limits_cpu}",
        "memory": f"{resource_limits_memory}"
    },
    "requests": {
        "cpu": f"{resource_requests_cpu}",
        "memory": f"{resource_requests_memory}"
    }
}

with open(value_file_name, "w") as f:
    yaml.dump(data, f)

print(f"##vso[task.setvariable variable=JMS_RELEASE_NAME]{release_name}")