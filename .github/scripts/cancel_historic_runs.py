import requests
import os

#take input params
github_pat = os.environ['github_pat']
repo_name = os.environ['repo_name']
workflow_name = os.environ['workflow_name']

organization = 'DigitalInnovation'
headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': f'Bearer {github_pat}',
    'X-GitHub-Api-Version': '2022-11-28',
}

#get runnig and inprogress workflow list
workflow_runs_waiting = []
workflow_runs_inprogress = []
# Get waiting workflow list
workflow_runs_waiting_stg = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/actions/workflows/{workflow_name}/runs?event=pull_request&status=waiting', headers=headers).json()
if workflow_runs_waiting_stg['total_count'] > 0:
    workflow_runs_waiting = [[x['id'],x['head_sha'],x['head_branch']] for x in workflow_runs_waiting_stg['workflow_runs']]
# Get inprogress workflow list
workflow_runs_inprogress_stg = requests.get(f'https://api.github.com/repos/{organization}/{repo_name}/actions/workflows/{workflow_name}/runs?event=pull_request&status=in_progress', headers=headers).json()
if workflow_runs_inprogress_stg['total_count'] > 0:
    workflow_runs_inprogress = [[x['id'],x['head_sha'],x['head_branch']] for x in workflow_runs_inprogress_stg['workflow_runs']]
tobe_cancelled_run = workflow_runs_waiting + workflow_runs_inprogress

#get open head branches from workflow runs
distinct_branches = list(set([x[2] for x in tobe_cancelled_run]))

#for every branch keep latest run and cancel rest
for branch in distinct_branches:
    runids = sorted([x[0] for x in tobe_cancelled_run if x[2]==branch])
    print(f'List of runids for head branch {branch} - {runids}')
    print(f'Latest runid for head branch {branch} - {sorted(runids)[-1]}')
    runids.remove(sorted(runids)[-1])
    if len(runids) > 0:
        print(f'To be cancelled runids for head branch {branch} - {sorted(runids)}')
        for runid in runids:
            print(f'Cancelling run {runid} of head branch {branch}')
            requests.post(f'https://api.github.com/repos/{organization}/{repo_name}/actions/runs/{runid}/cancel', headers=headers)
    else:
        print(f'No Historic workflow running for head branch {branch} to cancel')