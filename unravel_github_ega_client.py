import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import urllib3
import os
import time
import html
from jira import JIRA
from bs4 import BeautifulSoup
import markdown
from datetime import datetime, timedelta, timezone
import pytz

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
headers = {'Content-Type': 'application/json'}
# %%
# DBRKS URL pattern
pattern = r"^https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)$"
pattern_as_text = r"https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)"
cleanRe = re.compile("<.*?>")

app_summary_map = {}
app_summary_map_list = []

events_map = {
    "efficiency": "Insights to make this app resource/cost efficient",
    "appFailure": "Insights to help with failure analysis",
    "Bottlenecks": "Insights to make this app faster",
    "SLA": "Insights to make this app meet SLA",
}

# Git specific variables
pr_number = os.getenv("PR_NUMBER")
repo_name = os.getenv("GITHUB_REPOSITORY")
# access_token = os.getenv("GITHUB_TOKEN")
access_token = os.getenv("GIT_TOKEN")
pr_url = os.getenv("PR_URL")
pr_user_email = os.getenv("PR_USER_EMAIL")
pr_commit_id = os.getenv("COMMIT_SHA")
pr_base_branch = os.getenv("BASE_BRANCH")
pr_target_branch = os.getenv("TARGET_BRANCH")

# Unravel specific variables
unravel_url = os.getenv("UNRAVEL_URL")
unravel_token = os.getenv("UNRAVEL_JWT_TOKEN")

# Slack specific variables
slack_webhook = os.getenv("SLACK_WEBHOOK")

# Jira specific variables
domain = os.getenv("JIRA_DOMAIN")
email = os.getenv("JIRA_EMAIL")
project_key = os.getenv("JIRA_PROJECT_KEY")
api_token = os.getenv("JIRA_API_TOKEN")

# if "https://" in unravel_url:
#     lr_url = unravel_url.replace("https://", "http://") + ":4043"
# else:
#     lr_url = unravel_url + ":4043"
lr_url = "http://18.204.206.1:4043"

file_code_map = {}
file_code_map['df.toPandas()'] = 'toPandas() moves all the data to driver to convert the spark df to a pandas dataframe.\n\n Instead use this statement df.withColumn("<newColumn>", lit("<constant_value>"))'
file_code_map['df.collect()'] = 'Avoid collecting all the data if we require only few rows of dataframe..\n\n Instead use this statement df.take(n)'


def calculate_dates(days):
    """
    Takes number of days as input and returns the following:
    - Start and end times in ISO 8601 format with timezone offset.
    - Start and end times in Unix timestamp format.
    
    Args:
        days (int): Number of days for the date range.

    Returns:
        dict: A dictionary containing ISO 8601 and Unix timestamps for start and end times.
    """
    # Current date and time with timezone
    now = datetime.now(pytz.timezone('Asia/Kolkata'))

    # Calculate start and end times
    start_time = now - timedelta(days=days)
    end_time = now

    # Format as ISO 8601 with timezone
    iso_start = start_time.isoformat()
    iso_end = end_time.isoformat()

    # Convert to Unix timestamp (milliseconds)
    unix_start = int(start_time.timestamp() * 1000)
    unix_end = int(end_time.timestamp() * 1000)

    return {
        "iso_start": iso_start,
        "iso_end": iso_end,
        "unix_start": unix_start,
        "unix_end": unix_end
    }


days = 30
result = calculate_dates(days)
print("Start Time (ISO 8601):", result["iso_start"])
print("End Time (ISO 8601):", result["iso_end"])
print("Start Time (Unix):", result["unix_start"])
print("End Time (Unix):", result["unix_end"])


# %%
def get_api(api_url, api_token):
    response = requests.get(api_url, verify=False, headers={"Authorization": api_token,   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
})
    print(response.content)
    json_obj = json.loads(response.content)
    return json_obj


def check_response_on_get(json_val):
    if "message" in json_val:
        if json_val["message"] == "INVALID_TOKEN":
            raise ValueError("INVALID_TOKEN")
        
def get_gsp(job_id):

    url = f"{unravel_url}/api/v1/dbworkflows/api/v1/databricks/runs"

    payload = json.dumps({
    "appStatus": [
        "K",
        "F",
        "R",
        "S",
        "P",
        "U",
        "W"
    ],
    "appTypes": [
        "db"
    ],
    "start_time": result["iso_start"],
    "end_time": result["iso_end"],
    "from": 0,
    "sort": [
        {
        "startTime": {
            "order": "desc"
        }
        }
    ],
    "size": "15",
    "queryOnFinishedTime": False,
    "jobIds": [
        job_id
    ]
    })
    headers={"Authorization": unravel_token, "Origin": unravel_url,      "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15'}

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)

    return response.json()['results'][0]['id']


def get_jobid_from_job_name(job_name):


    url = f"{unravel_url}/api/v1/ds/api/v1/databricks/jobs/search"


    payload = json.dumps({
    "start_time": result["unix_start"],
    "end_time": result["unix_end"],
    "search": job_name
    })
    headers={"Authorization": unravel_token, "Origin": unravel_url,      "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15'}

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    print(response.status_code, response.text)
    return response.json()[0]['value']




# %%
def search_summary_by_globalsearchpattern(
    base_url, api_token, start_time, end_time, gsp
):
    api_url = base_url + "/api/v1/ds/api/v1/databricks/runs/" + gsp + "/tasks/summary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


# %%
def search_analysis(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/analysis"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


def search_summary(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/appsummary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


# %%
def get_job_runs_from_description(pr_id, description_json):
    job_run_list = []
    for run_url in description_json["runs"]:
        match = re.search(pattern, run_url)
        if match:
            print(run_url)
            workspace_id = match.group(3)
            job_id = match.group(4)
            run_id = match.group(5)
            job_run_list.append(
                {
                    "pr_id": pr_id,
                    "pdbrks_url": run_url,
                    "workspace_id": workspace_id,
                    "job_id": job_id,
                    "run_id": run_id,
                }
            )

    return job_run_list


# %%
def get_job_runs_from_description_as_text(pr_id, description_text):
    job_run_list = []
    print("Description:\n" + description_text)
    print("Patten: " + pattern_as_text)
    matches = re.findall(pattern_as_text, description_text)
    if matches:
        for match in matches:
            workspace_id = match[2]
            job_id = match[3]
            run_id = match[4]
            job_run_list.append(
                {
                    "pr_id": pr_id,
                    "workspace_id": workspace_id,
                    "job_id": job_id,
                    "run_id": run_id,
                }
            )
    else:
        print("no match")
    return job_run_list


# %%
def get_organization_connection(organization_url, personal_access_token):
    credentials = BasicAuthentication("", personal_access_token)
    connection = Connection(base_url=organization_url, creds=credentials)
    return connection


# %%
def create_comments_with_markdown(job_run_result_list):
    comments = ""
    if job_run_result_list:
        for r in job_run_result_list:
            comments += "----\n"
            comments += "<details>\n"
            # comments += "<img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'>\n\n"
            comments += "<summary> <img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'> <b>Workspace Id: {}, Job Id: {}, Run Id: {}</b></summary>\n\n".format(
                r["workspace_id"], r["job_id"], r["run_id"]
            )
            comments += "----\n"
            comments += "#### [{}]({})\n".format("Unravel url", r["unravel_url"])
            if r["app_summary"]:
                # Get all unique keys from the dictionaries while preserving the order
                headers = []
                for key in r["app_summary"].keys():
                    if key not in headers:
                        headers.append(key)

                # Generate the header row
                header_row = "| " + " | ".join(headers) + " |"

                # Generate the separator row
                separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

                # Generate the data rows
                data_rows = "\n".join(
                    [
                        "| "
                        + " | ".join(str(r["app_summary"].get(h, "")) for h in headers)
                    ]
                )

                # Combine the header, separator, and data rows
                comments += "----\n"
                comments += "App Summary  <sup>*Estimated cost is sum of DBUs and VM Cost</sup>\n"
                comments += "----\n"
                comments += header_row + "\n" + separator_row + "\n" + data_rows + "\n"
            if r["unravel_insights"]:
                comments += "\nUnravel Insights\n"
                comments += "----\n"
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]
                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]
                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comments += "| {}: {} |\n".format(
                                            i["key"].upper(), events_map[i["key"]]
                                        )
                                        comments += "|---	|\n"
                                        comments += "| ℹ️ **{}** 	|\n".format(
                                            i["title"]
                                        )
                                        comments += "| ⚡ **Details**<br>{}	|\n".format(
                                            i["events"]
                                        )
                                        comments += "| 🛠 **Actions**<br>{}	|\n".format(
                                            i["actions"]
                                        )
                                        comments += "\n"
            comments += "</details>\n\n"

    return comments


def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map_for_git_comment = {}
    app_summary_map_for_jira_comments = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = "{}/#/app/application/spark?execId={}&clusterUid={}".format(
        unravel_url, appId, clusterUId
    )
    app_summary_map_for_git_comment["Spark App"] = "[{}]({})".format(appId, url)
    app_summary_map_for_jira_comments["Spark App"] = "[{}|{}]".format(appId, url)
    cluster_url = "{}/#/compute/cluster_summary?cluster_uid={}&app_id={}".format(
        unravel_url, clusterUId, appId
    )
    app_summary_map_for_git_comment["Cluster"] = "[{}]({})".format(
        clusterUId, cluster_url
    )
    app_summary_map_for_jira_comments["Cluster"] = "[{}|{}]".format(
        clusterUId, cluster_url
    )

    estimated_cost = summary_dict["cents"] + summary_dict["dbuCost"]

    app_summary_map_for_git_comment["Estimated cost"] = "$ {}".format(
        round(estimated_cost, 3)
    )
    app_summary_map_for_jira_comments["Estimated cost"] = "$ {}".format(
        round(estimated_cost, 3)
    )
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map_for_git_comment["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map_for_jira_comments["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map_for_git_comment["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map_for_jira_comments["Driver Node Type"] = runinfo[
        "driver_node_type_id"
    ]
    app_summary_map_for_git_comment["Tags"] = runinfo["default_tags"]
    # tags = json.loads(runinfo["default_tags"])
    cluster_name = runinfo["default_tags"]['ClusterName']
    app_summary_map_for_jira_comments["Tags"] = runinfo["default_tags"]
    if "custom_tags" in runinfo.keys():
        app_summary_map_for_git_comment["Tags"] = {
            **app_summary_map_for_git_comment["Tags"],
            **runinfo["default_tags"],
        }
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map_for_git_comment["Autoscale"] = autoscale_dict
        app_summary_map_for_jira_comments["Autoscale"] = autoscale_dict
    else:
        app_summary_map_for_git_comment["Autoscale"] = "Autoscale is not enabled."
        app_summary_map_for_jira_comments["Autoscale"] = "Autoscale is not enabled."
    return app_summary_map_for_git_comment, app_summary_map_for_jira_comments, cluster_name


def get_pr_description():
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    pr_data = response.json()
    description = pr_data["body"]
    return description


def send_markdown_to_slack(channel, message):
    payload = {"channel": channel, "text": message, "mrkdwn": True}
    response = requests.post(slack_webhook, json=payload)
    if response.status_code == 200:
        print("Message sent successfully to Slack!")
    else:
        print(f"Failed to send message to Slack. Error: {response.text}")


def raise_jira_ticket(message):
    # Connect to Jira
    jira = JIRA(server="https://{}".format(domain), basic_auth=(email, api_token))

    # Create the issue
    issue_data = {
        "project": {"key": "CICD"},
        "summary": "Issue summary",
        "description": message,
        "issuetype": {"name": "Task"},
    }

    new_issue = jira.create_issue(fields=issue_data)

    print(new_issue)

    jira_link = "https://{}/browse/{}".format(domain, new_issue)

    return jira_link


def create_markdown_from_html(html_string):
    # Parse the HTML string
    soup = BeautifulSoup(html_string, "html.parser")

    # Find all <li> tags
    li_tags = soup.find_all("li")

    # Extract the text from each <li> tag
    bullet_points = [li.get_text(strip=True) for li in li_tags]

    # Convert bullet points to Markdown
    markdown_text = "\n".join([f"- {point}" for point in bullet_points])

    # Print the Markdown text
    print(markdown_text)

    return markdown_text


def get_pr_reviewers_list():
    # Set the GitHub access token

    # Set the API endpoint
    api_endpoint = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}"

    # Set the request headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Send the API request
    response = requests.get(api_endpoint, headers=headers)

    # Check the response status code
    if response.status_code == 200:
        # Parse the response JSON
        pr_data = response.json()

        # Get the list of reviewers
        reviewers = [reviewer["login"] for reviewer in pr_data["requested_reviewers"]]

        # Print the reviewers
        print("Reviewers:", reviewers)
        return reviewers
    else:
        print("Failed to fetch pull request details:", response.text)
        return []


def create_jira_message(job_run_result_list):
    comments = ""
    comments += "----\n"
    comments += "This Issue was automatically created by Unravel to follow up on the insights generated for the runs of the jobs mentioned in the description of pr number {} was raised to merge {} from {} to {}\n".format(
        pr_number, pr_commit_id, pr_base_branch, pr_target_branch
    )
    if job_run_result_list:
        for r in job_run_result_list:
            comments += "----\n"
            comments += "Workspace Id: {}, Job Id: {}, Run Id: {}\n\n".format(
                r["workspace_id"], r["job_id"], r["run_id"]
            )
            comments += "----\n"
            comments += "[{}|{}]\n".format("Unravel URL", r["unravel_url"])

            if r["jir_app_summary"]:
                headers = list(r["jir_app_summary"].keys())
                header_row = "|| " + " || ".join(headers) + " |\n"
                data_rows = (
                    "| "
                    + " | ".join(str(r["jir_app_summary"].get(h, "")) for h in headers)
                    + " |\n"
                )
                comments += "----\n"
                comments += "App Summary\n"
                comments += "*Estimated cost is the sum of DBUs and VM Cost\n"
                comments += "----\n"
                comments += "\n" + header_row + data_rows

            if r["unravel_insights"]:
                comments += "\nUnravel Insights\n"
                comments += "----\n"
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]
                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]
                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comments += "\n"
                                        comments += "|| {}: {} ||\n".format(
                                            i["key"].upper(), events_map[i["key"]]
                                        )
                                        comments += "| ℹ️ *{}* 	|\n".format(i["title"])
                                        comments += "| ⚡ *Details*\n{}	|\n".format(
                                            i["events"]
                                        )
                                        if "<li>" in i["actions"]:
                                            comments += "| 🛠 *Actions*\n{}	|\n".format(
                                                create_markdown_from_html(i["actions"])
                                            )
                                        else:
                                            comments += "| 🛠 *Actions*\n{}	|\n".format(
                                                i["actions"]
                                            )
                                        comments += "\n"
    return comments


def search_string_in_code(code, search_string):
    line_numbers = []
    lines = code.split("\n")
    for line_number, line in enumerate(lines, 1):
        if search_string in line:
            line_numbers.append(line_number)

    all_lines = []
    for line_number in line_numbers:
        result = []
        start_line = max(1, line_number - 3)
        end_line = min(len(lines), line_number)
        for i in range(start_line, end_line + 1):
            result.append(f"{i} {lines[i - 1]}")
        all_lines.append(result)
    return line_numbers, all_lines

def create_custom_code_block_and_add_pr_comment(code_block):
    comment = "\n```python\n"
    for code in code_block:
        comment += "{}\n".format(code)
    comment += "```\n\n"
    return comment

def perform_code_review(get_file_name_flag=False):
    # Get the changed file paths from the pull request event payload
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/files'
    response = requests.get(url, headers=headers)
    files = response.json()
    changed_files = [file['filename'] for file in files]
    if get_file_name_flag == True:
        return changed_files
    print(changed_files)

    file_contents = {}
    for file in files:
        file_url = file['raw_url']
        file_response = requests.get(file_url)
        file_content = file_response.text
        file_contents[file['filename']] = file_content
    # Personal access token (replace with your own token)


    for file_name, file_content in file_contents.items():
        for pattern, optimal_value in file_code_map.items():
            line_numbers, code_lines = search_string_in_code(file_content, pattern)

            # API endpoint
            url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/comments'

            # Request headers
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/vnd.github.v3+json',
                'X-GitHub-Api-Version': '2022-11-28'
            }

            for line_number, code_block in zip(line_numbers,code_lines):

                # Request body
                data = {
                    'body': optimal_value,
                    'path': file_name,
                    'commit_id': pr_commit_id,
                    'line': line_number
                }

                # Send POST request
                response = requests.post(url, headers=headers, data=json.dumps(data))

                # Check response status
                if response.status_code == 201:
                    print('Comment added successfully.')
                else:
                    comment = create_custom_code_block_and_add_pr_comment(code_block)
                    comment += "\n\n"
                    comment += optimal_value
                    url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/vnd.github.v3+json",
                    }
                    payload = {"body": "{}".format(comment)}
                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    
# d = '''{            "eventName": "CICDUsageEvent",
#                     "eventType": "CICD",
#                     "eventNumber": 4793,
#                     "eventTime": 1696896310332,
#                     "entityGroup": 2,
#                     "entityType": 2,
#                     "entityId": "8122943658466030_259557806380750_3341164-114",
#                     "user": "null",
#                     "queue": "null",
#                     "clusterName": "job-259557806380750-run-3341164",
#                     "clusterUid": "1013-000816-6ap9chzt",
#                     "staticRank": 20,
#                     "dynamicRank": 20.0,
#                     "title": "",
#                     "detail": "",
#                     "actions": ""
# }'''

def index_for_timestamp(prefix, ts):
    timestamp_seconds = ts / 1000
    ts = datetime.utcfromtimestamp(timestamp_seconds)
    if type(ts) == str:
        ts = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")
    d = datetime(int(year), int(month), int(day)).toordinal()
    sunday = str(datetime.fromordinal(d - (d % 7)))[8:10]
    saturday = str(datetime.fromordinal(d - (d % 7) + 6))[8:10]
    return prefix + year + month + sunday + "_" + saturday

def send_update_to_unravel(notification_sent,user_ids,jira_link,pr_url,pr_number,repo_name,es_document_list):
    detail_dict = {}
    detail_dict['agent'] = "GitHub"
    detail_dict['repository'] = repo_name
    detail_dict['prNumber'] = pr_number
    detail_dict['reviewers'] = user_ids
    detail_dict['prLink'] = pr_url
    detail_dict['fileDiff'] = perform_code_review(get_file_name_flag=True)
    detail_dict['imsLink'] = jira_link
    detail_dict['notificationSent'] = notification_sent
    for documents in es_document_list:
        documents['detail'] = str(detail_dict)
        id = documents['job']
        del documents['job']
        event_time = documents['eventTime']
        documents = json.dumps(documents)
        index = index_for_timestamp('ev-', event_time)
        body = f'{index} event {id} {5} {documents}'
        print(body)
        try:
            r = http.request('PUT', f'{lr_url}/logs/hl/hl/{id}/_bulkr', body=body, headers=headers)
            if r.status // 100 != 2:
                print(f'LR request failed: status={r.status} body={body} resp={r.data.decode()}')
            else:
                print("request sent to LR")
        except Exception as err:
            print(f'LR request failed: body={body} error={err}')



def create_es_document(gsp, cluster_name, cluster_uid, job):
    document = {}
    document['eventName'] = "CICDUsageEvent"
    document['eventType'] = "CICD"
    document['eventNumber'] = pr_number
    document['eventTime'] = int(time.time() * 1000)
    document['entityGroup'] = 2
    document['entityType'] = 2
    document['entityId'] = gsp
    document['user'] = "null"
    document['queue'] = "null"
    document['clusterName'] = cluster_name
    document['clusterUid'] = cluster_uid
    document['staticRank'] = 2
    document['dynamicRank'] = 2
    document['title'] = "CICD Document"
    document['detail'] = "detail_dict"
    document['actions'] = "null"
    document['job'] = job
    return document


# %%
def main():
    # description_json = json.loads(pr_json['description'])
    # job_run_list = get_job_runs_from_description(pr_id, description_json)
    raw_description = get_pr_description()
    es_document_list = []
    if not raw_description:
        print("Nothing to do without description, skipping!!")
        sys.exit(0)
    description = " ".join(raw_description.splitlines())
    description = re.sub(cleanRe, "", description)
    job_run_list = get_job_runs_from_description_as_text(pr_number, description)
    job_names_list = [job.strip() for job in description.split(",")]
    # start and end TS
    today = datetime.today()
    endDT = datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=today.hour,
        second=today.second,
    )
    startDT = endDT - timedelta(days=14)
    start_time = startDT.astimezone().isoformat()
    end_time = endDT.astimezone().isoformat()
    print("start: " + start_time)
    print("end: " + end_time)

    job_run_result_list = []
    for job_name in job_names_list:
        print(job_name)
        job_id = get_jobid_from_job_name(job_name)
        print(job_id)
        gsp = get_gsp(job_id)
        print(gsp)
        gsp_split = gsp.split("_")
        run = {
                    "pr_id": pr_number,
                    "workspace_id": gsp_split[0],
                    "job_id": gsp_split[1],
                    "run_id": gsp_split[2],
                }
        job_runs_json = search_summary_by_globalsearchpattern(
            unravel_url, unravel_token, start_time, end_time, gsp
        )

        if job_runs_json:
            """
            gsp_file = gsp + '_summary.json'
            with open(gsp_file, "w") as outfile:
              json.dump(job_runs_json, outfile)
            """
            clusterUId = job_runs_json[0]["clusterUid"]
            appId = job_runs_json[0]["sparkAppId"]
            print("clusterUid: " + clusterUId)
            print("sparkAppId: " + appId)

            result_json = search_analysis(unravel_url, unravel_token, clusterUId, appId)
            if result_json:
                """
                gsp_file = gsp + '_analysis.json'
                with open(gsp_file, "w") as outfile:
                  json.dump(result_json, outfile)
                """
                insights_json = result_json["insightsV2"]
                recommendation_json = result_json["recommendation"]
                insights2_json = []
                for item in insights_json:
                    # if item['key'] != 'SparkAppTimeReport':
                    insights2_json.append(item)

                run[
                    "unravel_url"
                ] = unravel_url + "/#/app/application/db?execId={}".format(gsp)
                run["unravel_insights"] = insights2_json
                run["unravel_recommendation"] = recommendation_json
                git_summary, jira_summary, c_name = fetch_app_summary(
                    unravel_url, unravel_token, clusterUId, appId
                )
                run["app_summary"] = git_summary
                run["jir_app_summary"] = jira_summary
                # add to the list
                job_run_result_list.append(run)

                es_document_list.append(create_es_document(gsp, c_name,clusterUId, run["job_id"]))

        else:
            print("job_run not found: " + gsp)

    if job_run_result_list:
        # unravel_comments = re.sub(cleanRe, '', json.dumps(job_run_result_list, indent=4))
        unravel_comments = create_comments_with_markdown(job_run_result_list)

        url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        payload = {"body": "{}".format(unravel_comments)}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        perform_code_review()

        jira_message = create_jira_message(job_run_result_list)

        jira_link = raise_jira_ticket(jira_message)

        channel = "#cicd-notifications"
        # Replace with your Markdown-formatted message
        message = "Unravel has insights for the pr number {} which was raised to merge {} from {} to {}. Click this link for further details {}, also a jira has been raised please find the jira link {}".format(
            pr_number, pr_commit_id, pr_base_branch, pr_target_branch, pr_url, jira_link
        )
        # Format the user IDs with '@' symbol
        user_ids = get_pr_reviewers_list()
        formatted_user_ids = ["@" + user_id for user_id in user_ids]

        # Create the message text with user mentions
        message_with_mentions = message + " " + " ".join(formatted_user_ids)

        try:
            send_markdown_to_slack(channel, message_with_mentions)
            notification_sent = True
        except:
            notification_sent = False

        send_update_to_unravel(notification_sent,user_ids,jira_link,pr_url,pr_number,repo_name,es_document_list)

    else:
        print("Nothing to do without Unravel integration")
        sys.exit(0)


# %%
if __name__ == "__main__":
    main()
