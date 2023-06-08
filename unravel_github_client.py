import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# %%
# DBRKS URL pattern
pattern = r"^https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)$"
pattern_as_text = r"https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)"
cleanRe = re.compile("<.*?>")

app_summary_map = {}
app_summary_map_list = []

events_map = {
"efficiency":"Insights to make this app resource/cost efficient",
"appFailure":"Insights to help with failure analysis",
"Bottlenecks":"Insights to make this app faster",
"SLA":"Insights to make this app meet SLA",
}

pr_number = os.getenv('PR_NUMBER')
repo_name = os.getenv('GITHUB_REPOSITORY')
access_token = os.getenv('GITHUB_TOKEN')
unravel_url = os.getenv('UNRAVEL_URL')
unravel_token = os.getenv('UNRAVEL_JWT_TOKEN')

# %%
def get_api(api_url, api_token):
    response = requests.get(api_url, verify=False, headers={"Authorization": api_token})
    json_obj = json.loads(response.content)
    return json_obj


def check_response_on_get(json_val):
    if "message" in json_val:
        if json_val["message"] == "INVALID_TOKEN":
            raise ValueError("INVALID_TOKEN")


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
                r["workspace_id"],r["job_id"], r["run_id"]
            )
            comments += "----\n"
            comments += "#### [{}]({})\n".format('Unravel url', r["unravel_url"])
            if r['app_summary']:
                # Get all unique keys from the dictionaries while preserving the order
                headers = []
                for key in r['app_summary'].keys():
                    if key not in headers:
                        headers.append(key)

                # Generate the header row
                header_row = "| " + " | ".join(headers) + " |"

                # Generate the separator row
                separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

                # Generate the data rows
                data_rows = "\n".join(
                    [
                        "| " + " | ".join(str(r['app_summary'].get(h, "")) for h in headers)
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
                                        comments += "| {}: {} |\n".format(i["key"].upper(), events_map[i['key']])
                                        comments += "|---	|\n"
                                        comments += "| ℹ️ **{}** 	|\n".format(i['title'])
                                        comments += "| ⚡ **Details**<br>{}	|\n".format(i["events"])
                                        comments += "| 🛠 **Actions**<br>{}	|\n".format(i['actions'])
                                        comments += "\n"
            comments += "</details>\n\n"

    return comments


def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = '{}/#/app/application/spark?execId={}&clusterUid={}'.format(unravel_url,appId,clusterUId)
    app_summary_map["Spark App"] = '[{}]({})'.format(appId, url)
    cluster_url = '{}/#/compute/cluster_summary?cluster_uid={}&app_id={}'.format(unravel_url,clusterUId,appId)
    app_summary_map["Cluster"] = '[{}]({})'.format(clusterUId, cluster_url)
    app_summary_map["Estimated cost"] = '$ {}'.format(summary_dict["cents"] + summary_dict["dbuCost"])
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map["Tags"] = runinfo["default_tags"]
    if 'custom_tags' in runinfo.keys():
        app_summary_map["Tags"] = {**app_summary_map["Tags"], **runinfo["default_tags"]}
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map['Autoscale'] = autoscale_dict
    else:
        app_summary_map['Autoscale'] = 'Autoscale is not enabled.'
    return app_summary_map

def get_pr_description():
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}'

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    

    pr_data = response.json()
    print("$$$$$$$$$")
    print(pr_data)
    description = pr_data['body']
    return description

# %%
def main():

    # description_json = json.loads(pr_json['description'])
    # job_run_list = get_job_runs_from_description(pr_id, description_json)
    raw_description = get_pr_description()
    print(raw_description)
    description = " ".join(raw_description.splitlines())
    description = re.sub(cleanRe, "", description)
    job_run_list = get_job_runs_from_description_as_text(pr_number, description)

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
    for run in job_run_list:
        gsp = run["workspace_id"] + "_" + run["job_id"] + "_" + run["run_id"]
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
                run["app_summary"] = fetch_app_summary(unravel_url, unravel_token, clusterUId, appId)
                # add to the list
                job_run_result_list.append(run)
        else:
            print("job_run not found: " + gsp)

    if job_run_result_list:
        # unravel_comments = re.sub(cleanRe, '', json.dumps(job_run_result_list, indent=4))
        unravel_comments = create_comments_with_markdown(job_run_result_list)


        url = url = f"https://api.github.com/repos/{repository}/issues/{pr_number}/comments"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        payload = {"body": '{}'.format(unravel_comments)}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
    else:
        print("Nothing to do without Unravel integration")
        sys.exit(0)


# %%
if __name__ == "__main__":
    main()
