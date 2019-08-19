import json
import glob, os
import sys
import requests
import git
import ast
from pathlib import Path
from collections import defaultdict

project_dir = os.environ['PROJECT_DIR']
application_name = os.environ['APP_NAME']
coordinator_parent_workflow_job_properties = os.environ['COORDINATOR_PARENT_WORKFLOW_JOB_PROPERTIES']
coordinator_job_properties = os.environ['COORDINATOR_JOB_PROPERTIES']
build_result = "build.json"
print(os.environ['HIVE'])
print(os.environ['PIG'])
print(os.environ['SHELL'])


''' 
I'll parse the coordinator json and will return the corresponding workflow path in dictionary
'''
def parse_json_object(data):
    workflow_dict = defaultdict(dict)
    for items in data:
        pig_list = []
        hive_list = []
        shell_list = []
        spark_list = []
        jar_list = []
        if str(items["fields"]["type"]) == "oozie-workflow2":
            data_item = items["fields"]["data"]
            try:
                decoded = json.loads(data_item)
                deployment_dir = str(decoded['workflow']['properties']['deployment_dir'])
                print("The deployment directory found is: %s" % deployment_dir)
                for item in decoded['workflow']['nodes']:
                    if item['type'] == "pig-widget":
                        print("[INFO] pig script path is: %s" %(item['properties']['script_path']))
                        pig_list.append(str(item['properties']['script_path']))
                        pig_list = list(set(pig_list))
                                                
                    elif item['type'] == "hive-widget" or item['type'] == "hive2-widget":
                        print("[INFO] hive script path is: %s" %(item['properties']['script_path']))
                        hive_list.append(str(item['properties']['script_path']))
                        hive_list = list(set(hive_list))
                        
                    elif item['type'] == "shell-widget":
                        for shell_files in item['properties']['files']:
                            print("[INFO] shell script path is: %s" %(shell_files['value']))
                            shell_list.append(str(shell_files['value']))
                            shell_list = list(set(shell_list))
                            
                    elif item['type'] == "spark-widget":
                        print("[INFO] Spark artifact name is: %s" %(item['properties']['jars']))
                        spark_list.append(str(item['properties']['jars']))
                        spark_list = list(set(spark_list))

                    elif item['type'] == "java-widget":
                        print("[INFO] Java artifact name is: %s" %(item['properties']['jar_path']))
                        jar_list.append(str(item['properties']['jar_path']))
                        jar_list = list(set(jar_list)) 
                    else:
                        continue

            except(ValueError, KeyError, TypeError):
                print("[WARNING] in coordinator JSON")

            if shell_list:
                workflow_dict[deployment_dir]['SHELL'] = shell_list
            if pig_list:
                workflow_dict[deployment_dir]['PIG'] = pig_list
            if hive_list:
                workflow_dict[deployment_dir]['HIVE'] = hive_list
            if spark_list:
                workflow_dict[deployment_dir]['GAVR'] = spark_list
            if jar_list:
                workflow_dict[deployment_dir]['JAVA'] = jar_list

        elif str(items["fields"]["type"]) == "oozie-coordinator2":
            workflow_dict["JOB_PROPERTIES"]["coordinator_primary_key"] = str(items["pk"])
        else:
            continue
    return workflow_dict


def get_all_applications_inside_project(project_dir):
    directory_list = [os.path.join(project_dir, o) for o in os.listdir(project_dir) 
                    if os.path.isdir(os.path.join(project_dir,o))]
    return directory_list

def get_last_commit_hash():
    process = subprocess.Popen(['git', 'rev-parse', 'HEAD'], shell=False, stdout=subprocess.PIPE)
    git_head_hash = process.communicate()[0].strip()
    print("[INFO] The last commit id is %s" %(git_head_hash))
    return git_head_hash

def get_changed_cordinator(cordinator_path):
    application_path = cordinator_path + "/" + application_name
    cordinator_list = []
    repo = git.Repo(".")
    for commit in list(repo.iter_commits(max_count=1)):
        print("[INFO] The last commit hash is %s" %(str(commit)))
        print("[INFO] The affected files due to this commit is:")
        print(commit.stats.files)
        commit_files_list =  commit.stats.files
        for key, value in commit_files_list.iteritems():
            input_string = str(key)
            if application_path in input_string:
                path = Path(input_string)
                relative_path = path.relative_to(project_dir)
                appname = str(relative_path).split("/")[0]
                cordinator_list.append(appname)
            else:
                print("[ERROR] No cordinator found in this path so exiting: %s" %(application_path))
                sys.exit(1)            
    return cordinator_list


''' 
I'll return full path of workflow depending upon key
'''
def fetch_repo_path(key):
    fetch_azure_devops_variable = os.environ[key]
    fetch_azure_devops_list = ast.literal_eval(fetch_azure_devops_variable)
    if fetch_azure_devops_list:
        return fetch_azure_devops_list
    else:
        return False

def merge_two_dicts(x, y):
    tmp_dict = x.copy()
    tmp_dict.update(y)
    return tmp_dict

''' 
I'll check whether the file is present on the required directory or not
'''
def check_artifact_on_vcs(final_dict):
    parent_workflow_job_properties_path = os.getcwd() + "/" + coordinator_parent_workflow_job_properties + "/job.properties"
    coordinator_job_properties_path = os.getcwd() + "/" + coordinator_job_properties + "/job.properties"
    key_list = final_dict.keys()
    result = defaultdict(dict)
    output = defaultdict(dict)
    shell_list = []
    pig_list = []
    hive_list = []
    spark_list = []
    jar_list = []
    for key, value in final_dict.iteritems():
        if key == "JOB_PROPERTIES":
            continue
        else:
            deployment_path = key
            for inner_key, inner_value in value.iteritems():
                if inner_key == "GAVR" or inner_key == "JAVA":
                    print("[INFO] Going to check nexus")
                    url_list = fetch_repo_path(inner_key)
                    output_result = check_artifact_from_nexus(url_list, inner_value, deployment_path)
                    spark_list.append(output_result)
                elif inner_key == "JAVA":
                    print("[INFO] Going to check nexus")
                    url_list = fetch_repo_path(inner_key)
                    output_result = check_artifact_from_nexus(url_list, inner_value, deployment_path)
                    jar_list.append(output_result)
                else:
                    found_artifact = []
                    artifact_path_list = list(inner_value)
                    file_found = False
                    for artifact_path in artifact_path_list:
                        artifact = os.path.basename(artifact_path)
                        repository_path_list = fetch_repo_path(inner_key)
                        for each_path in repository_path_list:
                            full_path =  os.getcwd()+ "/" + each_path + "/" + artifact
                            my_file = Path(full_path)
                            if artifact in found_artifact:
                                continue
                            if my_file.is_file():
                                file_found = True
                                # print("[INFO] This artifact: %s has been found in this path: %s" %(artifact, each_path))
                                if inner_key in output:
                                    found_artifact.append(artifact)
                                    path = full_path + ":::" + deployment_path
                                    if inner_key == "SHELL":
                                        shell_list.append(path)
                                    elif inner_key == "HIVE":
                                        hive_list.append(path)
                                    elif inner_key == "PIG":
                                        pig_list.append(path)
                                    else:
                                        print("[ERROR] This workflow: %s is not supported yet" %(inner_key))
                                        sys.exit(1)
                                else:
                                    found_artifact.append(artifact)
                                    path = full_path + ":::" + deployment_path
                                    if inner_key == "SHELL":
                                        shell_list.append(path)
                                    elif inner_key == "HIVE":
                                        hive_list.append(path)
                                    elif inner_key == "PIG":
                                        pig_list.append(path)
                                    else:
                                        print("[ERROR] This workflow: %s is not supported yet" %(inner_key))
                                        sys.exit(1)
                            else:
                                file_found = False
                                continue
                        if file_found:
                            print("[INFO] This artifact: %s exist in this path: %s" %(artifact, my_file))
                        else:
                            print("[ERROR] This artifact: %s of %s does not exist in the repository so exiting" %(artifact, inner_key))
                            sys.exit(1)   
    if shell_list:
        result["SHELL"]["both_path_list"] = shell_list
    if hive_list:
        result["HIVE"]["both_path_list"] = hive_list
    if pig_list:
        result["PIG"]["both_path_list"] = pig_list
    if spark_list:
        result["GAVR"]["both_path_list"] = spark_list
    if jar_list:
        result["JAVA"]["both_path_list"] = jar_list
    
    result["JOB_PROPERTIES"]["coordinator_primary_key"] = str(final_dict["JOB_PROPERTIES"]["coordinator_primary_key"])
    result["JOB_PROPERTIES"]["parent_workflow_job_properties_path"] = parent_workflow_job_properties_path
    result["JOB_PROPERTIES"]["coordinator_job_properties_path"] = coordinator_job_properties_path
    return result

def get_full_path(file_name, path):
    full_path = os.getcwd()+ "/" + path + "/" + file_name
    return full_path

def check_artifact_from_nexus(url_list, artifact_list, deployment_path):
    file_found = False
    found = False
    source_artifact_list = list(artifact_list)
    result = []
    found_artifact = []
    for source_artifact in source_artifact_list:        
        for each_nexus_url in url_list:
            try:
                filename = each_nexus_url[each_nexus_url.rfind("/")+1:]
                if filename in found_artifact:
                    print("[INFO] This artifact %s has already been found" %(filename))
                    continue
                if filename == source_artifact:
                    data = requests.get(each_nexus_url)
                    if data.status_code == 200:
                        found_artifact.append(filename)            
                        print("[INFO] This Artifact: %s has been found on this url: %s" %(source_artifact, each_nexus_url))    
                        file_found = True
                        path = each_nexus_url + ":::" + deployment_path + "/lib"
                        result.append(path)
                    else:
                        found_artifact.append(filename)
                        print("[ERROR] Problem in download artifact from nexus")
                else:
                    file_found = False
                    continue
            except(Exception) as e:
                print("[ERROR] Problem downloading the artifact. The error is: ")
                print(e)
                sys.exit(1)
        print("[FOUND] artifact is %s" %found_artifact)
        if file_found:
            print("[INFO] This artifact: %s exist in this url: %s" %(source_artifact, each_nexus_url))
        else:
            print("[ERROR] The artifact of %s does not exist in the repository so exiting" %(source_artifact))
            sys.exit(1)
    return result 


def get_cordinator_json(cordinator_path, application):
    cordinator_json_path = cordinator_path + "/" + application
    current_working_dir = os.getcwd()
    os.chdir(cordinator_json_path)
    json_file_list = []
    for json_file in glob.glob("*.json"):
        print("[INFO] Found cordinator file %s of this application: %s" %(json_file, application))
        file_path = os.path.abspath(json_file)
        json_file_list.append(file_path)
    os.chdir(current_working_dir)
    return json_file_list

def main():
    do_what =  sys.argv[1]
    if do_what == "build":
        print("[INFO] Going to build the application")
        print("[INFO] Getting the cordinator file")
        result = defaultdict(dict)
        application_build  = defaultdict(dict)
        cordinator_list = get_changed_cordinator(project_dir)
        if cordinator_list: 
            print("[INFO] Found cordinator files which needs to be build")
            for item in cordinator_list:
                cordinator_file_name = get_cordinator_json(project_dir, item)
                for each_file_in_cordinator in cordinator_file_name:
                    print(each_file_in_cordinator)
                    with open(each_file_in_cordinator, 'r') as json_file:
                        data = json.load(json_file)
                    final_dict = parse_json_object(data)
                    print("[INFO] Consolidated dict is: %s" %(final_dict))
                    output = check_artifact_on_vcs(final_dict)
                    result[each_file_in_cordinator] = output
            print("[INFO] Going to log all build information")
            for key , value in result.iteritems():
                app_name = os.path.dirname(key)
                app_name = os.path.basename(app_name)
                if app_name == application_name:
                    application_build[key] = result[key]
                    with open(build_result, 'w') as json_result:
                        json_result.write(json.dumps(application_build))
                else:
                    print("[INFO] This cordinator: %s file does not belong to this application: %s so not doing anything.." %(str(key), str(app_name)))
    elif do_what == "release":
        print("[INFO] Going to deploy the application")
    else:
        print("[ERROR] Service which you want to do is not currently supported")
        sys.exit(1)

if __name__ == '__main__':
    main()
