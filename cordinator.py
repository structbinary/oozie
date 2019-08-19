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
    pig_list = []
    hive_list = []
    shell_list = []
    spark_list = []
    jar_list = []
    for items in data:
        if str(items["fields"]["type"]) == "oozie-workflow2":
            data_item = items["fields"]["data"]
            try:
                decoded = json.loads(data_item)
                for item in decoded['workflow']['nodes']:
                    if item['type'] == "pig-widget":
                        print("[INFO] pig script path is: %s" %(item['properties']['script_path']))
                        print("[INFO] pig deployment path is: %s" %(decoded['workflow']['properties']['deployment_dir']))
                        if "PIG" in workflow_dict:
                            pig_list.append(str(item['properties']['script_path']))
                            pig_list = list(set(pig_list))
                            workflow_dict['PIG']["source_artifact"] = pig_list
                        else:
                            pig_list.append(str(item['properties']['script_path']))
                            tmp_list = list(set(pig_list))
                            workflow_dict['PIG']["source_artifact"] = pig_list
                        workflow_dict["PIG"]["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "hive-widget" or item['type'] == "hive2-widget":
                        print("[INFO] hive script path is: %s" %(item['properties']['script_path']))
                        print(decoded['workflow']['properties']['deployment_dir'])
                        if "HIVE" in workflow_dict:
                            hive_list.append(str(item['properties']['script_path']))
                            hive_list = list(set(hive_list))
                            workflow_dict['HIVE']["source_artifact"] = hive_list
                        else:
                            hive_list.append(str(item['properties']['script_path']))
                            hive_list = list(set(hive_list))
                            workflow_dict['HIVE']["source_artifact"] = hive_list
                        workflow_dict['HIVE']["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "shell-widget":
                        print(decoded['workflow']['properties']['deployment_dir'])
                        for shell_files in item['properties']['files']:
                            print("[INFO] shell script path is: %s" %(shell_files['value']))
                            if "SHELL" in workflow_dict:
                                shell_list.append(str(shell_files['value']))
                                shell_list = list(set(shell_list))
                                workflow_dict['SHELL']["source_artifact"] = shell_list
                            else:
                                shell_list.append(str(shell_files['value']))
                                shell_list = list(set(shell_list))
                                workflow_dict['SHELL']["source_artifact"] = shell_list
                            workflow_dict['SHELL']["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "spark-widget":
                        print("[INFO] Spark artifact name is: %s" %(item['properties']['jars']))
                        if "GAVR" in workflow_dict:
                            spark_list.append(str(item['properties']['jars']))
                            spark_list = list(set(spark_list))
                            workflow_dict['GAVR']["source_artifact"] = spark_list
                        else:
                            spark_list.append(str(item['properties']['jars']))
                            spark_list = list(set(spark_list))
                            workflow_dict['GAVR']["source_artifact"] = spark_list
                        workflow_dict["GAVR"]["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir']) + "/lib"
                    elif item['type'] == "java-widget":
                        print("[INFO] Java artifact name is: %s" %(item['properties']['jar_path']))
                        if "JAVA" in workflow_dict:
                            jar_list.append(str(item['properties']['jar_path']))
                            jar_list = list(set(jar_list))
                            workflow_dict['JAR']["source_artifact"] = jar_list
                        else:
                            jar_list.append(str(item['properties']['jar_path']))
                            jar_list = list(set(jar_list))
                            workflow_dict['JAR']["source_artifact"] = jar_list
                        workflow_dict["JAR"]["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir']) + "/lib"   
                    else:
                        continue
            except(ValueError, KeyError, TypeError):
                print("[WARNING] in coordinator JSON")
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
                print("[ERROR] No cordinator found in this path: %s" %(application_path))            
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
    for key_item in key_list:
        if key_item == "GAVR" or key_item == "JAVA":
            print("[INFO] Going to check nexus")
            gavr_list = fetch_repo_path(key_item)
            output = check_artifact_from_nexus(gavr_list, final_dict[key_item], key_item)
            result = merge_two_dicts(result, output)
        elif key_item == "JOB_PROPERTIES":
            continue
        else:
            tmp_list = []
            found_artifact = []
            artifact_path_list = list(final_dict[key_item]['source_artifact'])
            file_found = False
            for artifact_path in artifact_path_list:
                artifact = os.path.basename(artifact_path)
                repository_path_list = fetch_repo_path(key_item)
                for each_path in repository_path_list:               
                    full_path =  os.getcwd()+ "/" + each_path + "/" + artifact
                    my_file = Path(full_path)
                    if artifact in found_artifact:
                        continue
                    if my_file.is_file():
                        file_found = True
                        print("[INFO] This artifact: %s has been found in this path: %s" %(artifact, each_path))
                        if key_item in output:
                            tmp_list.append(full_path)
                            tmp_list = list(set(tmp_list))
                            output[key_item]["source_path"] = tmp_list
                            found_artifact.append(artifact)
                        else:
                            tmp_list.append(full_path)
                            tmp_list = list(set(tmp_list))
                            output[key_item]["source_path"] = tmp_list
                            output[key_item]["hdfs_path"] = final_dict[key_item]['hdfs_path']
                            found_artifact.append(artifact)
                    else:
                        file_found = False
                        continue
                if file_found:
                    print("[INFO] This artifact: %s exist in this path: %s" %(artifact, my_file))
                else:
                    print("[ERROR] This artifact: %s of %s does not exist in the repository so exiting" %(artifact, key_item))
                    sys.exit(1)
            result = merge_two_dicts(result, output)
    result["JOB_PROPERTIES"]["coordinator_primary_key"] = str(final_dict["JOB_PROPERTIES"]["coordinator_primary_key"])
    result["JOB_PROPERTIES"]["parent_workflow_job_properties_path"] = parent_workflow_job_properties_path
    result["JOB_PROPERTIES"]["coordinator_job_properties_path"] = coordinator_job_properties_path
    return result

def get_full_path(file_name, path):
    full_path = os.getcwd()+ "/" + path + "/" + file_name
    return full_path

def check_artifact_from_nexus(url_list, artifact_name, key_item):
    result = defaultdict(dict)
    file_found = False
    found = False
    source_artifact_list = list(artifact_name['source_artifact'])
    print(source_artifact_list)
    print(url_list)
    tmp_list = []
    for source_artifact in source_artifact_list:
        for each_nexus_url in url_list:
            print('[INFO] Going to check artifact on this url: %s' %(each_nexus_url))
            try:
                filename = each_nexus_url[each_nexus_url.rfind("/")+1:]
                if filename == source_artifact:
                    data = requests.get(each_nexus_url)
                    if data.status_code == 200:            
                        print("[INFO] Artifact has been found on the above url")    
                        file_found = True
                        if key_item in result:
                            tmp_list.append(each_nexus_url)
                            tmp_list = list(set(tmp_list))
                            result[key_item]["source_path"] = tmp_list
                        else:
                            tmp_list.append(each_nexus_url)
                            tmp_list = list(set(tmp_list))
                            result[key_item]["source_path"] = tmp_list
                            result[key_item]["hdfs_path"] = artifact_name['hdfs_path']
                    else:
                        print("[ERROR] Problem in download artifact from nexus")
                else:
                    file_found = False
            except(Exception) as e:
                print("[ERROR] Problem downloading the artifact. The error is: ")
                print(e)
                sys.exit(1)
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
