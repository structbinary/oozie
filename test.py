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
build_result = "build.json"


''' 
I'll parse the coordinator json and will return the hive, pig script path in dictionary
'''
def parse_json_object(data):
    workflow_dict = defaultdict(dict)
    for items in data:
        if str(items["fields"]["type"]) == "oozie-workflow2":
            data_item = items["fields"]["data"]
            try:
                decoded = json.loads(data_item)
                for item in decoded['workflow']['nodes']:
                    if item['type'] == "pig-widget":
                        print("[INFO] pig script path is: %s" %(item['properties']['script_path']))
                        workflow_dict['PIG']["source_artifact"] = str(item['properties']['script_path'])
                        workflow_dict["PIG"]["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "hive-widget":
                        print("[INFO] hive script path is: %s" %(item['properties']['script_path']))
                        print(decoded['workflow']['properties']['deployment_dir'])
                        workflow_dict['HIVE']["source_artifact"] = str(item['properties']['script_path'])
                        workflow_dict['HIVE']["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "shell-widget":
                        print(decoded['workflow']['properties']['deployment_dir'])
                        for shell_files in item['properties']['files']:
                            print("[INFO] shell script path is: %s" %(shell_files['value']))
                            workflow_dict['SHELL']["source_artifact"] = str(shell_files['value'])
                            workflow_dict['SHELL']["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir'])
                    elif item['type'] == "spark-widget":
                        print("[INFO] Spark artifact name is: %s" %(item['properties']['jars']))
                        workflow_dict["GAVR"]["source_jar"] = str(item['properties']['jars'])
                        workflow_dict["GAVR"]["hdfs_path"] = str(decoded['workflow']['properties']['deployment_dir']) + "/lib"
                    else:
                        continue
            except(ValueError, KeyError, TypeError):
                print("[WARNING] in coordinator JSON")
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
def check_artifact_on_vcs(final_dict , cordinator_path):
    cordinator_full_path = cordinator_path
    job_properties_path = str(os.path.dirname(os.path.abspath(cordinator_full_path))) + "/job.properties" 
    key_list = final_dict.keys()
    result = defaultdict(dict)
    for key_item in key_list:
        if key_item == "GAVR":
            print("[INFO] Going to check nexus")
            gavr_list = fetch_repo_path(key_item)
            output = check_artifact_from_nexus(gavr_list, final_dict[key_item])
            result = merge_two_dicts(result, output)
        else:
            artifact_path = final_dict[key_item]['source_artifact']
            print(artifact_path)
            artifact = os.path.basename(artifact_path)
            repository_path_list = fetch_repo_path(key_item)
            file_found = False
            found = False
            for each_path in repository_path_list:               
                full_path =  os.getcwd()+ "/" + each_path + "/" + artifact
                my_file = Path(full_path)
                if my_file.is_file():
                    file_found = True
                    result[key_item]["source_path"] = full_path
                    result[key_item]["hdfs_path"] = final_dict[key_item]['hdfs_path']
                    found = True
                    continue
                else:
                    if found:
                        # print("[INFO] The artifact: %s has allready been found" %(key_item))
                        continue
                    file_found = False
            if file_found:
                print("[INFO] The artifact of %s exist in this path: %s" %(key_item, my_file))
            else:
                print("[ERROR] The artifact of %s does not exist in the repository so exiting" %(key_item))
                sys.exit(1)
    result["job_properties_path"] = job_properties_path
    return result

def get_full_path(file_name, path):
    full_path = os.getcwd()+ "/" + path + "/" + file_name
    return full_path

def check_artifact_from_nexus(url_list, artifact_name):
    result = defaultdict(dict)
    file_found = False
    found = False
    for each_nexus_url in url_list:
        print('[INFO] Going to check artifact on this url: %s' %(each_nexus_url))
        try:
            filename = each_nexus_url[each_nexus_url.rfind("/")+1:]
            if filename == artifact_name['source_jar']:
                data = requests.get(each_nexus_url)
                if data.status_code == 200:            
                    print("[INFO] Artifact has been found on the above url")    
                    found = True
                    file_found = True
                    result["GAVR"]["hdfs_path"] = artifact_name['hdfs_path']
                    result["GAVR"]["source_path"] = each_nexus_url
                    continue
                else:
                    print("[ERROR] Problem in download artifact from nexus")
            else:
                if found:
                    continue
                else:
                    continue
        except(Exception) as e:
            print("[ERROR] Problem downloading the artifact. The error is: ")
            print(e)
            sys.exit(1)

    if not file_found:
        print("[ERROR] This artifact: %s does not exist in nexus" %(artifact_name['source_jar']))
        sys.exit(1)

    else:
        return result   


''' 
I'll check whether the file is present on the required directory or not
'''
def check_workflow_exist_or_not(final_dict, pig_path, hive_path, shell_path):
    key_list = final_dict.keys()
    for key_item in key_list:
        if key_item == "pig":
            pig_file_name = os.path.basename(final_dict[key_item])
            path = get_full_path(pig_file_name, pig_path)
        elif key_item == "hive":
            hive_file_name = os.path.basename(final_dict[key_item])
            path = get_full_path(hive_file_name, hive_path)
        elif key_item == "shell":
            shell_file_name = os.path.basename(final_dict[key_item])
            path = get_full_path(shell_file_name, shell_path)
        else:
            continue
        exists = os.path.isfile(path)
        if exists:
            print("[INFO] The artifact of %s exist in the repository" %(key_item))
        else:
            print("[ERROR] The artifact of %s does not exist in the repository so exiting" %(key_item))
            sys.exit(1)

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
                    output = check_artifact_on_vcs(final_dict, each_file_in_cordinator)
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
