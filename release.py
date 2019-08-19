import os
import json
import sys
import subprocess
import requests
from collections import defaultdict

build_result = "build.json"
hdfs_back_dir = os.environ['HDFS_BACK_DIR']
oozie_url = os.environ["OOZIE_URL"]


def execute_command(command):
    print(command)
    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    output = process.communicate()[0].strip()
    return (output, process.returncode)

def get_details_of_build_process(build_result):
    with open(build_result) as json_file:
        data = json.load(json_file)
    return data

def get_backup_directory_info(data):
    print("[INFO] Taking backup first..")
    output = defaultdict(dict)
    for key , value in data.iteritems():
        app_name = os.path.dirname(key)
        app_name = os.path.basename(app_name)
        tmp_list = []
        print("[INFO] Found cordinator for this application %s" %(app_name))
        for k,v in value.iteritems():
            if app_name in output:
                tmp_list.append(k)
                tmp_list = list(set(tmp_list))
                output[app_name] = tmp_list
            else:
                tmp_list.append(k)
                tmp_list = list(set(tmp_list))
                output[app_name] = tmp_list
    print(output)
    return output

def create_subworkflow_dir(appname, workflow_info):
    create_hdfs_directory_command = "hdfs dfs -mkdir " + appname
    create_output, create_status_code = execute_command(create_hdfs_directory_command)
    if create_status_code == 0:
        for each_element in workflow_info:
            sub_dir = appname + "/" + str(each_element)
            command = "hdfs dfs -mkdir " + sub_dir
            execute_command(command)
    else:
        print("ERROR in creating backup directory in hdfs")
        sys.exit(1)

def delete_artifact_on_hdfs(hdfs_path, artifact_name):
    deleted = False
    hdfs_full_path = hdfs_path  + "/" + artifact_name
    remove_hdfs_file_command = "hdfs dfs -rm -r " + hdfs_full_path
    remove_command_output , remove_command_status_code = execute_command(remove_hdfs_file_command)
    if remove_command_status_code == 0:
        deleted = True
    else:
        print("[ERROR] Something went wrong while executing this command %s" %(remove_hdfs_file_command))
        deleted = False
    return deleted

def delete_previous_artifact(build_info):
    output = defaultdict(dict)
    for key , value in build_info.iteritems():
        for k , v in value.iteritems():
            if str(k) == "GAVR":
                gavr_url_list = list(v["source_path"])
                for gavr_url in gavr_url_list:                    
                    source_path = gavr_url
                    file_name = source_path[source_path.rfind("/")+1:]
                    hdfs_path = str(v["hdfs_path"])
                    hdfs_full_path = hdfs_path  + "/" + file_name
                    is_deleted = delete_artifact_on_hdfs(hdfs_path, file_name)
                    if is_deleted:
                        print("[INFO] Successfully removed this artifact %s" %(hdfs_full_path))                                        
            elif str(k) == "JOB_PROPERTIES":
                continue
            else:
                source_path_list = list(v["source_path"])
                for source_path in source_path_list:
                    source_path = str(source_path)
                    file_name = os.path.basename(source_path)
                    hdfs_path = str(v["hdfs_path"])
                    hdfs_full_path = hdfs_path  + "/" + file_name
                    is_deleted = delete_artifact_on_hdfs(hdfs_path, file_name)
                    if is_deleted:
                        print("[INFO] Successfully removed this artifact %s" %(hdfs_full_path))                                                            


def check_backup_directory(directory_info, hdfs_back_dir):
    print("[INFO] Going to check backup folder")
    check_backup_dir_command = "if hdfs dfs -test -d " + hdfs_back_dir + "; then echo 'exist'; fi"
    check_backup_output, check_backup_status_code = execute_command(check_backup_dir_command)
    if check_backup_output == "exist" and check_backup_status_code == 0:
        print("[INFO] Backup directory exist so going to create application specific backup directories")
        for key, value in directory_info.iteritems():
            app_name = os.path.dirname(key)
            app_name = os.path.basename(app_name)
            application_dir = hdfs_back_dir + "/" + str(app_name)
            hdfs_command = "if hdfs dfs -test -d " + application_dir + "; then echo 'exist'; fi"
            output, status = execute_command(hdfs_command)
            if output == "exist" and status == 0:
                print("[INFO] Backup directory of this application: %s already exist so recreating from fresh.." %(str(key)))
                remove_backup_command = "hdfs dfs -rm -r " + application_dir
                remove_output, remove_status = execute_command(remove_backup_command)
                if remove_status == 0:
                    create_subworkflow_dir(application_dir, value)
            else:
                print("[INFO] Backup directory of this application: %s does not exist previously so going to create.." %(str(key)))
                create_subworkflow_dir(application_dir, value)
    else:
        print("[ERROR] Backup path: %s does not exist previously in hdfs so create first." %(hdfs_back_dir))
        sys.exit(1)

def get_existing_coordinator_json(primary_key, export_backup_json_path):
    print("[INFO] Going to export existing cordinator file in json")
    json_dump_file_name = "/data_" + primary_key + ".json"
    export_previous_cordinator_command = """sudo chmod 755 /var/run/cloudera-scm-agent/process/ ; export PATH="/home/cdhadmin/anaconda2/bin:$PATH" ;export HUE_CONF_DIR="/var/run/cloudera-scm-agent/process/`ls -alrt /var/run/cloudera-scm-agent/process | grep -i HUE_SERVER | tail -1 | awk '{print $9}'`" ; sudo chmod -R 757 $HUE_CONF_DIR; HUE_IGNORE_PASSWORD_SCRIPT_ERRORS=1 HUE_DATABASE_PASSWORD=ZbNNYWakrb /opt/cloudera/parcels/CDH/lib/hue/build/env/bin/hue dumpdata desktop.Document2 --indent 2 --pks=""" + primary_key + " --natural > " + export_backup_json_path + json_dump_file_name
    export_command_output, export_command_status_code = execute_command(export_previous_cordinator_command)
    if export_command_status_code == 0:
        print("[INFO] Previous cordinator file has been exported successfully for backup")
    else:
        print("[ERROR] Something went wrong while trying to export existing cordinator with this command %s" %(export_previous_cordinator_command))
        sys.exit(1)

def import_cordinator(cordinator_file_path):
    print("[INFO] Going to import cordinator")
    import_successfull = False
    import_command = """sudo chmod 755 /var/run/cloudera-scm-agent/process/ ; export PATH="/home/cdhadmin/anaconda2/bin:$PATH" ;export HUE_CONF_DIR="/var/run/cloudera-scm-agent/process/`ls -alrt /var/run/cloudera-scm-agent/process | grep -i HUE_SERVER | tail -1 | awk '{print $9}'`" ; sudo chmod -R 757 $HUE_CONF_DIR; HUE_IGNORE_PASSWORD_SCRIPT_ERRORS=1 HUE_DATABASE_PASSWORD=ZbNNYWakrb /opt/cloudera/parcels/CDH/lib/hue/build/env/bin/hue loaddata """ + str(cordinator_file_path)
    import_command_output, import_command_status_code = execute_command(import_command)
    if import_command_status_code == 0:
        print(import_command_output)
        print("[INFO] Successfully imported the cordinator")
        import_successfull = True
    else:
        print("[ERROR] Something went wrong while executing this command: %s" %(import_command))
        import_successfull = False

    return import_successfull

def copy_from_hdfs_to_backup(hdfs_full_path, file_name, back_up_dir):
    status = False
    command_to_check_file_exist_in_hdfs = "hdfs dfs -test -e " + hdfs_full_path + " && echo 'exist'"
    check_output, check_command_status = execute_command(command_to_check_file_exist_in_hdfs)
    if check_output == "exist" and check_command_status == 0:
        print("[INFO] This artifact: %s exist in hdfs path: %s so going to take backup" %(file_name, hdfs_full_path))
        backup_copy_command = "hdfs dfs -mv" + " " + hdfs_full_path + " " + back_up_dir
        print("[INFO] Taking backup from hdfs: %s to this path: %s" %(hdfs_full_path, back_up_dir))
        backup_output , backup_result = execute_command(backup_copy_command)
        if backup_result == 0:
            print("[INFO] Backup finished for this artifact")
            status = True
        else:
            print("[ERROR] Something happened while executing this command %s" %(backup_copy_command))
            status = False
    else:
        print("[INFO] Seems like You are doing deployment firsttime so continuing..")
        status = True
    return status

def get_coordinator_primary_key(value):
    cordinator_exported = False
    for k,v in value.iteritems():
        if str(k) == "JOB_PROPERTIES":
            cordinator_primary_key = str(v["coordinator_primary_key"])
            get_existing_coordinator_json(cordinator_primary_key, '/tmp')
            cordinator_exported = True
        else:
            continue
    return cordinator_exported

def get_file_name_from_hdfs_backup(backup_dir_path):
    get_file_name_command = "hdfs dfs -ls " + backup_dir_path + " | tail -1 | awk -F' ' '{print $8}'"
    get_filename, get_file_name_status_code =  execute_command(get_file_name_command)
    if get_file_name_status_code == 0:
        print("[INFO] Found this file %s in this path: %s" %(get_filename, backup_dir_path))
        return get_filename
    else:
        return False

def move_from_backup_to_hdfs(backup_path, destination_path):
    revert_command = "hdfs dfs -mv" + " " + backup_path + " " + destination_path
    revert_output, revert_status_code = execute_command(revert_command)
    print(revert_output)
    if revert_status_code == 0:
        print("[INFO] Successfully moved from: %s to this hdfs path: %s " %(backup_path, destination_path))
        return True
    else:
        print("[ERROR] Something went wrong while executing this command: %s" %(revert_command))
        return False

def get_primary_key(value):
    cordinator_primary_key = None
    for k,v in value.iteritems():
        if str(k) == "JOB_PROPERTIES":
            cordinator_primary_key = str(v["coordinator_primary_key"])
        else:
            continue
    return cordinator_primary_key

def revert_changes(build_info, hdfs_back_dir, type):
    application_name = None
    workflow_name = None
    is_hdfs_artifact_deleted = False
    status = False
    delete_previous_artifact(build_info)
    tmp_list = []
    for key , value in build_info.iteritems():
        app_name = os.path.dirname(key)
        app_name = os.path.basename(app_name)
        application_name = str(app_name)
        dictionary_length = len(value)
        count = 0
        coordinator_primary_key = get_primary_key(value)
        for k,v in value.iteritems():
            if str(k) == "GAVR":
                gavr_url_list = list(v["source_path"])
                for gavr_url in gavr_url_list:
                    workflow_name = str(k)
                    file_name = gavr_url[gavr_url.rfind("/")+1:]
                    if file_name in tmp_list:
                        continue
                    else:
                        workflow_backup_dir = hdfs_back_dir + "/" + application_name + "/" + workflow_name + "/" + file_name
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        get_backup_file_name = get_file_name_from_hdfs_backup(workflow_backup_dir)
                        tmp_list.append(file_name)
                        if get_backup_file_name:
                            backup_happened = move_from_backup_to_hdfs(get_backup_file_name, hdfs_full_path)
                            if not backup_happened:
                                sys.exit(1)
                        else:
                            print("[ERROR] No file found in this path %s" %(workflow_backup_dir))
            elif str(k) == "JOB_PROPERTIES":
                continue                
            else:
                source_path_list = list(v["source_path"])
                for source_path in source_path_list:
                    workflow_name = str(k)
                    file_name = os.path.basename(source_path)
                    if file_name in tmp_list:
                        continue
                    else:
                        workflow_backup_dir = hdfs_back_dir + "/" + application_name + "/" + workflow_name + "/" + file_name
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        get_backup_file_name = get_file_name_from_hdfs_backup(workflow_backup_dir)
                        tmp_list.append(file_name)
                        if get_backup_file_name:
                            backup_happened = move_from_backup_to_hdfs(get_backup_file_name, hdfs_full_path)
                            if not backup_happened:
                                sys.exit(1)
                        else:
                            print("[ERROR] No file found in this path %s" %(workflow_backup_dir))

        cordinator_path = "/tmp/data_" + coordinator_primary_key + ".json"
        import_previous_cordinator = import_cordinator(cordinator_path)

def take_backup_from_hdfs_and_do_release(build_info, hdfs_back_dir, type):
    application_name = None
    workflow_name = None
    status = False
    tmp_list = []
    for key , value in build_info.iteritems():
        app_name = os.path.dirname(key)
        app_name = os.path.basename(app_name)
        application_name = str(app_name)
        is_cordinator_exported = False
        dictionary_length = len(value)
        count = 0
        export_existing_coordinator = get_coordinator_primary_key(value)
        for k,v in value.iteritems():
            if str(k) == "GAVR":
                gavr_url_list = list(v["source_path"])
                for gavr_url in gavr_url_list:
                    workflow_name = str(k)
                    file_name = gavr_url[gavr_url.rfind("/")+1:]
                    if file_name in tmp_list:
                        continue
                    else:
                        workflow_backup_dir = hdfs_back_dir + "/" + application_name + "/" + workflow_name + "/" + file_name
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        do_back_up = copy_from_hdfs_to_backup(hdfs_full_path, file_name, workflow_backup_dir)
                        tmp_list.append(file_name)
                        if not do_back_up:
                            print("[ERROR] Not able to do backup from hdfs path: %s to back up path: %s" %(hdfs_back_dir, workflow_backup_dir))
                            sys.exit(1)                    
            elif str(k) == "JOB_PROPERTIES":
                continue                
            else:
                source_path_list = list(v["source_path"])
                for source_path in source_path_list:
                    workflow_name = str(k)
                    file_name = os.path.basename(source_path)
                    if file_name in tmp_list:
                        continue
                    else:
                        workflow_backup_dir = hdfs_back_dir + "/" + application_name + "/" + workflow_name + "/" + file_name
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        do_back_up = copy_from_hdfs_to_backup(hdfs_full_path, file_name, workflow_backup_dir)
                        tmp_list.append(file_name)
                        if not do_back_up:
                            print("[ERROR] Not able to do backup from hdfs path: %s to back up path: %s" %(hdfs_back_dir, workflow_backup_dir))
                            sys.exit(1)

def download_artifact_from_nexus(nexus_url, path_to_download):
    successfully_download = False
    filename = nexus_url[nexus_url.rfind("/")+1:]
    full_path = path_to_download + "/" + filename
    if os.path.isfile(filename):
        os.remove(filename)
    else:
        with open(full_path, "wb") as file:
            response = requests.get(nexus_url)
            file.write(response.content)
        if response.status_code == 200:
            successfully_download = True
        else:
            successfully_download = False
    return (full_path, successfully_download)

def run_oozie_jobs(build_data):
    successfully_ran = False
    for key , value in build_data.iteritems():
        for k,v in value.iteritems():
            if str(k) == "JOB_PROPERTIES":
                job_properties_path = str(v["parent_workflow_job_properties_path"])
                oozie_command_line_command = "oozie job -oozie " + oozie_url + " -config " + str(job_properties_path) + " -run"
                oozie_output, oozie_status_code = execute_command(oozie_command_line_command)
                if oozie_status_code == 0:
                    print(oozie_output)
                    print("[INFO] Successfully ran the workflow as well now going to check the execution status on oozie")
                    oozie_command_output = oozie_output.split(':')[1]
                    job_status_command = "oozie job -poll " + oozie_command_output + " -oozie " + oozie_url + " -interval 10 -timeout 60  -verbose"
                    fetch_job_status, fetch_job_status_code = execute_command(job_status_command)
                    refactored_fetch_job_status = " ".join(fetch_job_status.split()) 
                    if str(refactored_fetch_job_status) == "KILLED" or str(refactored_fetch_job_status) == "FAILED":
                        successfully_ran = False
                        print("[ERROR] The polling says the jobs has been: %s , So reverting the changes" %(str(refactored_fetch_job_status)))
                        revert_changes(build_data, hdfs_back_dir, "revert")
                    else:
                        successfully_ran = True
                        print("[INFO] The polling says the jobs has been: %s." %(str(refactored_fetch_job_status)))
                else:
                    print("[ERROR] Issue while executing this command: %s , So reverting the changes" %(oozie_command_line_command))
                    successfully_ran = False
                    revert_changes(build_data, hdfs_back_dir, "revert")
            else:
                continue
    print("[INFO] Deployment done successfully..")

def copy_artifact_from_local_to_hdfs_path(local_path, hdfs_path):
    deployment_command = "hdfs dfs -copyFromLocal " + local_path + " " + hdfs_path
    deployment_output, deployment_status_code = execute_command(deployment_command)
    if deployment_status_code == 0:
        print("[INFO] Successfully copied from local: %s to hdfs path: %s" %(local_path, hdfs_path))
        return True
    else:
        print("[ERROR] Something went wrong while executing this command: %s " %(deployment_command))
        print("[ERROR] Going to revert the changes")
        return False

def copy_from_local_to_hdfs_and_import_oozie(build_data):
    output = defaultdict(dict)
    tmp_list = []
    for key , value in build_data.iteritems():
        app_name = os.path.dirname(key)
        app_name = str(os.path.basename(app_name))
        print("[INFO] Going to do deployment of this application %s" %(app_name))
        for k,v in value.iteritems():
            if str(k) == "GAVR":
                download_path = "/tmp"
                gavr_url_list = list(v["source_path"])
                for gavr_url in gavr_url_list:
                    output_path , able_to_download = download_artifact_from_nexus(gavr_url, download_path)
                    source_path = output_path
                    file_name = gavr_url[gavr_url.rfind("/")+1:]
                    if file_name in tmp_list:
                        continue
                    else:
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        copy_successfully = copy_artifact_from_local_to_hdfs_path(source_path, hdfs_full_path)
                        tmp_list.append(file_name)
                        if not copy_successfully:
                            revert_changes(build_data, hdfs_back_dir, "revert")
                            sys.exit(1)

            elif str(k) == "JOB_PROPERTIES":
                continue
            else:
                source_path_list = list(v["source_path"])
                for source_path in source_path_list:
                    source_path = str(source_path)
                    file_name = os.path.basename(source_path)
                    if file_name in tmp_list:
                        continue
                    else:
                        hdfs_full_path = str(v["hdfs_path"]) + "/" + file_name
                        copy_successfully = copy_artifact_from_local_to_hdfs_path(source_path, hdfs_full_path)
                        tmp_list.append(file_name)
                        if not copy_successfully:
                            revert_changes(build_data, hdfs_back_dir, "revert")
                            sys.exit(1)
        print("[INFO] Now going to import the cordinator via hue")
        hue_command = """sudo chmod 755 /var/run/cloudera-scm-agent/process/ ; export PATH="/home/cdhadmin/anaconda2/bin:$PATH" ;export HUE_CONF_DIR="/var/run/cloudera-scm-agent/process/`ls -alrt /var/run/cloudera-scm-agent/process | grep -i HUE_SERVER | tail -1 | awk '{print $9}'`" ; sudo chmod -R 757 $HUE_CONF_DIR; HUE_IGNORE_PASSWORD_SCRIPT_ERRORS=1 HUE_DATABASE_PASSWORD=ZbNNYWakrb /opt/cloudera/parcels/CDH/lib/hue/build/env/bin/hue loaddata """ + str(key)
        hue_command_output, hue_command_status_code = execute_command(hue_command)
        if hue_command_status_code == 0:
            print(hue_command_output)
            print("[INFO] Successfully imported the cordinator")
        else:
            print("[ERROR] Something went wrong while executing this command: %s" %(hue_command))
            revert_changes(build_data, hdfs_back_dir, "revert")
            sys.exit(1)

def main():
    do_what =  sys.argv[1]
    if do_what == "release":
        print("[INFO] Going to do deployment")
        print("[INFO] Getting all details required for deployment")
        details = get_details_of_build_process(build_result)
        backup_directory_info = get_backup_directory_info(details)
        check_backup_directory(details, hdfs_back_dir)
        take_backup_from_hdfs_and_do_release(details, hdfs_back_dir, "release")
        copy_from_local_to_hdfs_and_import_oozie(details)
    
    elif do_what == "run":
        details = get_details_of_build_process(build_result)
        run_oozie_jobs(details)

    elif do_what == "revert":
        print("[INFO] Going to revert back")
        details = get_details_of_build_process(build_result)
        revert_changes(details, hdfs_back_dir, "revert")

    else:
        print("[ERROR] This service : %s is not currently supported" %(do_what))

if __name__ == '__main__':
    main()
