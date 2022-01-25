# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import glob
import json
import os
import pickle
import subprocess
import time
import xml.etree.ElementTree as ET

from tests.common.nebula_service import NebulaService
from tests.common.constants import NB_TMP_PATH, NB_SERVICE_OBJ

NEBULA_SERVICE=""
NEBULA_ADDRESS={}
AUDIT_LOG_DEFAULT_PARAMS = {
    'enable_audit' : 'false',
    'audit_log_handler' : 'file',
    'audit_log_file' : './logs/audit/audit.log',
    'audit_log_strategy' : 'synchronous',
    'audit_log_max_buffer_size' : '1048576',
    'audit_log_format' : 'xml',
    'audit_log_es_address' : '',
    'audit_log_es_user' : '',
    'audit_log_es_password' : '',
    'audit_log_es_batch_size' : '1000',
    'audit_log_exclude_spaces' : '',
    'audit_log_categories' : 'login,exit'
}

def prepare_audit_env():
    with open(NB_TMP_PATH, "r") as f:
        data = json.loads(f.readline())
        is_remote = data["is_remote"]
        if is_remote:
            return False
        global NEBULA_ADDRESS
        NEBULA_ADDRESS["address"] = data["ip"]

    global NEBULA_SERVICE
    with open(NB_SERVICE_OBJ, 'rb') as f:
        NEBULA_SERVICE = pickle.load(f)

    return True

def clean_audit_env():
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

def restart_nebula_with_new_config(audit_config, graphd_ports):
    NEBULA_SERVICE.stop(cleanup=False, remove_ports=False)
    NEBULA_SERVICE.update_graphd_param(audit_config)
    try:
        ports = NEBULA_SERVICE.start(add_hosts=False, no_throw=True)
        graphd_ports["ports"] = ports
        return NEBULA_SERVICE.is_all_procs_alive()
    except OSError as err:
        print("failed to start nebula: {}".format(str(err)))
        return False

def parse_to_xmls(file_path):
    xmls = []
    with open(file_path) as file:
        lines = file.readlines()
        j = ""
        for i in range(len(lines)):
           # A log has 15 lines
           if (i != 0 and i % 15 == 0):
             xmls.append(ET.fromstring(j))
             j = ""
             j  = j + lines[i]
           else:
             j  = j + lines[i]
        if j != "":
            xmls.append(ET.fromstring(j))
    return xmls

def parse_to_jsons(file_path):
    jsons = []
    with open(file_path) as file:
        lines = file.readlines()
        j = ""
        for i in range(len(lines)):
           # A log has 15 lines
           if (i != 0 and i % 15 == 0):
             jsons.append(json.loads(j))
             j = ""
             j  = j + lines[i]
           else:
             j  = j + lines[i]
        if j != "":
            jsons.append(json.loads(j))
    return jsons

def parse_to_csvs(file_path):
    csvs = []
    with open(file_path) as file:
        lines = file.readlines()
        for line in lines:
            csvs.append(line.split(','))
    return csvs

def run_command(command):
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = process.communicate()
    return result[0]
