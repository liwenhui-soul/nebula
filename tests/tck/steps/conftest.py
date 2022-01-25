# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import json
import os
import pytest
import random
import shutil
import time

from pytest_bdd import (
    given,
    when,
    then,
    parsers
)

from nebula3.common.ttypes import Value, NullType
from tests.common.utils import get_conn_pool
from tests.tck.utils.nbv import register_function, parse
from tests.tck.utils.audit import (
    NEBULA_ADDRESS,
    AUDIT_LOG_DEFAULT_PARAMS,
    restart_nebula_with_new_config,
    parse_to_xmls,
    parse_to_jsons,
    parse_to_csvs,
    run_command)

# You could register functions that can be invoked from the parsing text
register_function('len', len)


@given("a string: <format>", target_fixture="string_table")
def string_table(format):
    return dict(text=format)


@when('They are parsed as Nebula Value')
def parsed_as_values(string_table):
    cell = string_table['text']
    v = parse(cell)
    assert v is not None, f"Failed to parse `{cell}'"
    string_table['value'] = v


@then('The type of the parsed value should be <_type>')
def parsed_as_expected(string_table, _type):
    val = string_table['value']
    type = val.getType()
    if type == 0:
        actual = 'EMPTY'
    elif type == 1:
        null = val.get_nVal()
        if null == 0:
            actual = 'NULL'
        else:
            actual = NullType._VALUES_TO_NAMES[val.get_nVal()]
    else:
        actual = Value.thrift_spec[val.getType()][2]
    assert actual == _type, f"expected: {_type}, actual: {actual}"


####################### audit log begin ##########################

@pytest.fixture()
def context_holder():
    return dict()


@given("audit_log_format is assigned 'xml' and restart nebula")
def enable_xml_format(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder['graphd_ports'] = graphd_ports['ports']


@when("connect to any graphd")
def connect_any_graphd(context_holder):
    address = NEBULA_ADDRESS["address"]
    if len(context_holder['graphd_ports']) <= 1:
        selected_graphd = 0
    else:
        selected_graphd = random.randint(
            0, len(context_holder['graphd_ports']) - 1)
    pool = get_conn_pool(
        address, context_holder['graphd_ports'][selected_graphd], None)
    sess = pool.get_session('root', 'nebula')
    sess.release()
    time.sleep(1)

    context_holder["selected_graphd"] = selected_graphd


@then("nebula will record the connection information to an xml format file")
def record_connect_information_with_xml(context_holder):
    xmls = parse_to_xmls('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))
    assert len(xmls) == 2

    assert xmls[0].attrib.__contains__('CATEGORY')
    assert xmls[0].attrib.__contains__('TIMESTAMP')
    assert xmls[0].attrib.__contains__('TERMINAL')
    assert xmls[0].attrib.__contains__('CONNECTION_ID')
    assert xmls[0].attrib.__contains__('CONNECTION_STATUS')
    assert xmls[0].attrib.__contains__('CONNECTION_MESSAGE')
    assert xmls[0].attrib.__contains__('USER')
    assert xmls[0].attrib.__contains__('CLIENT_HOST')
    assert xmls[0].attrib.__contains__('HOST')
    assert xmls[0].attrib.__contains__('SPACE')
    assert xmls[0].attrib.__contains__('QUERY')
    assert xmls[0].attrib.__contains__('QUERY_STATUS')
    assert xmls[0].attrib.__contains__('QUERY_MESSAGE')

    assert xmls[0].attrib["CATEGORY"] == 'login'
    assert xmls[0].attrib['CONNECTION_STATUS'] == '0'
    assert xmls[0].attrib['CONNECTION_MESSAGE'] == ''
    assert xmls[0].attrib['SPACE'] == ''
    assert xmls[0].attrib['QUERY'] == ''
    assert xmls[0].attrib['QUERY_STATUS'] == '0'
    assert xmls[0].attrib['QUERY_MESSAGE'] == ''

    assert xmls[1].attrib["CATEGORY"] == 'exit'
    assert xmls[1].attrib["CONNECTION_STATUS"] == '0'
    assert xmls[1].attrib['CONNECTION_MESSAGE'] == ''
    assert xmls[1].attrib['SPACE'] == ''
    assert xmls[1].attrib['QUERY'] == ''
    assert xmls[1].attrib['QUERY_STATUS'] == '0'
    assert xmls[1].attrib['QUERY_MESSAGE'] == ''

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("audit_log_format is assigned 'json' and restart nebula")
def enable_json_format(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_format'] = 'json'
    audit_config['audit_log_categories'] = 'exit'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder['graphd_ports'] = graphd_ports['ports']


@then("nebula will record the connection information to an json format file")
def record_connect_information_with_json(context_holder):
    jsons = parse_to_jsons('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))
    assert len(jsons) == 1

    assert jsons[0].__contains__('category')
    assert jsons[0].__contains__('timestamp')
    assert jsons[0].__contains__('terminal')
    assert jsons[0].__contains__('connection_id')
    assert jsons[0].__contains__('connection_status')
    assert jsons[0].__contains__('connection_message')
    assert jsons[0].__contains__('user')
    assert jsons[0].__contains__('client_host')
    assert jsons[0].__contains__('host')
    assert jsons[0].__contains__('space')
    assert jsons[0].__contains__('query')
    assert jsons[0].__contains__('query_status')
    assert jsons[0].__contains__('query_message')

    assert jsons[0]['category'] == 'exit'
    assert jsons[0]['connection_status'] == '0'
    assert jsons[0]['connection_message'] == ''
    assert jsons[0]['space'] == ''
    assert jsons[0]['query'] == ''
    assert jsons[0]['query_status'] == '0'
    assert jsons[0]['query_message'] == ''

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("audit_log_format is assigned 'csv' and restart nebula")
def enable_csv_format(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_format'] = 'csv'
    audit_config['audit_log_categories'] = 'login'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder['graphd_ports'] = graphd_ports['ports']


@then("nebula will record the connection information to an csv format file")
def record_connect_information_with_csv(context_holder):
    csvs = parse_to_csvs('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))
    assert len(csvs) == 1
    assert len(csvs[0]) == 13
    assert csvs[0][0] == 'login'
    assert csvs[0][4] == '0'
    assert csvs[0][5] == ''
    assert csvs[0][9] == ''
    assert csvs[0][10] == ''
    assert csvs[0][11] == '0'

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("Disable audit log")
def disable_audit(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder['graphd_ports'] = graphd_ports['ports']


@then("nebula will not record any connection information")
def record_nothing(context_holder):
    file = '{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"])
    assert os.path.exists(file) == False

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("elasticsearch address")
def es_authorization(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'es'
    audit_config['audit_log_es_address'] = 'localhost:9200'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['audit_log_es_address'] = audit_config['audit_log_es_address']
    context_holder['graphd_ports'] = graphd_ports['ports']


@then("nebula will record the connection information to es")
def record_information_to_es(context_holder):
    cmd = '''curl -XPOST -sS -H "Content-Type: application/json; charset=utf-8 " \
    http://{}/audit_log_nebula/_search -d \'{{"query":{{"match_all":{{}}}}}}\''''.format(
        context_holder['audit_log_es_address'])

    time.sleep(10)  # May not be written successfully

    result = run_command(cmd)
    json_res = ""
    try:
        json_res = json.loads(result)
    except OSError as err:
        print("failed to connect to es, err={}".format(str(err)))

    assert len(json_res['hits']['hits']) == 2

    assert json_res['hits']['hits'][0]['_source']['category'] == 'login'
    assert json_res['hits']['hits'][0]['_source']['connection_status'] == '0'
    assert json_res['hits']['hits'][0]['_source']['connection_message'] == ''
    assert json_res['hits']['hits'][0]['_source']['space'] == ''
    assert json_res['hits']['hits'][0]['_source']['query'] == ''
    assert json_res['hits']['hits'][0]['_source']['query_status'] == '0'
    assert json_res['hits']['hits'][0]['_source']['query_message'] == ''

    assert json_res['hits']['hits'][1]['_source']['category'] == 'exit'
    assert json_res['hits']['hits'][1]['_source']['connection_status'] == '0'
    assert json_res['hits']['hits'][1]['_source']['connection_message'] == ''
    assert json_res['hits']['hits'][1]['_source']['space'] == ''
    assert json_res['hits']['hits'][1]['_source']['query'] == ''
    assert json_res['hits']['hits'][1]['_source']['query_status'] == '0'
    assert json_res['hits']['hits'][1]['_source']['query_message'] == ''

    # clean env
    cmd = '''curl -XDELETE -H "Content-Type: application/json; charset=utf-8" \
    http://{}/audit_log_nebula'''.format(context_holder['audit_log_es_address'])
    run_command(cmd)
    time.sleep(10)
    context_holder.clear()


@given("elasticsearch address, but wrong user and password")
def wrong_es_authorization(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'es'
    audit_config['audit_log_es_address'] = 'localhost:9200'
    audit_config['audit_log_es_user'] = 'nebula'
    audit_config['audit_log_es_password'] = 'es'
    context_holder['audit_log_es_address'] = audit_config['audit_log_es_address']
    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)
    context_holder['graphd_ports'] = graphd_ports['ports']


@then("nebula will not record any connection information to es")
def record_nothing_to_es(context_holder):
    cmd = '''curl -XPOST -sS -H "Content-Type: application/json; charset=utf-8 " \
    http://{}/audit_log_nebula/_search -d \'{{"query":{{"match_all":{{}}}}}}\''''.format(
        context_holder['audit_log_es_address'])

    time.sleep(10)  # May not be written successfully

    result = run_command(cmd)
    json_res = ""
    try:
        json_res = json.loads(result)
    except OSError as err:
        print("failed to connect to es, err={}".format(str(err)))

    assert json_res.__contains__("error")
    assert json_res.__contains__("status")
    assert json_res["status"] == 401

    # clean env
    context_holder.clear()


@given("audit_log_exclude_spaces is assigned with some spaces")
def exclude_spaces_test(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_exclude_spaces'] = 's1,s2'
    audit_config['audit_log_categories'] = 'dql'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)
    if len(graphd_ports['ports']) <= 1:
        selected_graphd = 0
    else:
        selected_graphd = random.randint(0, len(graphd_ports['ports']) - 1)

    address = NEBULA_ADDRESS["address"]
    pool = get_conn_pool(address, graphd_ports['ports'][selected_graphd], None)
    sess = pool.get_session('root', 'nebula')

    resp = sess.execute(
        "create space s1(partition_num=15, replica_factor=1, vid_type=INT)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute(
        "create space s2(partition_num=15, replica_factor=1, vid_type=INT)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute(
        "create space s3(partition_num=15, replica_factor=1, vid_type=INT)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    context_holder["session"] = sess
    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder["selected_graphd"] = selected_graphd


@when("perform some operations(A) within thoese spaces")
def do_operations_within_exclude_spaces(context_holder):
    sess = context_holder["session"]
    resp = sess.execute("use s1")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("return 's1'")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("use s2")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("return 's2'")
    assert resp.is_succeeded(), resp.error_msg()


@when("perform some operations(B) without thoese spaces")
def do_operations_without_exclude_spaces(context_holder):
    sess = context_holder["session"]
    resp = sess.execute("use s3")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("return 's3'")
    assert resp.is_succeeded(), resp.error_msg()

    time.sleep(1)


@then("nebula only record B operations")
def record_specific_information(context_holder):
    xmls = parse_to_xmls('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))
    assert len(xmls) == 1
    assert xmls[0].attrib["CATEGORY"] == "dql"
    assert xmls[0].attrib["CONNECTION_STATUS"] == "0"
    assert xmls[0].attrib["CONNECTION_MESSAGE"] == ""
    assert xmls[0].attrib["SPACE"] == "s3"
    assert xmls[0].attrib["QUERY"] == "return 's3'"
    assert xmls[0].attrib["QUERY_STATUS"] == "0"
    assert xmls[0].attrib["QUERY_MESSAGE"] == ""

    # clear env
    sess = context_holder["session"]
    resp = sess.execute("drop space s1")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("drop space s2")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("drop space s3")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    sess.release()
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("audit_log_categories is assigned with some categories")
def log_categories_test(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_categories'] = 'dql'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    address = NEBULA_ADDRESS["address"]
    if len(graphd_ports['ports']) <= 1:
        selected_graphd = 0
    else:
        selected_graphd = random.randint(0, len(graphd_ports['ports']) - 1)
    pool = get_conn_pool(address, graphd_ports['ports'][selected_graphd], None)
    sess = pool.get_session('root', 'nebula')

    context_holder['session'] = sess
    context_holder['audit_log_file'] = audit_config['audit_log_file']
    context_holder["selected_graphd"] = selected_graphd


@when("perform some operations(A) within thoese categories")
def do_operations_within_categories(context_holder):
    sess = context_holder["session"]

    resp = sess.execute("return 's1'")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("return 's2'")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("return 's3'")
    assert resp.is_succeeded(), resp.error_msg()


@when("perform some operations(B) without thoese categories")
def do_operations_without_categories(context_holder):
    sess = context_holder["session"]
    resp = sess.execute(
        "create space zzz(partition_num=1, replica_factor=1, vid_type=INT)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("use zzz")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("create tag t1(age int)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("insert vertex t1(age) values 10:(3)")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("drop tag t1")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("drop space zzz")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("create user u1 with password 'nebula'")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("drop user u1")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("show spaces")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("show spacess")
    # assert resp.is_succeeded(), resp.error_msg()

    sess.release()


@then("nebula only record A operations")
def record_specific_categories_information(context_holder):
    xmls = parse_to_xmls('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))

    assert len(xmls) == 3
    assert xmls[0].attrib["CATEGORY"] == 'dql'
    assert xmls[0].attrib["QUERY"] == "return 's1'"
    assert xmls[1].attrib["CATEGORY"] == 'dql'
    assert xmls[1].attrib["QUERY"] == "return 's2'"
    assert xmls[2].attrib["CATEGORY"] == 'dql'
    assert xmls[2].attrib["QUERY"] == "return 's3'"

    # clear env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given("audit_log_categories is assigned with all categories")
def log_categories_test2(context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_categories'] = 'login,exit,ddl,dql,dml,dcl,util,unknown'

    graphd_ports = {}
    restart_nebula_with_new_config(audit_config, graphd_ports)

    context_holder['graphd_ports'] = graphd_ports['ports']
    context_holder['audit_log_file'] = audit_config['audit_log_file']


@when("perform some operations within thoese categories")
def do_operations(context_holder):
    address = NEBULA_ADDRESS["address"]
    if len(context_holder['graphd_ports']) <= 1:
        selected_graphd = 0
    else:
        selected_graphd = random.randint(
            0, len(context_holder['graphd_ports']) - 1)
    context_holder["selected_graphd"] = selected_graphd

    pool = get_conn_pool(
        address, context_holder['graphd_ports'][selected_graphd], None)
    sess = pool.get_session('root', 'nebula')

    resp = sess.execute(
        "create space zzz(partition_num=1, replica_factor=1, vid_type=INT)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("use zzz")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("create tag t1(name string, age int)")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute(
        "insert vertex t1(name, age) values 10:('nebula', 3)")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("drop tag t1")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("drop space zzz")
    assert resp.is_succeeded(), resp.error_msg()
    time.sleep(2)

    resp = sess.execute("return 1")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("create user u1 with password 'nebula'")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("drop user u1")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("show spaces")
    assert resp.is_succeeded(), resp.error_msg()

    resp = sess.execute("show spacess")
    # assert resp.is_succeeded(), resp.error_msg()

    sess.release()
    time.sleep(1)


@then("nebula will record all operations")
def record_every_information(context_holder):
    xmls = parse_to_xmls('{}.{}'.format(
        context_holder['audit_log_file'], context_holder["selected_graphd"]))
    assert len(xmls) == 13
    assert xmls[0].attrib["CATEGORY"] == 'login'
    assert xmls[0].attrib["QUERY"] == ''

    assert xmls[1].attrib["CATEGORY"] == 'ddl'
    assert xmls[1].attrib["QUERY"] == "create space zzz(partition_num=1, replica_factor=1, vid_type=INT)"

    assert xmls[2].attrib["CATEGORY"] == 'util'
    assert xmls[2].attrib["QUERY"] == "use zzz"

    assert xmls[3].attrib["CATEGORY"] == 'ddl'
    assert xmls[3].attrib["QUERY"] == "create tag t1(name string, age int)"

    assert xmls[4].attrib["CATEGORY"] == 'dml'
    assert xmls[4].attrib["QUERY"] == "insert vertex t1(name, age) values 10:('nebula', 3)"

    assert xmls[5].attrib["CATEGORY"] == 'ddl'
    assert xmls[5].attrib["QUERY"] == "drop tag t1"

    assert xmls[6].attrib["CATEGORY"] == 'ddl'
    assert xmls[6].attrib["QUERY"] == "drop space zzz"

    assert xmls[7].attrib["CATEGORY"] == 'dql'
    assert xmls[7].attrib["QUERY"] == "return 1"

    assert xmls[8].attrib["CATEGORY"] == 'dcl'
    assert xmls[8].attrib["QUERY"] == "create user u1 with password 'nebula'"

    assert xmls[9].attrib["CATEGORY"] == 'dcl'
    assert xmls[9].attrib["QUERY"] == "drop user u1"

    assert xmls[10].attrib["CATEGORY"] == 'util'
    assert xmls[10].attrib["QUERY"] == "show spaces"

    assert xmls[11].attrib["CATEGORY"] == 'unknown'
    assert xmls[11].attrib["QUERY"] == "show spacess"

    assert xmls[12].attrib["CATEGORY"] == 'exit'
    assert xmls[12].attrib["QUERY"] == ""

    # clear env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@given(parsers.parse("enable_audit is assigned an normal parameter: <param>"))
@given(parsers.parse('enable_audit is assigned an exception parameter: <param>'))
def enable_audit_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_handler is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_handler is assigned an exception parameter: <param>"))
def audit_log_handler_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = param
    audit_config['audit_log_es_address'] = "localhost:9200"
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_file is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_file is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'file'
    audit_config['audit_log_file'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_strategy is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_strategy is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'file'
    audit_config['audit_log_strategy'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_max_buffer_size is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_max_buffer_size is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'file'
    audit_config['audit_log_strategy'] = 'asynchronous'
    audit_config['audit_log_max_buffer_size'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_format is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_format is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'file'
    audit_config['audit_log_strategy'] = 'asynchronous'
    audit_config['audit_log_format'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_es_address is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_es_address is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_handler'] = 'es'
    audit_config['audit_log_es_address'] = param
    context_holder.update(audit_config)


@given(parsers.parse("audit_log_categories is assigned an normal parameter: <param>"))
@given(parsers.parse("audit_log_categories is assigned an exception parameter: <param>"))
def audit_log_file_param(param, context_holder):
    audit_config = {}
    audit_config.update(AUDIT_LOG_DEFAULT_PARAMS)
    audit_config['enable_audit'] = 'true'
    audit_config['audit_log_categories'] = param
    context_holder.update(audit_config)


@when("restart nebula")
def restart_nebula(context_holder):
    graphd_ports = {}
    context_holder["result"] = restart_nebula_with_new_config(
        context_holder, graphd_ports)


@then("nebula will restart successfully")
def restart_successfully(context_holder):
    assert context_holder["result"] == True

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()


@then("nebula will fail to restart")
def failed_to_restart(context_holder):
    assert context_holder["result"] == False

    # clean env
    shutil.rmtree(os.path.dirname(context_holder['audit_log_file']),
                  ignore_errors=True)
    context_holder.clear()

####################### audit log end ##########################
