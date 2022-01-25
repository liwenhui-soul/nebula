# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

from pytest_bdd import scenarios
from tests.tck.utils.audit import prepare_audit_env, clean_audit_env

if prepare_audit_env():
    scenarios('audit')
    clean_audit_env()
