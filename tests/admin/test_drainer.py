# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestListener(NebulaTestSuite):
    def test_drainer(self):
        resp = self.client.execute('SHOW HOSTS')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        storage_ip = resp.row_values(0)[0].as_string()
        storage_port = resp.row_values(0)[1].as_int()

        self.use_nba()

        # Add on same as storage host
        resp = self.client.execute('ADD DRAINER {}:{}'.format(storage_ip, storage_port))
        self.check_resp_failed(resp)

        # Add nonexistent host，Because it's not a real drainer machine
        resp = self.client.execute('ADD DRAINER 127.0.0.1:8899')
        self.check_resp_failed(resp)

        # CHECK drainer
        resp = self.client.execute('SHOW DRAINERS;')
        self.check_resp_succeeded(resp)
        self.check_result(resp, [])

        # Remove drainer, drainer not exists
        resp = self.client.execute('REMOVE DRAINER;')
        self.check_resp_failed(resp)

        # Get space variable
        resp = self.client.execute('GET VARIABLES read_only;')
        self.check_resp_succeeded(resp)

        # Set space variable
        resp = self.client.execute('SET VARIABLES read_only=true;')
        self.check_resp_succeeded(resp)
