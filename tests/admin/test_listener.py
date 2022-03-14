# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestListener(NebulaTestSuite):
    def test_listener(self):
        resp = self.client.execute('SHOW HOSTS')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        storage_ip = resp.row_values(0)[0].as_string()
        storage_port = resp.row_values(0)[1].as_int()

        self.use_nba()

        # Add on same as storage host
        resp = self.client.execute('ADD LISTENER ELASTICSEARCH {}:{}'.format(storage_ip, storage_port))
        self.check_resp_failed(resp)

        # Add nonexistent host
        resp = self.client.execute('ADD LISTENER ELASTICSEARCH 127.0.0.1:8899')
        self.check_resp_succeeded(resp)

        # Show listener
        resp = self.client.execute('SHOW LISTENER;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[1, "ELASTICSEARCH", '"127.0.0.1":8899', "OFFLINE"]])

        # Show listener
        resp = self.client.execute('SHOW LISTENER ELASTICSEARCH;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[1, "ELASTICSEARCH", '"127.0.0.1":8899', "OFFLINE"]])

        # Remove listener
        resp = self.client.execute('REMOVE LISTENER ELASTICSEARCH;')
        self.check_resp_succeeded(resp)

        # CHECK listener
        resp = self.client.execute('SHOW LISTENER ELASTICSEARCH;')
        self.check_resp_succeeded(resp)
        self.check_result(resp, [])

        # Sync listener need sign in drainer service first
        resp = self.client.execute('SIGN IN DRAINER SERVICE (127.0.0.1:7899);')
        self.check_resp_succeeded(resp)

        # CHECK drainer clients
        resp = self.client.execute('SHOW DRAINER CLIENTS;')
        self.check_resp_succeeded(resp)

        # Add on same as storage host
        resp = self.client.execute('ADD LISTENER SYNC {}:{} TO SPACE default_space'.format(storage_ip, storage_port))
        self.check_resp_failed(resp)

        # Add non-existen host
        resp = self.client.execute('ADD LISTENER SYNC META 127.0.0.1:7899 STORAGE 127.0.0.1:7999 TO SPACE default_space')
        self.check_resp_succeeded(resp)

        # Show listener
        resp = self.client.execute('SHOW LISTENER SYNC;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[0, "SYNC", '"127.0.0.1":7899', "default_space", "OFFLINE", "ONLINE"]])
        self.search_result(resp, [[1, "SYNC", '"127.0.0.1":7999', "default_space", "OFFLINE", "ONLINE"]])

        # Stop listener
        resp = self.client.execute('STOP SYNC;')
        self.check_resp_succeeded(resp)

        # Show listener
        resp = self.client.execute('SHOW LISTENER SYNC;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[0, "SYNC", '"127.0.0.1":7899', "default_space", "OFFLINE", "OFFLINE"]])
        self.search_result(resp, [[1, "SYNC", '"127.0.0.1":7999', "default_space", "OFFLINE", "OFFLINE"]])

        # Restart listener
        resp = self.client.execute('RESTART SYNC;')
        self.check_resp_succeeded(resp)

        # Show listener
        resp = self.client.execute('SHOW LISTENER SYNC;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[0, "SYNC", '"127.0.0.1":7899', "default_space", "OFFLINE", "ONLINE"]])
        self.search_result(resp, [[1, "SYNC", '"127.0.0.1":7999', "default_space", "OFFLINE", "ONLINE"]])

        # Remove listener
        resp = self.client.execute('REMOVE LISTENER SYNC;')
        self.check_resp_succeeded(resp)

        # CHECK listener
        resp = self.client.execute('SHOW LISTENER;')
        self.check_resp_succeeded(resp)
        self.check_result(resp, [])

        # Sign out drainer service
        resp = self.client.execute('SIGN OUT DRAINER SERVICE;')
        self.check_resp_succeeded(resp)

        # CHECK drainer clients
        resp = self.client.execute('SHOW DRAINER CLIENTS;')
        self.check_resp_succeeded(resp)
