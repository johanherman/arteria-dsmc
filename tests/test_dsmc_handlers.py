
import json
import mock

from tornado.testing import *
from tornado.web import Application
from tornado.escape import json_encode

from arteria.web.state import State

from dsmc.app import routes
from dsmc import __version__ as checksum_version
from dsmc.handlers.checksum_handlers import StartHandler
from dsmc.lib.jobrunner import LocalQAdapter
from tests.test_utils import DummyConfig

"""

"""
class TestDsmcHandlers(AsyncHTTPTestCase):

    API_BASE="/api/1.0"

    runner_service = LocalQAdapter(nbr_of_cores=2, interval = 2, priority_method = "fifo")

    def get_app(self):
        return Application(
            routes(
                config=DummyConfig(),
                runner_service=self.runner_service))

    ok_runfolder = "tests/resources/ok_checksums"


    def test__validate_runfolder_exists_ok(self):
        is_valid = StartHandler._validate_runfolder_exists("ok_checksums", "tests/resources/")
        self.assertTrue(is_valid)

    def test__validate_runfolder_exists_not_ok(self):
        not_valid = StartHandler._validate_runfolder_exists("invalid_checksums", "tests/resources/")
        self.assertFalse(not_valid)

    def test__validate_md5sum_path_ok(self):
        valid = StartHandler._validate_md5sum_path(runfolder=TestChecksumHandlers.ok_runfolder,
                                                   md5sum_file_path=os.path.join(TestChecksumHandlers.ok_runfolder,
                                                                                 "md5_checksums"))
        self.assertTrue(valid)

    def test__validate_md5sum_nested_path_ok(self):
        nested_runfolder = "tests/resources/ok_nested_dir/"
        valid = StartHandler._validate_md5sum_path(runfolder=nested_runfolder,
                                                   md5sum_file_path=os.path.join(nested_runfolder,
                                                                                 "./md5sums/empty_file"))
        self.assertTrue(valid)

    def test__validate_md5sum_path_not_ok(self):
        not_valid = StartHandler._validate_md5sum_path(runfolder=TestChecksumHandlers.ok_runfolder,
                                                       md5sum_file_path=os.path.join(TestChecksumHandlers.ok_runfolder,
                                                                                     "no_file"))
        self.assertFalse(not_valid)

    def test_start_checksum(self):
        body = {"path_to_md5_sum_file": "md5_checksums"}
        response = self.fetch(
            self.API_BASE + "/start/ok_checksums",
            method="POST",
            body=json_encode(body))

        response_as_json = json.loads(response.body)

        # TODO if we want more tests on the same
        #      server, this needs to become
        #      a global variable.
        job_id = 1

        self.assertEqual(response.code, 202)
        self.assertEqual(response_as_json["job_id"], job_id)
        self.assertEqual(response_as_json["service_version"], checksum_version)

        expected_link = "http://localhost:{0}/api/1.0/status/{1}".format(self.get_http_port(), job_id)
        self.assertEqual(response_as_json["link"], expected_link)
        self.assertEqual(response_as_json["state"], State.STARTED)

    def test_start_checksum_with_shell_injection(self):
        body = {"path_to_md5_sum_file": "tests/$(cat /etc/shadow)"}
        response = self.fetch(
            self.API_BASE + "/start/ok_checksums",
            method="POST",
            body=json_encode(body))

        self.assertEqual(response.code, 500)


    def test_check_status(self):
        with mock.patch("checksum.lib.jobrunner.LocalQAdapter.status", return_value=State.DONE) as m:
            response = self.fetch(self.API_BASE + "/status/1")
            response_as_json = json.loads(response.body)
            self.assertEqual(response_as_json["state"], State.DONE)
            m.assert_called_with("1")

    def test_stop_all_checksum(self):
        with mock.patch("checksum.lib.jobrunner.LocalQAdapter.stop_all") as m:
            response = self.fetch(self.API_BASE + "/stop/all", method="POST", body="")
            self.assertEqual(response.code, 200)
            m.assert_called_with()


    def test_stop_one_checksum(self):
        with mock.patch("checksum.lib.jobrunner.LocalQAdapter.stop") as m:
            response = self.fetch(self.API_BASE + "/stop/1", method="POST", body="")
            self.assertEqual(response.code, 200)
            m.assert_called_with("1")

    def test_version(self):
        response = self.fetch(self.API_BASE + "/version")

        expected_result = { "version": checksum_version }

        self.assertEqual(response.code, 200)
        self.assertEqual(json.loads(response.body), expected_result)

    def test_raise_exception_on_log_dir_problem(self):
        with mock.patch("checksum.handlers.checksum_handlers.StartHandler._is_valid_log_dir", return_value=False):
            body = {"path_to_md5_sum_file": "md5_checksums"}
            response = self.fetch(
                    self.API_BASE + "/start/ok_checksums",
                    method="POST",
                    body=json_encode(body))

            self.assertEqual(response.code, 500)



