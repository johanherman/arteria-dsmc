
import json
import mock
from nose.tools import *
#import pytest

from tornado.testing import *
from tornado.web import Application
from tornado.escape import json_encode

import arteria

from arteria.web.state import State
from arteria.exceptions import ArteriaUsageException

import dsmc
from dsmc.app import routes
from dsmc import __version__ as dsmc_version
from dsmc.handlers.dsmc_handlers import VersionHandler, UploadHandler, StatusHandler, ReuploadHandler, CreateDirHandler, GenChecksumsHandler
from dsmc.lib.jobrunner import LocalQAdapter
from tests.test_utils import DummyConfig

"""

"""
class TestDsmcHandlers(AsyncHTTPTestCase):

    API_BASE="/api/1.0"
    
    dummy_config = DummyConfig()

    runner_service = LocalQAdapter(nbr_of_cores=2, whitelisted_warnings = dummy_config["whitelisted_warnings"], interval = 2, priority_method = "fifo")

    def get_app(self):
        return Application(
            routes(
                config=self.dummy_config,
                runner_service=self.runner_service))

    ##ok_runfolder = "tests/resources/ok_checksums"

    def test_version(self):
        response = self.fetch(self.API_BASE + "/version")

        expected_result = { "version": dsmc_version }

        self.assertEqual(response.code, 200)
        self.assertEqual(json.loads(response.body), expected_result)    


    def test__validate_runfolder_exists_ok(self):
        is_valid = UploadHandler._validate_runfolder_exists("testrunfolder", self.dummy_config["monitored_directory"])
        self.assertTrue(is_valid)

    def test__validate_runfolder_exists_not_ok(self):
        not_valid = UploadHandler._validate_runfolder_exists("non-existant", self.dummy_config["monitored_directory"])
        self.assertFalse(not_valid)

    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.start")
    def test_start_upload(self, mock_start):
        job_id = 24
        mock_start.return_value = job_id

        #body = {"path_to_md5_sum_file": "md5_checksums"}
        # Set monitored_dir to test/resources
        response = self.fetch(self.API_BASE + "/upload/test_archive", method="POST", allow_nonstandard_methods=True)#,
            #body=json_encode(body))

        json_resp = json.loads(response.body)
        #job_id = 1

        self.assertEqual(response.code, 202)
        self.assertEqual(json_resp["job_id"], job_id)
        self.assertEqual(json_resp["service_version"], dsmc_version)

        expected_link = "http://localhost:{0}/api/1.0/status/".format(self.get_http_port())
        self.assertTrue(expected_link in json_resp["link"])
        self.assertEqual(json_resp["state"], State.STARTED)
        # TODO: How to check the randomly generated file?  Clean it, and then check that one file has been created? 
        # TODO: And how to check when we're running with the real dsmc? Stub it somehow? 
        #self.assertEqual(json_resp["dsmc_log"], )

    """
    def test_start_checksum_with_shell_injection(self):
        body = {"path_to_md5_sum_file": "tests/$(cat /etc/shadow)"}
        response = self.fetch(
            self.API_BASE + "/start/ok_checksums",
            method="POST",
            body=json_encode(body))

        self.assertEqual(response.code, 500)
    """

    @mock.patch("dsmc.handlers.dsmc_handlers.UploadHandler._is_valid_log_dir")
    def test_raise_exception_on_log_dir_problem(self, mock__is_valid_log_dir):
        mock__is_valid_log_dir.return_value = False
        response = self.fetch(self.API_BASE + "/upload/test_archive", method="POST", allow_nonstandard_methods=True)

        self.assertEqual(response.code, 500)

    # TODO: Need to test our modifications of the LocalQAdaptor as well. 
    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.status")
    def test_check_status(self, mock_status):
        mock_status.return_value = State.DONE
        response = self.fetch(self.API_BASE + "/status/1")
        json_resp = json.loads(response.body)
        self.assertEqual(json_resp["state"], State.DONE)
        mock_status.assert_called_with("1")

    """
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
    """

    
    # TODO: Need also to test: 
    # - ReuploadHandler, , GenChecksumHandler

    def test_create_dir(self): 
        archive_path = "./tests/resources/archives/testrunfolder_archive/"

        # Base case
        body = {"remove": "True", "exclude_dirs": "['directory1', 'directory3', 'directory4']", "exclude_extensions": "['.txt', '.bar']"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        first_created_at = os.path.getctime(archive_path)

        self.assertEqual(json_resp["state"], State.DONE)
        self.assertTrue(os.path.exists(archive_path))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "directory1")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "directory2", "file.bar")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory2", "file.bin")))

        # Should fail due to folder already existing
        body = {"remove": "False", "exclude_dirs": "['foo', 'bar']", "exclude_extensions": "['.txt', '.bar']"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        self.assertEqual(json_resp["state"], State.ERROR)

        # Check that the dir is recreated
        os.mkdir(os.path.join(archive_path, "remove-me"))

        body = {"remove": "True", "exclude_dirs": "['foo', 'bar']", "exclude_extensions": "['.txt', '.bar']"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        self.assertEqual(json_resp["state"], State.DONE)
        second_created_at = os.path.getctime(archive_path)
        self.assertTrue(first_created_at < second_created_at)
        self.assertFalse(os.path.exists(os.path.join(archive_path, "remove-me")))
    
        import shutil
        shutil.rmtree(archive_path)
        
    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.start")
    def test_generate_checksum(self, mock_start): 
        job_id = 42
        mock_start.return_value = job_id

        path_to_archive = os.path.abspath(os.path.join(self.dummy_config["path_to_archive_root"], "test_archive"))
        checksum_log = os.path.abspath(os.path.join(self.dummy_config["dsmc_log_directory"], "checksum.log"))
        filename = "checksums_prior_to_pdc.md5"

        response = self.fetch(self.API_BASE + "/gen_checksums/test_archive", method="POST", allow_nonstandard_methods=True) #body=json_encode(body))
        json_resp = json.loads(response.body)

        expected_cmd = "cd {} && /usr/bin/find -L . -type f ! -path '{}' -exec /usr/bin/md5sum {{}} + > {}".format(path_to_archive, filename, filename)

        self.assertEqual(json_resp["state"], State.STARTED)
        self.assertEqual(json_resp["job_id"], job_id)
        mock_start.assert_called_with(expected_cmd, run_dir=os.path.abspath(self.dummy_config["path_to_archive_root"]), nbr_of_cores=1, stderr=checksum_log, stdout=checksum_log)
    
    def test_reupload(self): 
        pass





