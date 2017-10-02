
import json
import mock
import subprocess

from nose.tools import *
from mockproc import mockprocess
#from testfixtures import Replacer, ShouldRaise, compare
#from testfixtures.popen import MockPopen

from tornado.testing import *
from tornado.web import Application
from tornado.escape import json_encode

from arteria.web.state import State
from arteria.exceptions import ArteriaUsageException

from dsmc.app import routes
from dsmc import __version__ as dsmc_version
from dsmc.handlers.dsmc_handlers import VersionHandler, UploadHandler, StatusHandler, ReuploadHandler, CreateDirHandler, GenChecksumsHandler, ReuploadHelper, BaseDsmcHandler
from dsmc.lib.jobrunner import LocalQAdapter
from tests.test_utils import DummyConfig

# TODO: Uploadhandler is not correct tested yet. 

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

    # TODO: How to check the randomly generated file?  Clean it, and then check that one file has been created? 
    # TODO: And how to check when we're running with the real dsmc? Stub it somehow? 
    #self.assertEqual(json_resp["dsmc_log"], )
    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.start", autospec=True)
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


    @mock.patch("dsmc.handlers.dsmc_handlers.BaseDsmcHandler._is_valid_log_dir", autospec=True)
    def test_raise_exception_on_log_dir_problem(self, mock__is_valid_log_dir):
        mock__is_valid_log_dir.return_value = False
        response = self.fetch(self.API_BASE + "/upload/test_archive", method="POST", allow_nonstandard_methods=True)

        self.assertEqual(response.code, 500)

    # TODO: Need to test our modifications of the LocalQAdaptor as well. 
    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.status", autospec=True)
    def test_check_status(self, mock_status):
        mock_status.return_value = State.DONE
        response = self.fetch(self.API_BASE + "/status/1")
        json_resp = json.loads(response.body)
        self.assertEqual(json_resp["state"], State.DONE)
        mock_status.assert_called_with(self.runner_service, "1")

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
        body = {"remove": "False"} 
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
       # shutil.rmtree(archive_path)
        
    @mock.patch("dsmc.lib.jobrunner.LocalQAdapter.start", autospec=True)
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
        mock_start.assert_called_with(self.runner_service, expected_cmd, run_dir=os.path.abspath(self.dummy_config["path_to_archive_root"]), nbr_of_cores=1, stderr=checksum_log, stdout=checksum_log)
        #TODO: Check the existance of the md5sumfile. 
    
    def test_reupload_handler(self):
        job_id = 27
      
        with \
            mock.patch \
                ("dsmc.handlers.dsmc_handlers.ReuploadHelper.get_pdc_descr",\
                autospec=True) as mock_get_pdc_descr, \
            mock.patch \
                ("dsmc.handlers.dsmc_handlers.ReuploadHelper.get_pdc_filelist",\
                autospec=True) as mock_get_pdc_filelist, \
            mock.patch \
                ("dsmc.handlers.dsmc_handlers.ReuploadHelper.get_local_filelist",\
                autospec=True) as mock_get_local_filelist, \
            mock.patch \
                ("dsmc.handlers.dsmc_handlers.ReuploadHelper.get_files_to_reupload",\
                autospec=True) as mock_get_files_to_reupload, \
            mock.patch("dsmc.handlers.dsmc_handlers.ReuploadHelper.reupload",\
                autospec=True) as mock_reupload:

            mock_get_pdc_descr.return_value = "abc123"
            mock_get_pdc_filelist.return_value = "{'foo': 123}"
            mock_get_local_filelist.return_value = "{'foo': 123, 'bar': 456}"
            mock_get_files_to_reupload.return_value = "{'bar': 456}"
            mock_reupload.return_value = job_id
            
            resp = self.fetch(self.API_BASE + "/reupload/test_archive", method="POST", 
            allow_nonstandard_methods=True)
        
        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.STARTED)
        self.assertEqual(json_resp["job_id"], job_id)

    # Successful test
    # TODO: Write some failing tests 
    def test_get_pdc_descr(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=0,
                            script="""#!/bin/bash
cat tests/resources/dsmc_output/dsmc_descr.txt
""")

        with self.scripts:
            descr = helper.get_pdc_descr("/data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive")

        self.assertEqual(descr, "e374bd6b-ab36-4f41-94d3-f4eaea9f30d4")

    def test_get_pdc_filelist(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=0, 
                            script="""#!/bin/bash
cat tests/resources/dsmc_output/dsmc_pdc_filelist.txt
""")

        with self.scripts:
            filelist = helper.get_pdc_filelist("/data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive", "e374bd6b-ab36-4f41-94d3-f4eaea9f30d4")

        with open("tests/resources/dsmc_output/dsmc_pdc_converted_filelist.txt") as f: 
            nr_of_files = 0
            for line in f:
                size, name = line.split()
                self.assertEqual(int(filelist[name]), int(size))
                nr_of_files += 1

            self.assertEqual(len(filelist.keys()), nr_of_files)

    def test_get_local_filelist(self):
        helper = ReuploadHelper()
        path = "tests/resources/archives/archive_from_pdc"

        cmd = "find {} -type f -exec du -b {{}} \;".format(path)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 
        du_out, du_err = p.communicate()
        du_out = du_out.splitlines()

        files = helper.get_local_filelist(path)

        for line in du_out:
            size, filename = line.split()
            path = os.path.join(path, filename)

            self.assertEqual(files[filename], size)

        self.assertEqual(len(files.keys()), len(du_out))
  
    def test_get_files_to_reupload(self): 
        helper = ReuploadHelper()

        local_files = {"foo": 23, "bar": 46}
        uploaded_files = {"foo": 23}
        expected = ["bar"]
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

        uploaded_files = {"foo": 44}
        expected = ["foo", "bar"]
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

        local_files = {"foo": 44}
        uploaded_files = {"foo": 44}
        expected = []
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

    def test_reupload(self):
        helper = ReuploadHelper()
        uniq_id = "test"
        dsmc_log_file = "foolog"
        descr = "foodescr"
        run_dir = "foodir"

        exp_id = 72

        class MyRunner(object):
            def start(self, cmd, nbr_of_cores, run_dir, stdout=dsmc_log_file, stderr=dsmc_log_file): 
                self.components = cmd.split("=")
                return exp_id

        runsrv = MyRunner()

        local_files = {"foo": 23, "bar": 46, "uggla": 72}
        uploaded_files = {"foo": 23, "uggla": 15}
        exp_upload = ['"bar"\n', '"uggla"\n']
        reupload_files = helper.get_files_to_reupload(local_files, uploaded_files)

        res_id = helper.reupload(reupload_files, descr, uniq_id, run_dir, dsmc_log_file, runsrv)

        self.assertEqual(res_id, exp_id)
        self.assertEqual(runsrv.components[-1], descr)

        path = runsrv.components[1].split()[0]
        with open(path) as f:
            uploaded = f.readlines()

        import sets
        uploaded = sets.Set(uploaded)
        exp_upload = sets.Set(exp_upload)
        self.assertEqual(len(uploaded.symmetric_difference(exp_upload)), 0)

    def test_compress_archive_full(self): 
        import shutil
        root = self.dummy_config["path_to_archive_root"]
        archive_path = os.path.join(root, "johanhe_test_archive")

        shutil.copytree(os.path.join(root, "johanhe_test_runfolder"), archive_path)

        resp = self.fetch(self.API_BASE + "/compress_archive/johanhe_test_archive", method="POST", 
                          allow_nonstandard_methods=True)
                
        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.DONE)
        
        self.assertFalse(os.path.exists(os.path.join(archive_path, "RunInfo.xml")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "Config")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "johanhe_test_archive.tar.gz")))

        shutil.rmtree(archive_path)

    def test_compress_archive_mini(self): 
        import shutil
        root = self.dummy_config["path_to_archive_root"]
        archive_path = os.path.join(root, "testrunfolder_archive_tmp")

        shutil.copytree(os.path.join(root, "testrunfolder_archive"), archive_path)

        resp = self.fetch(self.API_BASE + "/compress_archive/testrunfolder_archive_tmp", method="POST", 
                          allow_nonstandard_methods=True)
                
        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.DONE)
        
        self.assertTrue(os.path.exists(os.path.join(archive_path, "file.csv")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "file.bin")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory2")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "testrunfolder_archive_tmp.tar.gz")))    

        shutil.rmtree(archive_path)    