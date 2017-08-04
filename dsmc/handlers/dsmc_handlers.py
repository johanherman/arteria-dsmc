import json
import logging
import os
import datetime
import uuid
import subprocess
import re 

from arteria.exceptions import ArteriaUsageException
from arteria.web.state import State
from arteria.web.handlers import BaseRestHandler

from dsmc import __version__ as version
from dsmc.lib.jobrunner import LocalQAdapter

log = logging.getLogger(__name__)

class BaseDsmcHandler(BaseRestHandler):
    """
    Base handler for checksum.
    """

    def initialize(self, config, runner_service):
        """
        Ensures that any parameters feed to this are available
        to subclasses.

        :param: config configuration used by the service
        :param: runner_service to use. Must fulfill `dsmc.lib.jobrunner.JobRunnerAdapter` interface

        """
        self.config = config
        self.runner_service = runner_service


class VersionHandler(BaseDsmcHandler):

    """
    Get the version of the service
    """
    def get(self):
        """
        Returns the version of the dsmc-service
        """
        self.write_object({"version": version })


class ReuploadHandler(BaseDsmcHandler):
    # TODO: Refactor out
    @staticmethod
    def _validate_runfolder_exists(runfolder, monitored_dir):
        if os.path.isdir(monitored_dir):
            sub_folders = [ name for name in os.listdir(monitored_dir)
                            if os.path.isdir(os.path.join(monitored_dir, name)) ]
            return runfolder in sub_folders
        else:
            return False

# Step 1b. Handle archive of specific files.
# I.e. so we can re-upload failed uploads, and also (if we want to), upload only
# certain files (this is a second feature though).
#
# Either the service waits and retries until all files have succeeded, or it
# can reuse pdc-descr.sh, pdc-diff.sh and pdc-upload-missing.sh to upload
# at a later time. More robust to re-use the scripts. Just don't fail on the 
# things in section 1c. 
#
# See if I can implement the script logic in the service instead. Because I 
# need to be able to launch it on Irma as well. Otherwise the service will 
# just have to call out to the scripts. 


    # TODO: Check so that we are not making any mistakes with 
    def post(self, runfolder_archive): 
        monitored_dir = self.config["monitored_directory"]

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder_archive, monitored_dir))            

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)
        uniq_id = str(uuid.uuid4())        
        dsmc_log_dir = self.config["dsmc_log_directory"]

        if not UploadHandler._is_valid_log_dir(dsmc_log_dir):
            raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_dir))

        dsmc_log_file = "{}/dsmc_{}_{}-{}".format(dsmc_log_dir,
                                                      runfolder_archive,
                                                      uniq_id,
        #                                              description,
                                                      datetime.datetime.now().isoformat())            


        # Problem here is that we do not want to queue all the DSMC jobs, as that would become messy
        # Because we don't want to respond each time. 
        # I.e. the fetch of descr, and the diff commands we want to execute directly. 
        # Only the last reupload should be done with the queue. 

        # Step 1 - fetch the description of the last uploaded version of this archive

        #dsmc q ar /proj/ngi2016001/incoming/${RUNFOLDER} | grep "/proj/ngi2016001/incoming" | awk '{print $3" "$NF}'


        #cmd = "dsmc q ar {}".format(path_to_runfolder)
        cmd = "cat /tmp/apa/descr-list.txt"
        # TODO: Check that process completed successfully
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # TODO: This can deadlock according to https://docs.python.org/2/library/subprocess.html
        retval = p.wait()

        #Accessing as node: SLLUPNGI_TEST
        #13              Size  Archive Date - Time    File - Expires on - Description
        #14              ----  -------------------    -------------------------------
        #15          4,096  B  01/10/2017 15:16:26    /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive Never #bce2592a-3bcb-4d5d-acbc-3170469ff23a
        #16          4,096  B  01/10/2017 16:09:42    /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive Never #d1b51671-25b8-4bb1-85bf-f9d82082f0ef
        
        #my_string.split('\n')
        my_lines = p.stdout.readlines()
        #my_match = os.path.join(monitored_dir, runfolder_archive)
        log.debug("My lines: {}".format(my_lines))
        #log.debug("My match: {}".format(my_match))
        matched_lines = [line.strip() for line in my_lines if path_to_runfolder in line]
        log.debug("Matched lines: {}".format(matched_lines))
        latest_upload = matched_lines[-1:][0]
        latest_descr = latest_upload.split()[-1:][0]
        log.debug("Last upload: {}".format(latest_upload))
        log.debug("Last descr: {}".format(latest_descr))

        #status_end_point = "{0}://{1}{2}".format(
        #    self.request.protocol,
        #    self.request.host,
        #    self.reverse_url("status", job_id))

        # Step 2 - check the difference of the uploaded version vs the local archive

        # Step 1, get filelist from PDC      
        #cmd = "dsmc q ar {} -subdir=yes -description={}".format(path_to_runfolder, latest_descr)
        cmd = "cat /tmp/apa/runfolder-content.txt"
        # TODO: Check that process completed successfully
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # TODO: This can deadlock according to https://docs.python.org/2/library/subprocess.html
        retval = p.wait()
        my_lines = p.stdout.readlines()
        log.debug("RAW uploaded data: {}".format(my_lines))

        # Take out the bytes and the filename from the output
        # convert raw output to bytes - first field in matched_lines
        #
        # Get the lines containing the path. Then take out the field nr 1 and 5
        matched_lines = [line.strip() for line in my_lines if path_to_runfolder in line]
        log.debug("RAW matched lines: {}".format(matched_lines))

        # FIXME: build up this with dicts instead - makes it easier to compare. 
        # NB uploaded list contains folders as well, but when we check local content
        # we only look at the files, and ignore the folders.
        uploaded_files = {} #[line for line in matched_lines]

        for line in matched_lines: 
            elements = line.split()
            byte_size = elements[0].replace(",", "")
            filename = elements[4]
            # Check so that the key doesn't exist first 
            uploaded_files[filename] = byte_size

        
        log.debug("Uploaded files: {}".format(uploaded_files))

        # Then, get the expected filelist from us
        # FIXME: Use a dict instead 
        local_files = {}
        for root, directories, filenames in os.walk(path_to_runfolder):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                local_size = os.path.getsize(full_path)
                local_files[full_path] = str(local_size)
        
        log.debug("Local files {}".format(local_files))

        # Sort both files - then compare them. We do not need to sort them if we are using dicts!
        # We consider local files to be the truth - obviously. So if there are more data uploaded 
        # than present locally we ignore it. 
        reupload_files = []
        for k, v in local_files.iteritems(): 
            if k in uploaded_files: 
                log.debug("Local file has been uploaded {}".format(k))

                if v != uploaded_files[k]: 
                    log.info("::: ERROR ::: Local file size {} doesn't match remote file size {} for file {}".format(v, uploaded_files[k], k))
                    reupload_files.append(k)
                else: 
                    log.debug("Local file size matches uploaded file size")
            else: 
                log.info("::: ERROR ::: Local file has NOT been uploaded {}".format(k))
                reupload_files.append(k)

        # Step 3 - upload the missing files with the previous description
        if reupload_files: 
            log.info("Will now reupload the following files: {}".format(reupload_files))

            # TODO: Better path? 
            dsmc_reupload = os.path.join("/tmp", "dsmc-reupload-{}".format(uniq_id))
            with open(dsmc_reupload, 'wa') as f:
                for r in reupload_files:  
                    f.write('"{}"\n'.format(r))

            log.debug("Written files to reupload to {}".format(dsmc_reupload))

            #dsmc archive -descr=${DESCR} -filelist=${FILELIST} 2>&1 | tee `pwd`/${FILELIST}.tsm_log
            cmdtwo = "dsmc archive -filelist={} -description={}".format(dsmc_reupload, latest_descr)
            cmd = "echo {}".format(cmdtwo)
            log.debug("Running command {}".format(cmdtwo))
            job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=monitored_dir, stdout=dsmc_log_file, stderr=dsmc_log_file)

        else: 
            log.debug("Nothing to do - everything already uploaded.")

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED,
            "dsmc_log": dsmc_log_file}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)     

'''        

        uniq_id = str(uuid.uuid4())
        cmd = "dsmc archive {}/ -subdir=yes -description={}".format(path_to_runfolder, uniq_id)
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir="/tmp", stdout="/tmp/stdout", stderr="/tmp/stderr")

'''

class UploadHandler(BaseDsmcHandler):

    """
    Validate that the runfolder exists under monitored directories
    :param runfolder: The runfolder to check for
    :param monitored_dir: The root in which the runfolder should exist
    :return: True if this is a valid runfolder
    """
    @staticmethod
    def _validate_runfolder_exists(runfolder, monitored_dir):
        if os.path.isdir(monitored_dir):
            sub_folders = [ name for name in os.listdir(monitored_dir)
                            if os.path.isdir(os.path.join(monitored_dir, name)) ]
            return runfolder in sub_folders
        else:
            return False

    @staticmethod
    def _is_valid_log_dir(log_dir):
        """
        Check if the log dir is valid. Right now only checks it is a directory.
        :param: log_dir to check
        :return: True is valid dir, else False
        """
        return os.path.isdir(log_dir)


    """
    Start a dsmc process.

    The request needs to pass the path the md5 sum file to check in "path_to_md5_sum_file". This path
    has to point to a file in the runfolder.

    :param runfolder: name of the runfolder we want to start archiving

    """
    def post(self, runfolder_archive):

        monitored_dir = self.config["monitored_directory"]

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder_archive, monitored_dir))

        #request_data = json.loads(self.request.body)
        #description = request_data["description"]

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_dir = self.config["dsmc_log_directory"]
        uniq_id = str(uuid.uuid4())

        if not UploadHandler._is_valid_log_dir(dsmc_log_dir):
            raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_dir))

        dsmc_log_file = "{}/dsmc_{}_{}-{}".format(dsmc_log_dir,
                                                      runfolder_archive,
                                                      uniq_id,
        #                                              description,
                                                      datetime.datetime.now().isoformat())

       # cmd = "export DSM_LOG={} && dsmc archive {} -subdir=yes -desc={}".format(dsmc_log_file,
       #                                                                          runfolder,
       #                                                                          description)
        #cmd = "/usr/bin/dsmc q"
        #dsmc archive <path to runfolder_archive>/ -subdir=yes -description=`uuidgen`
        
        #cmd = "dsmc archive {}/ -subdir=yes -description={}".format(path_to_runfolder, uniq_id)
        # FIXME: echo is just used when testing return codes locally. 
        #cmd = "echo 'ANS1809W ANS2000W Test run started.' && echo ANS9999W && echo ANS1809W && exit 8" #false
        cmd = "echo 'ANS1809W Test run started.' && echo ANS1809W && exit 8"
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=monitored_dir, stdout=dsmc_log_file, stderr=dsmc_log_file)

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED,
            "dsmc_log": dsmc_log_file}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)


class StatusHandler(BaseDsmcHandler):
    """
    Get the status of one or all jobs.
    """

    def get(self, job_id):
        """
        Get the status of the specified job_id, or if now id is given, the
        status of all jobs.
        :param job_id: to check status for (set to empty to get status for all)
        """

        if job_id:
            status = {"state": self.runner_service.status(job_id)}
        else:
            all_status = self.runner_service.status_all()
            status_dict = {}
            for k, v in all_status.iteritems():
                status_dict[k] = {"state": v}
            status = status_dict

        self.write_json(status)

#class StopHandler(BaseDsmcHandler):
#    """
#    Stop one or all jobs.
#    """
#
#    def post(self, job_id):
#        """
#        Stops the job with the specified id.
#        :param job_id: of job to stop, or set to "all" to stop all jobs
#        """
#        try:
#            if job_id == "all":
#                log.info("Attempting to stop all jobs.")
#                self.runner_service.stop_all()
#                log.info("Stopped all jobs!")
#                self.set_status(200)
#            elif job_id:
#                log.info("Attempting to stop job: {}".format(job_id))
#                self.runner_service.stop(job_id)
#                self.set_status(200)
#            else:
#                ArteriaUsageException("Unknown job to stop")
#        except ArteriaUsageException as e:
#            log.warning("Failed stopping job: {}. Message: ".format(job_id, e.message))
#            self.send_error(500, reason=e.message)
