import json
import logging
import os
import datetime
import uuid
import subprocess
import shutil
import re 
import pdb;

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


class ReuploadHelper(object):
    """
    Does the same as 
            #dsmc q ar /proj/ngi2016001/incoming/${RUNFOLDER} | grep "/proj/ngi2016001/incoming" | awk '{print $3" "$NF}'
    """
    # TODO: What to return if nothing is found? 
    # TODO: Check that process completed successfully    
    def get_pdc_descr(self, path_to_archive):     
        cmd = "dsmc q ar {}".format(path_to_archive)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        dsmc_out, dsmc_err = p.communicate()
        dsmc_out = dsmc_out.splitlines()

        # if dsmc_err: 
        #if process.returncode:
            #raise RuntimeError('something bad happened')

        uploaded_versions = [line.strip() for line in dsmc_out if path_to_archive in line]
        log.debug("Found the following uploaded versions of this archive: {}".format(uploaded_versions))
        
        # Uploads are chronologically sorted, with the latest upload last.
        latest_upload = uploaded_versions[-1:][0] 

        # We need the description of this upload: the last field. E.g.: 
        # 4,096  B  01/10/2017 16:47:24    /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive Never a33623ba-55ad-4034-9222-dae8801aa65e
        latest_descr = latest_upload.split()[-1:][0]
        log.debug("Latest uploaded version is {} with description {}".format(latest_upload, latest_descr))

        return latest_descr

    """
    Does the same as 
         
    """
    def get_pdc_filelist(self, path_to_archive, descr): 
        cmd = "dsmc q ar {} -subdir=yes -description={}".format(path_to_archive, descr)

        # TODO: Check that process completed successfully
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 
        dsmc_out, dsmc_err = p.communicate()
        dsmc_out = dsmc_out.splitlines()
        # if dsmc_err: 
        #if process.returncode:
            #raise RuntimeError('something bad happened')        

        # Take out the bytes and the filename from the output
        # convert raw output to bytes - first field in matched_lines
        #
        # Get the lines containing the path. Then take out the field nr 1 and 5
        matched_lines = [line.strip() for line in dsmc_out if path_to_archive in line]
        log.debug("RAW matched lines: {}".format(matched_lines))

        # NB uploaded list contains folders as well, but when we check local content
        # we only look at the files, and ignore the folders.
        uploaded_files = {} #[line for line in matched_lines]

        # Line looks like
        #4,096  B  2017-07-27 17.48.34    /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive/Config Never e374bd6b-ab36-4f41-94d3-f4eaea9f30d4
        # but sometimes it can be "4 096", depending on the OS locale. 
        for line in matched_lines: 
            elements = line.split()
            
            if "," in elements[0]: 
                byte_size = elements[0].replace(",", "")
                filename = elements[4]
            elif elements[0] == "0": 
                byte_size = 0
                filename = elements[4]
            else: 
                byte_size = "{}{}".format(elements[0], elements[1])
                filename = elements[5]
            
            # TODO: Check so that the key doesn't exist first?
            # E.g. if uploaded twice with the same descr 
            uploaded_files[filename] = byte_size
        
        log.debug("Previously uploaded files for the archive are: {}".format(uploaded_files))

        return uploaded_files

    def get_local_filelist(self, path_to_archive): 
        local_files = {}
        for root, directories, filenames in os.walk(path_to_archive):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                local_size = os.path.getsize(full_path)
                local_files[full_path] = str(local_size)
        
        log.debug("Local files for the archive are {}".format(local_files))

        return local_files

    def get_files_to_reupload(self, local_files, uploaded_files):
        # Sort both - then compare them. We do not need to sort them if we are using dicts!
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

        return reupload_files

    # TODO: Return something sensible. Error checking. 
    def reupload(self, reupload_files, descr, uniq_id, run_dir, dsmc_log_file, runner_service):
        log.info("Will now reupload the following files: {}".format(reupload_files))

        dsmc_reupload = os.path.join("/tmp", "arteria-dsmc-reupload-{}".format(uniq_id))

        with open(dsmc_reupload, 'wa') as f:
            for r in reupload_files:
                f.write('"{}"\n'.format(r))

        log.debug("Written files to reupload to {}".format(dsmc_reupload))

        #dsmc archive -descr=${DESCR} -filelist=${FILELIST} 2>&1 | tee `pwd`/${FILELIST}.tsm_log
        cmd = "dsmc archive -filelist={} -description={}".format(dsmc_reupload, descr)
        log.debug("Running command {}".format(cmd))
        job_id = runner_service.start(cmd, nbr_of_cores=1, run_dir=run_dir, stdout=dsmc_log_file, stderr=dsmc_log_file)

        return job_id

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

    def post(self, runfolder_archive): 
        monitored_dir = self.config["path_to_archive_root"]
        helper = ReuploadHelper()

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return     

        path_to_archive = os.path.join(monitored_dir, runfolder_archive)
        uniq_id = str(uuid.uuid4())        
        dsmc_log_dir = self.config["dsmc_log_directory"]

        if not UploadHandler._is_valid_log_dir(dsmc_log_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not a directory!".format(dsmc_log_dir))
            self.write_object(response_data)
            return                 

        # FIXME: log file not used atm
        dsmc_log_file = "{}/dsmc_{}_{}-{}".format(dsmc_log_dir,
                                                      runfolder_archive,
                                                      uniq_id,
                                                      datetime.datetime.now().isoformat())            

        #pdb.set_trace() 

        # Step 1 - fetch the description of the last uploaded version of this archive
        descr = helper.get_pdc_descr(path_to_archive)

        # Step 2 - check the difference of the uploaded version vs the local archive
        # Step 2a, get filelist from PDC      
        uploaded_files = helper.get_pdc_filelist(path_to_archive, descr)

        # 2b, Then, get the expected filelist from us
        local_files = helper.get_local_filelist(path_to_archive)

        # 2c, Check if we have to reupload anything
        reupload_files = helper.get_files_to_reupload(local_files, uploaded_files)

        # Step 3 - upload the missing files with the previous description
        if reupload_files: 
            job_id = helper.reupload(reupload_files, descr, uniq_id, monitored_dir, dsmc_log_file, self.runner_service)
            log.debug("job_id {}".format(job_id))
        
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

            self.set_status(202, reason="started reuploading")
        else: 
            log.debug("Nothing to do - everything already uploaded.")

            response_data = {
            "service_version": version,
            "link": status_end_point,
            "state": State.DONE,
            "dsmc_log": dsmc_log_file}

            self.set_status(200, reason="nothing to reupload")
        
        self.write_object(response_data)     

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

        monitored_dir = self.config["path_to_archive_root"]

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_dir = self.config["dsmc_log_directory"]
        uniq_id = str(uuid.uuid4())

        if not UploadHandler._is_valid_log_dir(dsmc_log_dir):
            raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_dir))

        # TODO: Need to put the logs in the commands as well. 
        dsmc_log_file = "{}/dsmc_{}_{}-{}".format(dsmc_log_dir,
                                                      runfolder_archive,
                                                      uniq_id,
        #                                              description,
                                                      datetime.datetime.now().isoformat())

       # cmd = "export DSM_LOG={} && dsmc archive {} -subdir=yes -desc={}".format(dsmc_log_file,
       #                                                                          runfolder,
       #                                                                          description)
        
        cmd = "dsmc archive {}/ -subdir=yes -description={}".format(path_to_runfolder, uniq_id)
        # FIXME: echo is just used when testing return codes locally. 
        #cmd = "echo 'ANS1809W ANS2000W Test run started.' && echo ANS9999W && echo ANS1809W && exit 8" #false
        #cmd = "echo 'ANS1809W Test run started.' && echo ANS1809W && exit 8"
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


class GenChecksumsHandler(BaseDsmcHandler): 
    # TODO: Add helper functions - refactor with other base class 

    def post(self, runfolder_archive):
        path_to_archive_root = os.path.abspath(self.config["path_to_archive_root"])
        checksum_log = os.path.abspath(os.path.join(self.config["dsmc_log_directory"], "checksum.log"))
        
        if not UploadHandler._validate_runfolder_exists(runfolder_archive, path_to_archive_root):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return

        path_to_archive = os.path.join(path_to_archive_root, runfolder_archive)
        filename = "checksums_prior_to_pdc.md5"
        
        cmd = "cd {} && /usr/bin/find -L . -type f ! -path '{}' -exec /usr/bin/md5sum {{}} + > {}".format(path_to_archive, filename, filename)
        log.debug("Will now execute command {}".format(cmd))
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=path_to_archive_root, stdout=checksum_log, stderr=checksum_log) 

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)

class CreateDirHandler(BaseDsmcHandler):
    # TODO: Refactor
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


    @staticmethod
    def _verify_unaligned(srcdir):
        # On biotanks we need to verify the Unaligned link. 
        unaligned_link = os.path.join(srcdir, "Unaligned")
        unaligned_dir = os.path.abspath(unaligned_link)

        if not os.path.exists(unaligned_link) or not os.path.islink(unaligned_link): 
            log.info("Expected link {} doesn't seem to exist or is broken. Aborting.".format(unaligned_link))
            return False
        elif not os.path.exists(unaligned_dir) or not os.path.isdir(unaligned_dir): 
            log.info("Expected directory {} doesn't seem to exist. Aborting.".format(unaligned_dir))
            return False

        return True
    
    @staticmethod
    def _verify_dest(destdir, remove=False): 
        log.debug("Checking to see if {} exists".format(destdir))

        if os.path.exists(destdir):
            if remove: 
                log.debug("Archive directory {} already exists. Operator requested to remove it.".format(destdir))
                shutil.rmtree(destdir)
                return True
            else: 
                log.debug("Archive directory {} already exists. Aborting.".format(destdir))
                return False
        else: 
            return True

    """ 
    Symlink _archive dir to runfolder, and filter out some stuff. 
    """
    @staticmethod
    def _create_archive(oldtree, newtree, exclude_dirs=[], exclude_extensions=[]): 
        try: 
            content = os.listdir(oldtree)

            for entry in content: 
                oldpath = os.path.join(oldtree, entry)
                newpath = os.path.join(newtree, entry)

                if os.path.isdir(oldpath) and entry not in exclude_dirs:
                    os.mkdir(newpath)
                    CreateDirHandler._create_archive(oldpath, newpath, exclude_dirs, exclude_extensions)
                elif os.path.isfile(oldpath):
                    _, ext = os.path.splitext(oldpath)

                    if ext not in exclude_extensions: 
                        os.symlink(oldpath, newpath)
                    else: 
                        log.debug("Skipping {} because it is excluded".format(oldpath))
                else: 
                    log.debug("Skipping {} because it is excluded".format(oldpath))
        except OSError, msg: 
            errmsg = "Error when creating archive directory: {}".format(msg)
            log.debug(errmsg)
            raise ArteriaUsageException(errmsg)       

    """
    Create a directory to be used for archiving.

    :param runfolder: name of the runfolder we want to create an archive dir of
    :param exclude: list of patterns to use when excluding files and/or dirs 
    :param remove: boolean to indicate if we should remove previous archive 

    """
    def post(self, runfolder):

        monitored_dir = self.config["monitored_directory"]
        path_to_runfolder = os.path.abspath(os.path.join(monitored_dir, runfolder))
        #TODO: On Irma we want /proj/ngi2016001/nobackup/arteria/pdc_archive_links
        path_to_archive_root = self.config["path_to_archive_root"]
        path_to_archive = os.path.abspath(os.path.join(path_to_archive_root, runfolder) + "_archive")

        request_data = json.loads(self.request.body)
        # TODO: Catch when no data is included
        remove = eval(request_data["remove"]) # str2bool
        exclude_dirs = self.config["exclude_dirs"]
        exclude_extensions = self.config["exclude_extensions"]

        # FIXME: Dont raise here. 
        if not CreateDirHandler._validate_runfolder_exists(runfolder, monitored_dir):
            # TODO: Write a wrapper that can print out this. 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "{} is not found under {}!".format(runfolder_archive, monitored_dir)
            log.debug("Error encountered when validating runfolder: {}".format(reason))
            self.set_status(500, reason=reason)
            self.write_object(response_data)
            return

        # We want to verify that the Unaligned folder is setup correctly when running on biotanks.
        my_host = self.request.headers.get('Host')            
        # FIXME: Don't raise here
        # FIXME: Make testcase for biotank stuff. 
        if "biotank" in my_host and not CreateDirHandler._verify_unaligned(path_to_runfolder): 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Unaligned directory link {} is broken or missing!".format(os.path.join(path_to_runfolder, "Unaligned"))
            log.debug("Error encountered when validating Unaligned: {}".format(reason))
            self.set_status(500, reason=reason)
            self.write_object(response_data)      
            return      

        # FIXME: Don't raise here
        if not CreateDirHandler._verify_dest(path_to_archive, remove): 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Error when checking the destination path {} (remove={}).".format(path_to_archive, remove)
            log.debug("Error encountered when validating Unaligned: {}".format(reason))
            self.set_status(500, reason=reason)
            self.write_object(response_data)      
            return      
              
        # Raise exception? Print out error to user client. 
        try: 
            os.mkdir(path_to_archive)
            CreateDirHandler._create_archive(path_to_runfolder, path_to_archive, exclude_dirs, exclude_extensions)
        except ArteriaUsageException, msg: 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Error when creating archive dir: {}".format(msg)
            log.debug(reason)
            self.set_status(500, reason=reason)
            self.write_object(response_data)      
            return                  

        response_data = {"service_version": version, "state": State.DONE}

        self.set_status(200, reason="Finished processing.")
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
            # TODO: Update the correct status for all jobs; the filtering in jobrunner doesn't work here.
            all_status = self.runner_service.status_all()
            status_dict = {}
            for k, v in all_status.iteritems():
                status_dict[k] = {"state": v}
            status = status_dict

        self.write_json(status)

class StopHandler(BaseDsmcHandler):
    """
    Stop one or all jobs.
    """

    def post(self, job_id):
        """
        Stops the job with the specified id.
        :param job_id: of job to stop, or set to "all" to stop all jobs
        """
        try:
            if job_id == "all":
                log.info("Attempting to stop all jobs.")
                self.runner_service.stop_all()
                log.info("Stopped all jobs!")
                self.set_status(200)
            elif job_id:
                log.info("Attempting to stop job: {}".format(job_id))
                self.runner_service.stop(job_id)
                self.set_status(200)
            else:
                ArteriaUsageException("Unknown job to stop")
        except ArteriaUsageException as e:
            log.warning("Failed stopping job: {}. Message: ".format(job_id, e.message))
            self.send_error(500, reason=e.message)
