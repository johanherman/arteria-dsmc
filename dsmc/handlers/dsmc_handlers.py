import json
import logging
import os
import datetime
import uuid
import subprocess
import shutil
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


class GenChecksumsHandler(BaseDsmcHandler): 
    # TODO: Add helper functions - refactor with other base class 

    def post(self, runfolder_archive):

        # FIXME: This needs to be an other path for archives
        #monitored_dir = self.config["monitored_directory"]
        path_to_archive_root = "/tmp/apa/pdc_archive_links"
        monitored_dir = path_to_archive_root

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder_archive, monitored_dir))

        #request_data = json.loads(self.request.body)
        #description = request_data["description"]

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)

        filename = "checksums_prior_to_pdc.md5"
        
        #ssh <% $.host %> "cd <% $.runfolder %>_archive && find -L . -type f ! -path './checksums_prior_to_pdc.md5' -exec md5sum {} + > checksums_prior_to_pdc.md5"
        cmd = "cd {} && /usr/bin/find -L . -type f ! -path '{}' -exec /usr/bin/md5sum {{}} + > {}".format(path_to_runfolder, filename, filename)
        log.debug("Will now execute command {}".format(cmd))
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=monitored_dir, stdout="/tmp/checksum.log", stderr="/tmp/checksum.log") #FIXME: better log

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED}#,
            #"dsmc_log": dsmc_log_file}

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

    @staticmethod
    # Copies src dir tree to dest dir, and excludes a dir if it fullfills. 
    def _create_dest_dir(srcdir, destdir, exclude=None): 

        def _ignore_all_dirs(directory, content): 
            return set(f for f in content if not os.path.isdir(os.path.join(directory, f))) 

        shutil.copytree(source, dest, ignore=_ignore_all_dirs)

        #os.makedirs(destdir)

        #for entry in os.listdir(srcdir):
            #if not entry in exclude: 
             #   os.symlink(os.path.join(srcdir, entry), os.path.join(destdir, entry))
    
        #log.debug("Archive directory {} created successfully.".format(destdir))

    @staticmethod
    def _create_dest_file_links(srcdir, destdir, exclude=None):
        pass

    @staticmethod
    def _create_dest_archive(srcdir, destdir):
        cmd = "/bin/cp -rs {} {}"
        # FIXME: Check return code 
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # TODO: This can deadlock according to https://docs.python.org/2/library/subprocess.html
        retval = p.wait()
        #my_lines = p.stdout.readlines()
        return retval 


    """
    Create a directory to be used for archiving.

    :param runfolder: name of the runfolder we want to create an archive dir of
    :param exclude: list of patterns to use when excluding files and/or dirs 
    :param remove: boolean to indicate if we should remove previous archive 

    """
    def post(self, runfolder):

  #destdir = srcdir + "_archive" 
  #verify_src(srcdir, threshold)
  #verify_dest(destdir, remove)
  #create_dest(srcdir, destdir, exclude)

        monitored_dir = self.config["monitored_directory"]
        path_to_runfolder = os.path.join(monitored_dir, runfolder)
        #/proj/ngi2016001/nobackup/arteria/pdc_archive_links
        path_to_archive_root = "/tmp/apa/pdc_archive_links"
        path_to_archive = os.path.join(path_to_archive_root, runfolder) + "_archive"

        request_data = json.loads(self.request.body)
        log.debug("Body is {}".format(request_data))
        remove = eval(request_data["remove"]) # str2bool
        log.debug("Remove is {}".format(remove))
        exclude_dirs = request_data["exclude_dirs"]
        exclude_extensions = request_data["exclude_extensions"]

        if not CreateDirHandler._validate_runfolder_exists(runfolder, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder, monitored_dir))

        # We want to verify that the Unaligned folder is setup correctly when running on biotanks.
        my_host = self.request.headers.get('Host')            
        if "biotank" in my_host and not CreateDirHandler._verify_unaligned(path_to_runfolder): 
            raise ArteriaUsageException("Unaligned directory link {} is broken or missing!".format(os.path.join(path_to_runfolder, "Unaligned")))

        if not CreateDirHandler._verify_dest(path_to_archive, remove): 
            raise ArteriaUsageException("Error when checking the destination path.")
        
        log.debug("Copying {} to {}".format(path_to_runfolder, path_to_archive))
        cmd = "/bin/cp -rs {}/ {}".format(path_to_runfolder, path_to_archive)
        # FIXME: Check return code 
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # TODO: This can deadlock according to https://docs.python.org/2/library/subprocess.html
        retval = p.wait()
        
        if retval != 0: 
            raise ArteriaUsageException("Error when symlinking {}".format(p.stdout.readlines()))

        # Now we want to eliminate some of our files from the archive 
        for root, dirs, files in os.walk(path_to_archive):
            for name in dirs: 
                if name in exclude_dirs: 
                    log.debug("Removing {} from {}".format(name, root))
                    shutil.rmtree(os.path.join(root, name))

            for name in files:
                for ext in exclude_extensions: 
                    if name.endswith(ext):
                        log.debug("Removing {} from {}".format(name, root))
                        os.remove(os.path.join(root, name))

        #Risk these will take long time
        #_create_dest_dir(srcdir, destdir, exclude)
        #_create_dest_file_links(srcdir, destdir, exclude)
        #if not CreateDirHandler._create_dest_archive(path_to_runfolder, path_to_archive)

       # cmd = "export DSM_LOG={} && dsmc archive {} -subdir=yes -desc={}".format(dsmc_log_file,
       #                                                                          runfolder,
       #                                                                          description)
        #cmd = "/usr/bin/dsmc q"
        #dsmc archive <path to runfolder_archive>/ -subdir=yes -description=`uuidgen`
        
        #cmd = "dsmc archive {}/ -subdir=yes -description={}".format(path_to_runfolder, uniq_id)
        # FIXME: echo is just used when testing return codes locally. 
        #cmd = "echo 'ANS1809W ANS2000W Test run started.' && echo ANS9999W && echo ANS1809W && exit 8" #false
#        cmd = "echo 'ANS1809W Test run started.' && echo ANS1809W && exit 8"
 #       job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=monitored_dir, stdout=dsmc_log_file, stderr=dsmc_log_file)

  #      status_end_point = "{0}://{1}{2}".format(
   #         self.request.protocol,
    #        self.request.host,
     #       self.reverse_url("status", job_id))

        response_data = {
            #"job_id": job_id,
            "service_version": version,
            #"link": status_end_point,
            "state": State.DONE}#,
            #"dsmc_log": dsmc_log_file}

        self.set_status(202, reason="finished processing")
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
            # FIXME: Update the correct status for all jobs; the filtering in jobrunner doesn't work here
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
