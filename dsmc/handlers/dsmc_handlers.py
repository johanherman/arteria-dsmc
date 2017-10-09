import json
import logging
import os
import datetime
import uuid
import subprocess
import shutil
import re 
import pdb
import tarfile
import errno

from arteria.exceptions import ArteriaUsageException
from arteria.web.state import State
from arteria.web.handlers import BaseRestHandler

from dsmc import __version__ as version
from dsmc.lib.jobrunner import LocalQAdapter

log = logging.getLogger(__name__)

class BaseDsmcHandler(BaseRestHandler):
    """
    Base handler for dsmc upload operations.
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

    @staticmethod
    def _validate_runfolder_exists(runfolder, monitored_dir):
        """
        Validate that the runfolder exists under monitored directories
        :param runfolder: The runfolder to check for
        :param monitored_dir: The root in which the runfolder should exist
        :return: True if this is a valid runfolder
        """
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

class VersionHandler(BaseDsmcHandler):
    """
    Get the version of the service
    """

    def get(self):
        """
        Returns the version of the dsmc service
        """
        self.write_object({"version": version })


class ReuploadHelper(object):
    """ 
    Helper class for the ReuploadHandler. Methods put her mainly to faciliate easier testing. 
    """
    # TODO: What to return if nothing is found? 
    # TODO: Check that process completed successfully
    def get_pdc_descr(self, path_to_archive, dsmc_log_dir):
        """
        Fetches the archive `description` label from PDC. 

        :param path_to_archive: The path to the archive uploaded that we want to get the description for
        :return: A dsmc description if successful, otherwise a FOO
        """
        log.info("Fetching description for latest upload of {} to PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc q ar {}".format(dsmc_log_dir, path_to_archive)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        dsmc_out, dsmc_err = p.communicate()
        dsmc_out = dsmc_out.splitlines()

        log.debug("Raw output from dsmc: {}".format(dsmc_out))

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

    def get_pdc_filelist(self, path_to_archive, descr, dsmc_log_dir): 
        """
        Gets the files and their sizes from PDC for a certain path (archive), with a specific description. 

        :param path_to_archive: The path to the archive 
        :param descr: The description label for the uploaded archive
        :return The dict `uploaded_files` containing a mapping between uploaded file and size in bytes
        """
        log.info("Fetching remote filelist for {} from PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc q ar {} -subdir=yes -description={}".format(dsmc_log_dir, path_to_archive, descr)

        # TODO: Check that process completed successfully
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 
        dsmc_out, dsmc_err = p.communicate()
        dsmc_out = dsmc_out.splitlines()
        # if dsmc_err: 
        #if process.returncode:
            #raise RuntimeError('something bad happened')        

        # Take out the bytes and the filename from the output. Then convert the raw filesize 
        # (first field in matched_lines) output to bytes. 
        
        # We're only interested in the lines from the dsmc output that contains the 
        # path to the archive.
        matched_lines = [line.strip() for line in dsmc_out if path_to_archive in line]
        log.debug("Uploaded files to PDC: {}".format(matched_lines))

        uploaded_files = {} 

        # We need to convert the sizes to a common format for easier comparison with local size. 
        # An output line can look like 
        #4,096  B  2017-07-27 17.48.34    /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive/Config Never e374bd6b-ab36-4f41-94d3-f4eaea9f30d4
        # but varies, depending on the environment's locale. 
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

    # TODO: In try block? 
    def get_local_filelist(self, path_to_archive): 
        """
        Gets the list of all files and their sizes in the local archive. 

        :param path_to_archive: The path to the local archive
        :return: The dict `local_files` that maps between local file and size in bytes
        """
        log.info("Generating local filelist for {}...".format(path_to_archive))
        local_files = {}
        for root, directories, filenames in os.walk(path_to_archive):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                local_size = os.path.getsize(full_path)
                local_files[full_path] = str(local_size)
        
        log.debug("Local files for the archive are {}".format(local_files))

        return local_files

    def get_files_to_reupload(self, local_files, uploaded_files):
        """
        Compare the list of local and uploaded files. If the size in byte differs, 
        or if the file exists locally, but not remotely, then it should be re-uploaded. 

        :param local_files: Dict local files -> size in bytes
        :param uploaded_files: Dict of remote files -> size in bytes
        :return: List `reupload_files` with the path to all files that needs reuploading
        """
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
    def reupload(self, reupload_files, descr, uniq_id, dsmc_log_dir, output_file, runner_service):
        """
        Tells `dsmc` to upload all files in the given filelist.

        :param reupload_files: List of files to reupload
        :param descr: The unique description of the already uploaded archive with missing files
        :param uniq_id: A uniq ID for this sessions DSMC interactions
        :param FIXME run_dir: The current dir when `dsmc` starts running
        :param FIXME dsmc_log_file: Path to the file where stdout and stderr from `dsmc` will be sent
        :param runner_service: The runner service to use 
        :return: The LocalQ job id associated with this job
        """
        log.info("Will now reupload the following files: {}".format(reupload_files))

        dsmc_reupload = os.path.join("/tmp", "arteria-dsmc-reupload-{}".format(uniq_id))

        with open(dsmc_reupload, 'wa') as f:
            for r in reupload_files:
                f.write('"{}"\n'.format(r))

        log.debug("Written files to reupload to {}".format(dsmc_reupload))

        cmd = "export DSM_LOG={} && dsmc archive -filelist={} -description={}".format(dsmc_log_dir, dsmc_reupload, descr)
        log.debug("Running command {}".format(cmd))
        job_id = runner_service.start(cmd, nbr_of_cores=1, run_dir=dsmc_log_dir, stdout=output_file, stderr=output_file)

        return job_id

# FIXME: Helper function for returning an error message. 
class ReuploadHandler(BaseDsmcHandler):
    """
    Handler for (re-)uploading missing files for a certain archive already uploaded to PDC. 
    Useful when e.g. a previous upload was interrupted, or if new files should be added. 
    """

    def post(self, runfolder_archive): 
        """
        Compares local copy of the runfolder archive with the latest uploaded version.
        If any files are missing on the remote (PDC) side then they will be uploaded.
        Job is run in the background to be polled by the status endpoint.         

        :param runfolder_archive: the archive we want to re-upload
        :return: HTTP 200 if nothing to reupload, HTTP 202 if reupload started successfully, with a `job_id` to be used for later polling,
                 HTTP 500 if unexpected error detected. 
            
        """
        monitored_dir = self.config["path_to_archive_root"]
        helper = ReuploadHelper()

        if not UploadHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return     

        path_to_archive = os.path.join(monitored_dir, runfolder_archive)
        uniq_id = str(uuid.uuid4())        
        dsmc_log_root_dir = self.config["dsmc_log_directory"]

        if not BaseDsmcHandler._is_valid_log_dir(dsmc_log_root_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not a directory!".format(dsmc_log_root_dir))
            self.write_object(response_data)
            return                 

        # FIXME: log file not used atm
        dsmc_log_dir = "{}/dsmc_{}_{}".format(dsmc_log_root_dir,
                                                      runfolder_archive,
                                                      uniq_id)
        if not os.path.exists(dsmc_log_dir): 
            os.makedirs(dsmc_log_dir)
        
        dsmc_output = "{}/dsmc_output".format(dsmc_log_dir)

        # Step 1 - fetch the description of the last uploaded version of this archive
        # TODO: What to do if not found? 
        descr = helper.get_pdc_descr(path_to_archive, dsmc_log_dir)

        # Step 2 - check the difference of the uploaded version vs the local archive
        # Step 2a, get filelist from PDC      
        uploaded_files = helper.get_pdc_filelist(path_to_archive, descr, dsmc_log_dir)

        # 2b, Then, get the expected filelist from us
        # TODO: What to do if no files are found? 
        local_files = helper.get_local_filelist(path_to_archive)
        # NB uploaded list contains folders as well, but when we check local content
        # we only look at the files, and ignore the folders.
        # 2c, Check if we have to reupload anything
        # TODO: Is this enough? 
        reupload_files = helper.get_files_to_reupload(local_files, uploaded_files)

        # Step 3 - upload the missing files with the previous description
        if reupload_files: 
            job_id = helper.reupload(reupload_files, descr, uniq_id, dsmc_log_dir, dsmc_output, self.runner_service)
            log.debug("Reupload job_id {}".format(job_id))
        
            status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

            response_data = {
                "job_id": job_id,
                "service_version": version,
                "link": status_end_point,
                "state": State.STARTED,
                "dsmc_log_dir": dsmc_log_dir}

            self.set_status(202, reason="started reuploading")
        else: 
            log.debug("Nothing to do - everything already uploaded.")

            response_data = {
            "service_version": version,
            "link": status_end_point,
            "state": State.DONE,
            "dsmc_log_dir": dsmc_log_dir}

            self.set_status(200, reason="nothing to reupload")
        
        self.write_object(response_data)     

class UploadHandler(BaseDsmcHandler):
    """
    Handler for uploading an archive to PDC. 
    """
    def post(self, runfolder_archive):
        """
        Tells `dsmc` to upload `runfolder_archive` to PDC, with a uniquely generated description label. 
        Job is run in the background to be polled by the status endpoint. 

        :param runfolder_archive: the name of the archive that we want to upload
        :return: HTTP 202 if the upload as started successfully, with a `job_id` to be used for later status polling, 
                 HTTP 500 if an unexpected error was encountered
        """

        monitored_dir = self.config["path_to_archive_root"]

        if not BaseDsmcHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return

        path_to_archive = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_root_dir = self.config["dsmc_log_directory"]
        uniq_id = str(uuid.uuid4())

        if not BaseDsmcHandler._is_valid_log_dir(dsmc_log_root_dir):
            raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_root_dir))

        # TODO: Need to put the logs in the commands as well. 
        dsmc_log_dir = "{}/dsmc_{}_{}".format(dsmc_log_root_dir,
                                                      runfolder_archive,
                                                      uniq_id)
        if not os.path.exists(dsmc_log_dir): 
            os.makedirs(dsmc_log_dir)

        dsmc_output = "{}/dsmc_output".format(dsmc_log_dir)

        log.info("Uploading {} to PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc archive {}/ -subdir=yes -description={}".format(dsmc_log_dir, path_to_archive, uniq_id)
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=dsmc_log_dir, stdout=dsmc_output, stderr=dsmc_output)

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED,
            "dsmc_log_dir": dsmc_log_dir}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)


# TODO: Add helper functions - refactor with other base class 
class GenChecksumsHandler(BaseDsmcHandler): 
    """
    Handler for generating checksums for an archive before uploading to PDC. 
    """

    def post(self, runfolder_archive):
        """
        Calculates the MD5 checksums for each file in the runfolder archive, before uploading to PDC. 
        Job is run in the background to be polled by the status endpoint. 
    
        :param runfolder_archive: Name of the runfolder archive 
        :returns: HTTP 202 if checksum job has started successfully, with a `job_id` to be used in later polling, 
                  HTTP 500 if an unexpected error was encountered
        """
        path_to_archive_root = os.path.abspath(self.config["path_to_archive_root"])
        log_dir = self.config["dsmc_log_directory"]
        checksum_log = os.path.abspath(os.path.join(log_dir, "checksum.log"))
        
        if not BaseDsmcHandler._validate_runfolder_exists(runfolder_archive, path_to_archive_root):
            response_data = {"service_version": version, "state": State.ERROR}
            self.set_status(500, reason="{} is not found under {}!".format(runfolder_archive, path_to_archive_root))
            self.write_object(response_data)
            return

        path_to_archive = os.path.join(path_to_archive_root, runfolder_archive)
        filename = "checksums_prior_to_pdc.md5"
        
        # FIXME: The checksum file includes itself. Fix the removal!
        cmd = "cd {} && /usr/bin/find -L . -type f ! -path '{}' -exec /usr/bin/md5sum {{}} + > {}".format(path_to_archive, filename, filename)
        log.info("Generating checksums for {}".format(path_to_archive))
        log.debug("Will now execute command {}".format(cmd))
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir=log_dir, stdout=checksum_log, stderr=checksum_log) 

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
    """
    Handler for creating an archive to upload. 
    """

    @staticmethod
    def _verify_unaligned(srcdir):
        """
        Check that the archive contains the `Unaligned` symlink when running on biotanks. 
        The link should point to a proper directory. 

        :param srcdir: The path to the archive which we should investigate
        :return: True if `srcdir` contains a symlink `Unaligned` that points to a directory, 
                 False otherwise 
        """
        # TODO: Need a testcase for this 
        unaligned_link = os.path.join(srcdir, "Unaligned")
        unaligned_dir = os.path.abspath(unaligned_link)

        # TODO: Rewrite logic according to docstring. 
        if not os.path.exists(unaligned_link) or not os.path.islink(unaligned_link): 
            log.info("Expected link {} doesn't seem to exist or is broken. Aborting.".format(unaligned_link))
            return False
        elif not os.path.exists(unaligned_dir) or not os.path.isdir(unaligned_dir): 
            log.info("Expected directory {} doesn't seem to exist. Aborting.".format(unaligned_dir))
            return False

        return True
    
    @staticmethod
    def _verify_dest(destdir, remove=False):
        """
        Check if the proposed new archive already exists, and if the operator wants to remove it then do so. 

        :param destdir: Path to the archive to create
        :param remove: Boolean that specifies whether or not we should remove `destdir` if it already exists
        :return: True if the archive doesn't exist, or if it was removed successfully, 
                 False otherwise
        """
        log.debug("Checking to see if {} exists".format(destdir))

        # TODO: Try for rmtree
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
    def _create_archive(oldtree, newtree, exclude_dirs=[], exclude_extensions=[]): 
        """ 
        Create a new runfolder archive named `<runfolder>_archive` by iterating through 
        the runfolder and symlinking each file. If the service has been configured to 
        exclude certain directories or file extensions then those directories and symlinks
        will be ignored. 
        
        :param oldtree: Path to the runfolder
        :param newtree: Path to the archive which we are going to create
        :param exclude_dir: List of directory names to exclude from the archive
        :param exclude_extensions: List of file extensions to exclude from the archive 
        """        
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

    def post(self, runfolder):
        """
        Create a directory to be used for archiving.

        :param runfolder: name of the runfolder we want to create an archive dir of 
        :param remove: boolean to indicate if we should remove previous archive 
        :return: HTTP 200 if runfolder archive was created successfully, 
                 HTTP 500 otherwise
        """
        log.debug("Fetching configuration...")

        monitored_dir = self.config["monitored_directory"]
        path_to_runfolder = os.path.abspath(os.path.join(monitored_dir, runfolder))
        #TODO: On Irma we want /proj/ngi2016001/nobackup/arteria/pdc_archive_links
        path_to_archive_root = self.config["path_to_archive_root"]
        path_to_archive = os.path.abspath(os.path.join(path_to_archive_root, runfolder) + "_archive")

        exclude_dirs = self.config["exclude_dirs"]
        exclude_extensions = self.config["exclude_extensions"]

        log.debug("Parsing data from HTTP request...")

        request_data = json.loads(self.request.body)
        # TODO: Catch when no data is included
        remove = eval(request_data["remove"]) # str2bool

        log.debug("Validating runfolder...")

        if not BaseDsmcHandler._validate_runfolder_exists(runfolder, monitored_dir):
            # TODO: Write a wrapper that can print out this. 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "{} is not found under {}!".format(runfolder_archive, monitored_dir)
            log.debug("Error encountered when validating runfolder: {}".format(reason))
            self.set_status(500, reason=reason)
            self.write_object(response_data)
            return

        # We want to verify that the Unaligned folder is setup correctly when running on biotanks.
        log.debug("Validating Unaligned...")

        my_host = self.request.headers.get('Host')            
        # FIXME: Make testcase for biotank stuff. 
        if "biotank" in my_host and not CreateDirHandler._verify_unaligned(path_to_runfolder): 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Unaligned directory link {} is broken or missing!".format(os.path.join(path_to_runfolder, "Unaligned"))
            log.debug("Error encountered when validating Unaligned: {}".format(reason))
            self.set_status(500, reason=reason)
            self.write_object(response_data)      
            return      

        log.debug("Validating destination for archive...")

        if not CreateDirHandler._verify_dest(path_to_archive, remove): 
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Error when checking the destination path {} (remove={}).".format(path_to_archive, remove)
            log.debug(reason)
            self.set_status(500, reason=reason)
            self.write_object(response_data)      
            return      
              
        # Raise exception? Print out error to user client. 
        try: 
            log.info("Creating a new archive {}...".format(path_to_archive))
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

class CompressArchiveHandler(BaseDsmcHandler): 
    """
    Handler for compressing certain files in the archive before uploading. 
    """

    def post(self, archive): 
        """
        Create a gziped tarball of most files in the archive, with the exception of 
        certain excluded files and directories that are to be kept as-is in the archive.

        :param archive: The name of the archive which we should pack together
        :return: HTTP 200 if the tarball was created successfully, 
                 HTTP 500 otherwise 

        """
        # TODO: Validate archive exists etc
        path_to_archive_root = self.config["path_to_archive_root"]
        path_to_archive = os.path.abspath(os.path.join(path_to_archive_root, archive))
        tarball_path = "{}.tar.gz".format(os.path.join(path_to_archive, archive))

        exclude_from_tarball = self.config["exclude_from_tarball"]

        log.debug("Checking to see if {} exists".format(tarball_path))

        if os.path.exists(tarball_path):
            response_data = {"service_version": version, "state": State.ERROR}
            reason = "Tarball {} already exists. Manual intervention required. Aborting.".format(tarball_path)
            log.debug(reason)
            self.set_status(500, reason=reason)
            self.write_object(response_data)                  
            return 

        def exclude_content(tarinfo):
            """
            Filter function when creating the tarball
            """
            name = os.path.basename(tarinfo.name)
            # The name field contains the path to the file relative to the 
            # root dir of the archive, i.e. the path starts with "./".
            # Therefore the second element in the list split on "/"
            # will be the first subdir (if any) inside the archive.
            first_dir = tarinfo.name.split("/")[1]

            # Don't include the file if it matches our list of 
            # files to exclude, or if the first dir in its path
            # matches one of the dir names in our exception list. 
            for exclude in exclude_from_tarball: 
                if exclude == name or exclude == first_dir:
                    return None

            return tarinfo
      
        log.info("Creating tarball {}...".format(tarball_path))

        with tarfile.open(name=tarball_path, mode="w:gz", dereference=True) as tar: 
            tar.add(path_to_archive, arcname="./", recursive=True, filter=exclude_content)
        

        log.info("Removing files from {} that were added to {}".format(path_to_archive_root, tarball_path))

        # Remove files that we added to the tarball. 
        with tarfile.open(tarball_path) as tar: 
            for member in tar.getmembers(): 
                try: 
                    filepath = os.path.normpath(os.path.join(path_to_archive_root, archive, member.name))
                    if os.path.isfile(filepath): 
                        os.remove(filepath)
                    # FIXME: For now we ignore that we will end up with empty directories afterwards, 
                    # as it introduces extra complexities. 
                    #elif member.name != "." and os.path.isdir(filepath):
                    #    os.removedirs(filepath) 

                except OSError, e: 
                    response_data = {"service_version": version, "state": State.ERROR}
                    reason = "Could not remove file {}: {} Aborting".format(filepath, e)
                    log.debug(reason)
                    self.set_status(500, reason=reason)
                    self.write_object(response_data)
                    return 

        response_data = {"service_version": version, "state": State.DONE}
        self.set_status(200, reason="Finished creating the tarball")
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
