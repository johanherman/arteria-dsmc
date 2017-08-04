
import json
import logging
import os
import datetime
import uuid

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


class UploadMissingHandler(BaseDsmcHandler):
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

    def post(self, runfolder_archive): 
        pass
'''        monitored_dir = self.config["monitored_directory"]

        if not StartHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder_archive, monitored_dir))            

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)

        ## pdc-descr

        if [ $# -eq 0 ]; then
        echo "Missing argument. Must give 1) the runfolder/archive (not path, and without trailing /) name to check for."
        exit 1
        fi 

        RUNFOLDER=$1

        dsmc q ar /proj/ngi2016001/incoming/${RUNFOLDER} | grep "/proj/ngi2016001/incoming" | awk '{print $3" "$NF}'


        ## pdc-diff

        if [ $# -le 1 ]; then
        echo "Missing argument. Must give 1) the path to the runfolder/archive to check for, 2) description of archived path."
        exit 1
        fi 

        RFPATH=`realpath $1`
        RUNFOLDER=`basename ${RFPATH}`
        RFPARENT=`dirname ${RFPATH}`
        DESCR=$2

        # Step 1, get filelist from PDC
        dsmc q ar /proj/ngi2016001/incoming/${RUNFOLDER}/ -subdir=yes -description=${DESCR} > ${RUNFOLDER}.pdc_raw
        grep "/proj/ngi2016001" ${RUNFOLDER}.pdc_raw | awk '{print $1}'| sed 's/,//g' > ${RUNFOLDER}.pdc_bytes
        grep "/proj/ngi2016001" ${RUNFOLDER}.pdc_raw | awk -F " Never " '{print $1}' | sed -n 's,.*/proj/ngi2016001/incoming,/proj/ngi2016001/incoming,p' > ${RUNFOLDER}.pdc_files
        paste -d " " ${RUNFOLDER}.pdc_files ${RUNFOLDER}.pdc_bytes | sort > ${RUNFOLDER}.pdc_list
        #rm pdc-bytes pdc-files ${RUNFOLDER}.pdc_raw

        # Step 2, get expected filelist from us 
        find -L ${RFPATH}/ -printf "%p %s\n" > ${RUNFOLDER}.orig_list
        # Convert paths to original format used when uploading 
        sed -i "s,${RFPARENT},/proj/ngi2016001/incoming,g" ${RUNFOLDER}.orig_list 
        # remove first line, the parent dir, which is not included in pdc output
        sed '1d' ${RUNFOLDER}.orig_list > ${RUNFOLDER}.tmp_list 
        sort ${RUNFOLDER}.tmp_list > ${RUNFOLDER}.orig_list_sorted
        mv ${RUNFOLDER}.orig_list_sorted ${RUNFOLDER}.orig_list
        rm ${RUNFOLDER}.tmp_list

        if ! diff ${RUNFOLDER}.orig_list ${RUNFOLDER}.pdc_list > ${RUNFOLDER}.diff ; then 
                echo "Filelist and/or number of bytes between ${RFPATH} and PDC differs!"
                echo "See ${RUNFOLDER}.diff for details."
                exit 1
        else
                echo "Filelist and number of bytes are identical between ${RFPATH} and copy on PDC."
                exit 0
        fi

        ## pdc-upload-missing

        if [ $# -le 2 ]; then
        echo "Missing argument. Must give 1) the runfolder/archive (not path, and without trailing /) name to upload for, 2) the description to re-use, 3) path to file containing the missing files to upload."
        exit 1
        fi

        RUNFOLDER=$1
        DESCR=$2
        DIFF=$3

        if [ `grep ${RUNFOLDER} ${DIFF} | wc -l` -eq 0 ]; then 
                echo "No mention of ${RUNFOLDER} in ${DIFF}. Did you enter correct arguments?"
                exit 1
        fi 

        FILELIST=${RUNFOLDER}.files_to_upload

        # Remove first and last column, preserving eventual spaces in paths, and then in the end
        # enclose the strings (paths to files for uploading) in quotes. 
        grep "/proj/ngi2016001/incoming" ${DIFF} | cut -d' ' -f2- | rev | cut -d ' ' -f2- | rev | sed -e 's/^/"/; s/$/"/' > ${FILELIST}

        uniq_id = str(uuid.uuid4())
        cmd = "dsmc archive {}/ -subdir=yes -description={}".format(path_to_runfolder, uniq_id)
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir="/tmp", stdout="/tmp/stdout", stderr="/tmp/stderr")

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
'''

class StartHandler(BaseDsmcHandler):

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

        if not StartHandler._validate_runfolder_exists(runfolder_archive, monitored_dir):
            raise ArteriaUsageException("{} is not found under {}!".format(runfolder_archive, monitored_dir))

        #request_data = json.loads(self.request.body)
        #description = request_data["description"]

        path_to_runfolder = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_dir = self.config["dsmc_log_directory"]
        uniq_id = str(uuid.uuid4())

        if not StartHandler._is_valid_log_dir(dsmc_log_dir):
            raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_dir))

        dsmc_log_file = "{}/dsmc_{}_{}-{}".format(dsmc_log_dir,
                                                      runfolder_archive,
                                                      uniq_id,
        #                                              description,
                                                      datetime.datetime.now().isoformat())

        #cmd = " ".join(["md5sum -c", path_to_md5_sum_file])
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

        #job_id = self.runner_service.start(cmd,
        #                                   nbr_of_cores=1,
        #                                   run_dir=monitored_dir,
        #                                   stdout=dsmc_log_file,
        #                                   stderr=dsmc_log_file)

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
