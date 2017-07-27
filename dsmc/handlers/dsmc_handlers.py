
import json
import logging
import os
import datetime


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


class StartHandler(BaseDsmcHandler):

    """
    Validate that the runfolder exists under monitored directories
    :param runfolder: The runfolder to check for
    :param monitored_dir: The root in which the runfolder should exist
    :return: True if this is a valid runfolder
    """
    #@staticmethod
    #def _validate_runfolder_exists(runfolder, monitored_dir):
    #    if os.path.isdir(monitored_dir):
    #        sub_folders = [ name for name in os.listdir(monitored_dir)
    #                        if os.path.isdir(os.path.join(monitored_dir, name)) ]
    #        return runfolder in sub_folders
    #    else:
    #        return False

    #@staticmethod
    #def _is_valid_log_dir(log_dir):
    #    """
    #    Check if the log dir is valid. Right now only checks it is a directory.
    #    :param: log_dir to check
    #    :return: True is valid dir, else False
    #    """
    #    return os.path.isdir(log_dir)


    """
    Start a dsmc process.

    The request needs to pass the path the md5 sum file to check in "path_to_md5_sum_file". This path
    has to point to a file in the runfolder.

    :param runfolder: name of the runfolder we want to start archiving

    """
    def post(self, runfolder):

        #monitored_dir = self.config["monitored_directory"]

        #if not StartHandler._validate_runfolder_exists(runfolder, monitored_dir):
            #raise ArteriaUsageException("{} is not found under {}!".format(runfolder, monitored_dir))

        #request_data = json.loads(self.request.body)

        #path_to_runfolder = os.path.join(monitored_dir, runfolder)
        #description = request_data["description"]

        ##dsmc_log_dir = self.config["dsmc_log_directory"]

        ##if not StartHandler._is_valid_log_dir(dsmc_log_dir):
            ##raise ArteriaUsageException("{} is not a directory!".format(dsmc_log_dir))

        #dsmc_log_file = "{}/dsmerror_{}_{}-{}".format(dsmc_log_dir,
        #                                              runfolder,
        #                                              description,
        #                                              datetime.datetime.now().isoformat())

        #cmd = " ".join(["md5sum -c", path_to_md5_sum_file])
       # cmd = "export DSM_LOG={} && dsmc archive {} -subdir=yes -desc={}".format(dsmc_log_file,
       #                                                                          runfolder,
       #                                                                          description)
        cmd = "/usr/bin/dsmc q"
        job_id = self.runner_service.start(cmd, nbr_of_cores=1, run_dir="/tmp", stdout="/tmp/stdout", stderr="/tmp/stderr")

        #job_id = self.runner_service.start(cmd,
        #                                   nbr_of_cores=1,
        #                                   run_dir=monitored_dir,
        #                                   stdout=dsmerror_log_file,
        #                                   stderr=dsmerror_log_file)

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
