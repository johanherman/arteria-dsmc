from localq.localQ_server import LocalQServer, Status
from arteria.web.state import State as arteria_state

class JobRunnerAdapter:
    """
    Specifies interface that should be used by jobrunners.
    """

    def start(self, cmd, nbr_of_cores, run_dir, stdout=None, stderr=None):
        """
        Start a job corresponding to cmd
        :param cmd: to run
        :param nbr_of_cores: cores the job needs
        :param run_dir: where to run the job
        :param stdout: Reroute stdout to here
        :param stderr: Reroute stderr to here
        :return: the jobid associated with it (None on failure).
        """
        raise NotImplementedError("Subclasses should implement this!")

    def stop(self, job_id):
        """
        Stop job with job_id
        :param job_id: of job to stop
        :return: the job_id of the stopped job, or None if not found.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def stop_all(self):
        """
        Stop all jobs
        :return: Nothing
        """
        raise NotImplementedError("Subclasses should implement this!")

    def status(self, job_id):
        """
        Status of job with id
        :param job_id: to get status for.
        :return: It's status
        """
        raise NotImplementedError("Subclasses should implement this!")

    def status_all(self):
        """
        Get status for all jobs
        :param job_id: to get status for.
        :return: A dict containing all jobs with job_id as key and status as value.
        """
        raise NotImplementedError("Subclasses should implement this!")


class LocalQAdapter(JobRunnerAdapter):
    """
    An implementation of `JobRunnerAdapter` running jobs through
    localq (a jobrunner which will schedule jobs on a single node).
    """

    @staticmethod
    def localq2arteria_status(status):
        """
        Convert a localq status to an arteria state
        :param status: to convert
        :return: the arteria state
        """

        if status == Status.COMPLETED:
            return arteria_state.DONE
        elif status == Status.FAILED:
            return arteria_state.ERROR
        elif status == Status.PENDING:
            return arteria_state.PENDING
        elif status == Status.RUNNING:
            return arteria_state.STARTED
        elif status == Status.CANCELLED:
            return arteria_state.CANCELLED
        elif status == Status.NOT_FOUND:
            return arteria_state.NONE
        else:
            return arteria_state.NONE

    def __init__(self, nbr_of_cores, interval=30, priority_method="fifo"):
        self.nbr_of_cores = nbr_of_cores
        self.server = LocalQServer(nbr_of_cores, interval, priority_method, use_shell=True)
        self.server.run()

    def start(self, cmd, nbr_of_cores, run_dir, stdout=None, stderr=None):
        return self.server.add(cmd, nbr_of_cores, run_dir, stdout=stdout, stderr=stderr)

    def stop(self, job_id):
        return self.server.stop_job_with_id(job_id)

    def stop_all(self):
        return self.server.stop_all_jobs()

    def status(self, job_id):
        return LocalQAdapter.localq2arteria_status(self.server.get_status(job_id))
        ###arteria_status = LocalQAdapter.localq2arteria_status(self.server.get_status(job_id))

# FIXME: Here we should probably do our error checking so that we signal
# the correct respond to our backup workflow.
# The code below contains some error that causes dsmc-ws to throw:
#  File "/opt/arteria/arteria-dsmc-env/lib/python2.7/site-packages/dsmc/lib/jobrunner.py", line 106, in status
#    if job.proc.returncode == 8: # warning
#AttributeError: 'NoneType' object has no attribute 'proc'


    ###    if arteria_status == arteria_state.ERROR:
            # Should be able to do something like
    ###        job = self.server.get_job_with_id(job_id)
# 36     if [ `grep -E 'ANS[0-9]+W' ${DSM_LOG} | wc -l` -eq `grep "ANS1809W" ${DSM_LOG}
#  | wc -l` ]; then

        ###    if job.proc.returncode == 8: # warning
        ###        warnings = []

        ###        with open(job.stdout) as log:
        ###            #Search for the regexp and count of many ANS1809W we have; if it is
                    #  the same length
                    #Search for the regexp and if we find any other warnings, return 8,
                    #  otherwise 0
        ###            for line in log:
        ###                match = re.findall(r'ANS[0-9]+W', line)
        ###                if match:
        ###                    warnings.append(match[0])

        ###            for match in warnings:
        ###                if match != "ANS1809W":
        ###                    return arteria_state.ERROR
        ###                else:
        ###                    return arteria_state.DONE

        ###    else:
        ###        return arteria_state.ERROR
        ###else:
        ###    return arteria_status

    # TODO: Perhaps fix this?
    def status_all(self):
        jobs_and_status = {}
        for k, v in self.server.get_status_all().iteritems():
            jobs_and_status[k] = LocalQAdapter.localq2arteria_status(v)
        return jobs_and_status
