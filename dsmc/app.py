
from tornado.web import URLSpec as url

from arteria.web.app import AppService

from dsmc.handlers.dsmc_handlers import VersionHandler, StartHandler, StatusHandler#, StopHandler
from dsmc.lib.jobrunner import LocalQAdapter

# NB: Remember that the service must be able to run on both biotanks and Irma. 
# I.e. when constructing the archive dir different filters will have to be used. 
# 
# 
# Step 1. Only wrap around command
# "dsmc archive <path to runfolder_archive>/ -subdir=yes -description=`uuidgen`"
#
# Step 1b. Handle archive of specific files.
# I.e. so we can re-upload failed uploads, and also (if we want to), upload only
# certain files (this is a second feature though). 
# 
# Either the service waits and retries until all files have succeeded, or it 
# can reuse pdc-descr.sh, pdc-diff.sh and pdc-upload-missing.sh to upload
# at a later time. 
#
# Step 1c. Don't exit with an error return code for certain TSM warnings. 
# E.g. ANS1809W which happens for temporary connection errors. And retry 
# until it works (until a certain timeout). 
# for, root dir in file. 
# Step 2? Create the archive dir to upload 
# I.e. lift in the create_archive_dir.py functionality to the service. 
# Might have to check create_symlinked_runfolders.sh for Irma as well. 
# 
# Step 3? Compress the archive dir to upload
# I.e. port compress_archive_package.sh to the service
#
# Step 4? Generate checksums for the archive dir
#
# Step X. Implement a service that can do readback tests.
# Step Y. Implement a service that can remove runfolders when necessary
# (e.g. when read backs have been done)
#
# Probably best do have a central SQL server for arteria-dsmc + X + Y.
#  

   

def routes(**kwargs):
    """
    Setup routes and feed them any kwargs passed, e.g.`routes(config=app_svc.config_svc)`
    Help will be automatically available at /api, and will be based on the
    doc strings of the get/post/put/delete methods
    :param: **kwargs will be passed when initializing the routes.
    """

    return [
        url(r"/api/1.0/version", VersionHandler, name="version", kwargs=kwargs),
        url(r"/api/1.0/start/([\w_-]+)", StartHandler, name="start", kwargs=kwargs),
        url(r"/api/1.0/status/(\d*)", StatusHandler, name="status", kwargs=kwargs),
        #url(r"/api/1.0/stop/([\d|all]*)", StopHandler, name="stop", kwargs=kwargs),
    ]

def start():
    """
    Start the dsmc-ws app
    """

    app_svc = AppService.create(__package__)

    number_of_cores_to_use = app_svc.config_svc["number_of_cores"]
    runner_service = LocalQAdapter(nbr_of_cores=number_of_cores_to_use, interval = 2, priority_method = "fifo")

    app_svc.start(routes(config=app_svc.config_svc, runner_service = runner_service))
