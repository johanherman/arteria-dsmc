
from tornado.web import URLSpec as url

from arteria.web.app import AppService

from dsmc.handlers.dsmc_handlers import VersionHandler, UploadHandler, StatusHandler, ReuploadHandler#, StopHandler
from dsmc.lib.jobrunner import LocalQAdapter

# NB: Remember that the service must be able to run on both biotanks and Irma.
# I.e. when constructing the archive dir different filters will have to be used.
#
# FIXME: Write test cases!!!
# FIXME: Better logging. 
# FIXME: Better documentation. 
# FIXME: Integrate with Arteria workflows. 
# FIXME: Check how many TSM jobs we can queue, and how many can run in 
# parallel. We should define this somewhere. 
# TODO: On Irma we should upload everything under /proj/ngi2016001/nobackup/NGI/ANALYSIS/<PROJECT ID>
#       with the exception of BAM files. So we need to filter these somehow. 

# TODO: We need 1b on Irma, because we can't execute the scripts there otherwise. 
#       But 2-4 is a bit more uncertain. It depends on what exactly we are uploading
#       from Irma. Could perhaps be that we can continue to use the scripts locally
#       for now, if they are not needed on Irma. If that is the case then we it is 
#       better do start working on readbacks and removal service after we are done with 1b. 
#

#### DONE ####

### Step 1. Only wrap around command <-- WORKS.
# "dsmc archive <path to runfolder_archive>/ -subdir=yes -description=`uuidgen`" 
#
### Step 1b. Handle archive of specific files.<-- WORKS
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
#
### Step 1c. Don't exit with an error return code for certain TSM warnings. <-- WORKS
# E.g. ANS1809W which happens for temporary connection errors. And retry
# until it works (until a certain timeout). 
#

# I would probably implement these as separate endpoints, to simplify things. 
# Otherwise I have to figure out how to handle many long running tasks after each other. 
#
# There is an argument for implementing these things in the service anyways, 
# because it will make things more robust, instead of having to trust the shell scripts. 
#
# Step 2 is not needed on biotanks, but for Irma we need to filter BAM files somehow. 
# Step 3 is not needed on biotanks, but PERHAPS we need to compress them on Irma, depending on how many files there are
# Step 4 is not needed on biotanks, but we must generate checksums on Irma somehow. 

# For Irma: 
# 2: Either create an archive dir with symlinks (combination of create-archive-dir and create-symlinked-dirs), 
#    or ignore the files with TSM filter. Best would be if we use the same system as on biotanks though. 
# 3: Best case, nothing to do. But if we start implementing archive dirs in the service, 
#    then we might want to include the compression inside the service as well. 
#    If that is the case, check compress-archive-package.sh
# 4: Perhaps best to modify arteria-checksum? So we can generate it with that. 

#### LEFT TO DO ####
#
# Step 2? Create the archive dir to upload
# I.e. lift in the create_archive_dir.py functionality to the service.
# Might have to check create_symlinked_runfolders.sh for Irma as well.
#
# Step 3? Compress the archive dir to upload
# I.e. port compress_archive_package.sh to the service
#
# Step 4? Generate checksums for the archive dir

def routes(**kwargs):
    """
    Setup routes and feed them any kwargs passed, e.g.`routes(config=app_svc.config_svc)`
    Help will be automatically available at /api, and will be based on the
    doc strings of the get/post/put/delete methods
    :param: **kwargs will be passed when initializing the routes.
    """

    return [
        url(r"/api/1.0/version", VersionHandler, name="version", kwargs=kwargs),
        url(r"/api/1.0/upload/([\w_-]+)", UploadHandler, name="start", kwargs=kwargs),
        url(r"/api/1.0/status/(\d*)", StatusHandler, name="status", kwargs=kwargs),
        url(r"/api/1.0/reupload/([\w_-]+)", ReuploadHandler, name="reupload", kwargs=kwargs),
        #url(r"/api/1.0/stop/([\d|all]*)", StopHandler, name="stop", kwargs=kwargs),
    ]

def start():
    """
    Start the dsmc-ws app
    """

    app_svc = AppService.create(__package__)

    number_of_cores_to_use = app_svc.config_svc["number_of_cores"]
    whitelist = app_svc.config_svc["whitelisted_warnings"]
    runner_service = LocalQAdapter(nbr_of_cores=number_of_cores_to_use, whitelisted_warnings=whitelist, interval=2, priority_method="fifo")

    app_svc.start(routes(config=app_svc.config_svc, runner_service = runner_service))
