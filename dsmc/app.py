
from tornado.web import URLSpec as url

from arteria.web.app import AppService

from dsmc.handlers.dsmc_handlers import VersionHandler, UploadHandler, StatusHandler, ReuploadHandler, CreateDirHandler, GenChecksumsHandler#, StopHandler
from dsmc.lib.jobrunner import LocalQAdapter

# TODO: Cleanup comments
# FIXME: Write test cases!!!
# FIXME: Better logging. 
# FIXME: Better documentation. 
# FIXME: Integrate with Arteria workflows. 
# FIXME: Check how many TSM jobs we can queue, and how many can run in 
# parallel. We should define this somewhere. 


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
        url(r"/api/1.0/create_dir/([\w_-]+)", CreateDirHandler, name="createdir", kwargs=kwargs),
        url(r"/api/1.0/gen_checksums/([\w_-]+)", GenChecksumsHandler, name="genchecksums", kwargs=kwargs)
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
