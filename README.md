Arteria DSMC
=================

A self contained (Tornado) REST service that wraps IBM's TSM backup/archive client dsmc. 

Trying it out
-------------
    
    # install dependencies
    pip install -r requirements/prod .
    

Try running it:

     dsmc-ws --config config/ --port 8080 --debug

And then you can find a simple api documentation by going to:

    http://localhost:8888/api/1.0


REST endpoints
--------------

# FIXME: Update example
Start dsmc by:

    curl -X POST -w '\n' --data '{"path_to_md5_sum_file": "<path_to_checksum_file>"}' http://localhost:8080/api/1.0/start/<runfolder>
    
Please note that it's necessary for the file containing the md5sums to be placed within the runfolder you want to 
test.


You can build check the status of your job by using:
 
     curl -w '\n' http://localhost:8080/api/1.0/status/<jobid or all>
     
And you can stop a job by:

    curl -w '\n' http://localhost:8080/api/1.0/stop/<jobid or all>
    
Finally if you want to know the version of the service running:

    curl -w '\n' http://localhost:8080/api/1.0/version