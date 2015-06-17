# ALSStratificiationChallenge
This is the project for the scoring process for the DREAM ALS Stratification Challenge.  Requires the following parameters, 
which can either be included as a properties file on the runtime classpath or as a string of runtime ("-D") parameters:

ADMIN_USERNAME=xxxx
ADMIN_PASSWORD=xxxx
SCORING_DATA_ID=synxxxx
EVALUATION_ID=xxxx
DOCKER_COMMAND=/usr/local/bin/docker
# 600 sec or 10 min
MAX_SCRIPT_EXECUTION_TIME_MILLIS=600000
DOCKER_HOST=tcp://192.168.59.103:2376
DOCKER_CERT_PATH=<HOME>/.boot2docker/certs/boot2docker-vm
DOCKER_TLS_VERIFY=1
# this is where .dockercfg is found
HOME_DIR=<HOME>
INPUT_FILE_NAME=in.txt
# http://stackoverflow.com/questions/23439126/how-to-mount-host-directory-in-docker-container
USING_BOOT2_DOCKER=true
