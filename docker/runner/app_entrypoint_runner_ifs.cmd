## --------------------------------------------------------------------------------------------------
## BUILD DOCKERS
# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build libs
DOCKER_BUILDKIT=1 docker build --progress=plain --no-cache -f Dockerfile --target app-builder-libs -t libs-builder:dev .

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build runner
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-runner -t runner:dev .
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## IFS ENV VARIABLES
TIME_RUN="2021-11-27 01:23";
TIME_PERIOD=5;
DOMAIN_NAME='marche';
PATH_APP=$HOME/run_base_hmc/exec/;
PATH_GEO=/home/fabio/Desktop/shybox/dset/case_study_destine/runner_hmc/geo/;
PATH_SRC=$HOME/Desktop/shybox/dset/case_study_destine/runner_hmc/data/ifs_nc/;
PATH_DST=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/ifs/results/;
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/ifs/tmp/;
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/ifs/log/;
PATH_INFO=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/ifs/info/;
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## VOLUMES --> EXAMPLE(S) USE TOOLS FOR VOLUMES
# docker volume(s) -- chmod 777 mode
# docker volume(s) -- chmod 777 mode
mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/data/
mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/results/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/data/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/results/


docker volume rm case_study_in
docker volume create --name case_study_in \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/data/ \
  --opt o=bind
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## DOCKER EXECUTION(S)  
# docker run using bash (for debugging)

domain_name='marche'

docker run -it \
	--entrypoint /bin/bash \
	-e TIME_RUN='2025-11-17 00:00' \
	-e TIME_RESTART='2025-11-16 23:00' \
	-e TIME_PERIOD=5 \
	-e DOMAIN_NAME=${domain_name} \
	-e APP_CONFIG=/app/execution/app_runner_workflow_hmc.json \
	--env-file app_entrypoint_runner_ifs.env \
	-v /home/fabio/Desktop/shybox/docker/runner/app_runner_workflow_hmc_${domain_name}.json:/app/execution/app_runner_workflow_hmc.json \
	-v case_study_destine_runner_ifs_geo:/app/mnt_geo/:rw \
	-v case_study_destine_runner_ifs_restart:/app/mnt_restart/:rw \
	-v case_study_destine_runner_ifs_in:/app/mnt_in/:rw \
	-v case_study_destine_runner_ifs_out:/app/mnt_out/:rw \
	-v case_study_destine_runner_ifs_log:/app/mnt_log/:rw \
	-v case_study_destine_runner_ifs_tmp:/app/mnt_tmp/:rw \
	-v case_study_destine_runner_ifs_info:/app/mnt_info/:rw \
  runner:dev

# docker run using entrypoint

domain_name='marche'

docker run -it \
  -e TIME_RUN='2025-11-17 00:00' \
  -e TIME_RESTART='2025-11-16 23:00' \
  -e TIME_PERIOD=24 \
  -e DOMAIN_NAME=${domain_name} \
  -e APP_CONFIG='/app/execution/app_runner_workflow_hmc.json' \
  --env-file app_entrypoint_runner_ifs.env \
  -v /home/fabio/Desktop/shybox/docker/runner/app_runner_workflow_hmc_${domain_name}.json:/app/execution/app_runner_workflow_hmc.json \
	-v case_study_destine_runner_ifs_geo:/app/mnt_geo/:rw \
	-v case_study_destine_runner_ifs_restart:/app/mnt_restart/:rw \
	-v case_study_destine_runner_ifs_in:/app/mnt_in/:rw \
	-v case_study_destine_runner_ifs_out:/app/mnt_out/:rw \
	-v case_study_destine_runner_ifs_log:/app/mnt_log/:rw \
	-v case_study_destine_runner_ifs_tmp:/app/mnt_tmp/:rw \
	-v case_study_destine_runner_ifs_info:/app/mnt_info/:rw \
  runner:dev
## --------------------------------------------------------------------------------------------------
