## --------------------------------------------------------------------------------------------------
## BUILD DOCKERS
# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build converter [NO-CACHE]
DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain -f Dockerfile --target app-analyzer -t analyzer:dev .

# docker build converter [ONLY ANALYZER]
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-analyzer -t analyzer:dev .
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## ENV VARIABLES
# environmental variables
TIME_RUN="2025-09-10 07:34";
TIME_PERIOD=1;
DOMAIN_NAME='LiguriaDomain';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/analyzer_hmc/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/analyzer_hmc/data/';
PATH_DST='/home/fabio/Desktop/shybox/exec/case_study_destine/analyzer_hmc/data/';
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_destine/analyzer_hmc/tmp/;
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_destine/analyzer_hmc/log/;
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## VOLUMES
# docker volume(s) -- chmod 777 mode

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/analyzer_hmc/geo/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/analyzer_hmc/geo/

# volume geo
docker volume rm case_study_analyzer_ts_geo
docker volume create --name case_study_analyzer_ts_geo \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/analyzer_hmc/geo/ \
  --opt o=bind
  
docker volume inspect case_study_analyzer_ts_geo
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## DOCKER EXECUTION(S)  
# docker run using bash (for debugging)
docker run -it \
  --entrypoint /bin/bash \
  -e TIME_START='2025-09-10 07:34' \
  -e TIME_END='2025-09-10 07:34'\
  -e TIME_PERIOD=1 \
  -e DOMAIN_NAME='LiguriaDomain' \
  --env-file app_entrypoint_analyzer_ts.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_analyzer_workflow_hmc_time_series_discharge.json:/app/execution/app_analyzer_workflow_hmc_time_series.json \
  -v case_study_analyzer_ts_geo:/app/mnt_geo/:rw \
  -v case_study_analyzer_ts_in:/app/mnt_in/:rw \
  -v case_study_analyzer_ts_out:/app/mnt_out/:rw \
  -v case_study_analyzer_ts_log:/app/mnt_log/:rw \
  -v case_study_analyzer_ts_tmp:/app/mnt_tmp/:rw \
  analyzer:dev

# docker run using entrypoint
docker run -it \
  -e TIME_START='2025-09-10 07:34' \
  -e TIME_END='2025-09-10 07:34'\
  -e TIME_PERIOD=1 \
  -e DOMAIN_NAME='LiguriaDomain' \
  --env-file app_entrypoint_analyzer_ts.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_analyzer_workflow_hmc_time_series_discharge.json:/app/execution/app_analyzer_workflow_hmc_time_series.json \
  -v case_study_analyzer_ts_geo:/app/mnt_geo/:rw \
  -v case_study_analyzer_ts_in:/app/mnt_in/:rw \
  -v case_study_analyzer_ts_out:/app/mnt_out/:rw \
  -v case_study_analyzer_ts_log:/app/mnt_log/:rw \
  -v case_study_analyzer_ts_tmp:/app/mnt_tmp/:rw \
  analyzer:dev
## --------------------------------------------------------------------------------------------------

