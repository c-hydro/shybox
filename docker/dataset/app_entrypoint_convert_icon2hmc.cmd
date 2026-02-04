## --------------------------------------------------------------------------------------------------
## BUILD DOCKERS
# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build converter [NO-CACHE]
DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain -f Dockerfile --target app-converter -t converter:dev .

# docker build converter [ONLY CONVERTER]
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-converter -t converter:dev .
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## ENV VARIABLES
# environmental variables
DOMAIN_NAME='marche';
TIME_START='2024-10-17 06:00';
TIME_END='2024-10-17 06:00';
TIME_PERIOD=5;
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/icon/';
PATH_DST='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/data/icon/';
PATH_LOG='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/log/icon/';
PATH_TMP='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/tmp/icon/';

## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## VOLUMES
# docker volume(s) -- chmod 777 mode

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/icon/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/icon/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/icon/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/icon/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/icon/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/icon/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/icon/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/icon/

# volume geo
docker volume rm case_study_destine_icon_geo
docker volume create --name case_study_destine_icon_geo \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/ \
  --opt o=bind
  
docker volume inspect case_study_destine_icon_geo

# volume in
docker rm $(docker ps -aq --filter volume=case_study_destine_icon_in)
docker volume rm case_study_destine_icon_in

docker volume create --name case_study_destine_icon_in \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/icon/ \
  --opt o=bind

docker volume inspect case_study_destine_icon_in

# volume out
docker volume rm case_study_destine_icon_out 
docker volume create --name case_study_destine_icon_out \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/icon/ \
  --opt o=bind
  
docker volume inspect case_study_destine_icon_out  

# volume log
docker volume rm case_study_destine_icon_log 
docker volume create --name case_study_destine_icon_log \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/icon/ \
  --opt o=bind
  
docker volume inspect case_study_destine_icon_log

# volume tmp
docker volume rm case_study_destine_icon_tmp 
docker volume create --name case_study_destine_icon_tmp \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/icon/ \
  --opt o=bind
  
docker volume inspect case_study_destine_icon_tmp
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## DOCKER EXECUTION(S)  
# docker run using bash (for debugging)
docker run -it \
  --entrypoint /bin/bash \
  -e TIME_START='2024-10-17 06:00' \
  -e TIME_END='2024-10-17 06:00'\
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  --env-file app_entrypoint_converter_icon2hmc.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_converter_workflow_hmc_destine_icon.json:/app/execution/app_converter_workflow_hmc_destine_icon.json \
  -v case_study_destine_icon_geo:/app/mnt_geo/:rw \
  -v case_study_destine_icon_in:/app/mnt_in/:rw \
  -v case_study_destine_icon_out:/app/mnt_out/:rw \
  -v case_study_destine_icon_log:/app/mnt_log/:rw \
  -v case_study_destine_icon_tmp:/app/mnt_tmp/:rw \
  converter:dev

# docker run using entrypoint
docker run -it \
  -e TIME_START='2024-10-17 06:00' \
  -e TIME_END='2024-10-17 06:00'\
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  --env-file app_entrypoint_converter_icon2hmc.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_converter_workflow_hmc_destine_icon.json:/app/execution/app_converter_workflow_hmc_destine_icon.json \
  -v case_study_destine_icon_geo:/app/mnt_geo/:rw \
  -v case_study_destine_icon_in:/app/mnt_in/:rw \
  -v case_study_destine_icon_out:/app/mnt_out/:rw \
  -v case_study_destine_icon_log:/app/mnt_log/:rw \
  -v case_study_destine_icon_tmp:/app/mnt_tmp/:rw \
  converter:dev
## --------------------------------------------------------------------------------------------------

