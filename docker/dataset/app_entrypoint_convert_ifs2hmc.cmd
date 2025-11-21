## --------------------------------------------------------------------------------------------------
## BUILD DOCKERS
# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build converter
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-converter -t converter:dev .
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## ENV VARIABLES
# environmental variables
DOMAIN_NAME='marche';
TIME_START='2025-11-17 00:00';
TIME_END='2025-11-17 00:00';
TIME_PERIOD=5;
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/ifs/';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/destination/ifs/';
PATH_LOG=/home/fabio/Desktop/shybox/dset/case_study_destine/log/
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## VOLUMES
# docker volume(s) -- chmod 777 mode
mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/ifs/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/ifs/

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/results/ifs/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/results/ifs/

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/

# volume geo
docker volume rm case_study_destine_ifs_geo
docker volume create --name case_study_destine_ifs_geo \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_geo

# volume in
docker rm $(docker ps -aq --filter volume=case_study_destine_ifs_in)
docker volume rm case_study_destine_ifs_in

docker volume create --name case_study_destine_ifs_in \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/ifs/ \
  --opt o=bind

docker volume inspect case_study_destine_ifs_in

# volume out
docker volume rm case_study_destine_ifs_out 
docker volume create --name case_study_destine_ifs_out \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/results/ifs/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_out  
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## DOCKER EXECUTION(S)  
# docker run using bash (for debugging)
docker run -it \
  --entrypoint /bin/bash \
  -e TIME_START='2025-11-17 00:00' \
  -e TIME_END='2025-11-17 00:00'\
  -e TIME_PERIOD=2 \
  -e DOMAIN_NAME='marche' \
  --env-file app_entrypoint_converter_ifs2hmc.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_converter_workflow_hmc_destine_ifs.json:/app/execution/app_converter_workflow_hmc_destine_ifs.json \
  -v case_study_destine_ifs_geo:/app/mnt_geo/:rw \
  -v case_study_destine_ifs_in:/app/mnt_in/:rw \
  -v case_study_destine_ifs_out:/app/mnt_out/:rw \
  converter:dev

# docker run using entrypoint
docker run -it \
  -e TIME_START='2025-11-17 00:00' \
  -e TIME_END='2025-11-17 00:00'\
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  --env-file app_entrypoint_converter_ifs2hmc.env \
  -v /home/fabio/Desktop/shybox/docker/dataset/app_converter_workflow_hmc_destine_ifs.json:/app/execution/app_converter_workflow_hmc_destine_ifs.json \
  -v case_study_destine_ifs_geo:/app/mnt_geo/:rw \
  -v case_study_destine_ifs_in:/app/mnt_in/:rw \
  -v case_study_destine_ifs_out:/app/mnt_out/:rw \
  converter:dev
## --------------------------------------------------------------------------------------------------

