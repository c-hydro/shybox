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
## IFS ENV VARIABLES
DOMAIN_NAME='marche';
TIME_PERIOD=5;
TIME_START='2025-11-17 00:00';
TIME_END='2025-11-17 00:00';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/ifs/';
PATH_DST='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/data/ifs/';
PATH_LOG='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/log/ifs/';
PATH_TMP='/home/fabio/Desktop/shybox/hpc/case_study_destine/converter_hmc/tmp/ifs/';
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## VOLUMES --> EXAMPLE(S) USE TOOLS FOR VOLUMES
# docker volume(s) -- chmod 777 mode

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/

mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/ifs/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/ifs/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/ifs/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/ifs/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/ifs/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/ifs/

mkdir -p /home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/ifs/
chmod 777 /home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/ifs/

# volume geo
docker volume rm case_study_destine_ifs_geo
docker volume create --name case_study_destine_ifs_geo \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/geo/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_geo

# volume in
docker rm $(docker ps -aq --filter volume=case_study_destine_ifs_in)
docker volume rm case_study_destine_ifs_in

docker volume create --name case_study_destine_ifs_in \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc/data/ifs/ \
  --opt o=bind

docker volume inspect case_study_destine_ifs_in

# volume out
docker volume rm case_study_destine_ifs_out 
docker volume create --name case_study_destine_ifs_out \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/data/converter_hmc/ifs/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_out  

# volume log
docker volume rm case_study_destine_ifs_log 
docker volume create --name case_study_destine_ifs_log \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/log/converter_hmc/ifs/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_log

# volume tmp
docker volume rm case_study_destine_ifs_tmp 
docker volume create --name case_study_destine_ifs_tmp \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/hpc/destine/tmp/converter_hmc/ifs/ \
  --opt o=bind
  
docker volume inspect case_study_destine_ifs_tmp
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
  -v case_study_destine_ifs_log:/app/mnt_log/:rw \
  -v case_study_destine_ifs_tmp:/app/mnt_tmp/:rw \
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
  -v case_study_destine_ifs_log:/app/mnt_log/:rw \
  -v case_study_destine_ifs_tmp:/app/mnt_tmp/:rw \
  converter:dev
## --------------------------------------------------------------------------------------------------

