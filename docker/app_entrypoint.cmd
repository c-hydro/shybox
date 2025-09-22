# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build libs
DOCKER_BUILDKIT=1 docker build --progress=plain --no-cache -f Dockerfile --target app-builder-libs -t libs-builder:dev .

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build runner
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-runner -t runner:dev .

# docker volume(s) -- chmod 777 mode
mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/data/
mkdir -p /home/fabio/Desktop/shybox/dset/case_study_destine/results/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/data/
chmod 777 /home/fabio/Desktop/shybox/dset/case_study_destine/results/

# docker volume(s) -- chown + chmod mode
chown -R 1456:1456 /home/fabio/Desktop/shybox/dset/case_study_destine/
chmod -R u+rwX,go-rwx /home/fabio/Desktop/shybox/dset/case_study_destine/

docker volume rm case_study_in
docker volume create --name case_study_in \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/data/ \
  --opt o=bind

docker volume rm case_study_out 
docker volume create --name case_study_out \
  --opt type=none \
  --opt device=/home/fabio/Desktop/shybox/dset/case_study_destine/results/ \
  --opt o=bind
  
# docker run using bash (for debugging)
docker run -it \
  --entrypoint /bin/bash \
  -e TIME_RUN='2021-11-26 00:00' \
  -e TIME_RESTART='2021-11-25 23:00' \
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  -e APP_CONFIG='/app/execution/app_runner_workflow_hmc_marche.json' \
  --env-file app_entrypoint.env \
  -v /home/fabio/Desktop/shybox/docker/app_runner_workflow_hmc_marche.json:/app/execution/app_runner_workflow_hmc_marche.json \
  -v case_study_in:/app/mnt_in/:rw \
  -v case_study_out:/app/mnt_out/:rw \
  runner:dev

# docker run using entrypoint
docker run -it \
  -e TIME_RUN='2021-11-26 00:00' \
  -e TIME_RESTART='2021-11-25 23:00' \
  -e TIME_PERIOD=2 \
  -e DOMAIN_NAME='marche' \
  -e APP_CONFIG='/app/execution/app_runner_workflow_hmc_marche.json' \
  --env-file app_entrypoint.env \
  -v /home/fabio/Desktop/shybox/docker/app_runner_workflow_hmc_marche.json:/app/execution/app_runner_workflow_hmc_marche.json \
  -v case_study_in:/app/mnt_in/:rw \
  -v case_study_out:/app/mnt_out/:rw \
  runner:dev

