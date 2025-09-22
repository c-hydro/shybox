# Docker Workflow for Case Study Runner

## 1. Build the Image
```bash
DOCKER_BUILDKIT=1 docker build \
  --progress=plain \
  -f Dockerfile \
  --target app-runner \
  -t runner:dev .
```

---

## 2. Prepare Host Directories
Create input/output directories on the host. Adjust ownership to match the container’s runtime UID/GID.

```bash
mkdir -p /path/to/case_study/data
mkdir -p /path/to/case_study/results
chown -R 1000:1000 /path/to/case_study
chmod -R u+rwX,go-rwx /path/to/case_study
```

---

## 3. Create Bind-Backed Volumes
```bash
docker volume create --name case_study_in \
  --opt type=none \
  --opt device=/path/to/case_study/data \
  --opt o=bind
  
docker volume create --name case_study_out \
  --opt type=none \
  --opt device=/path/to/case_study/results \
  --opt o=bind
```

---

## 4. Environment File
Define base variables in `app_entrypoint.env`:

```env
TIME_RUN=2021-11-26 00:00
TIME_RESTART=2021-11-25 23:00
TIME_PERIOD=5
DOMAIN_NAME=marche
APP_CONFIG=/app/execution/app_runner_workflow_hmc.json
TZ=Europe/Rome
```

---

## 5. Run Options

> **Note on precedence:** environment variables provided with `-e` override values in `--env-file` if both are set.

### Option A: Debug Mode (bash shell)
```bash
docker run -it \
  --entrypoint /bin/bash \
  -e TIME_RUN='2021-11-26 00:00' \
  -e TIME_RESTART='2021-11-25 23:00' \
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  -e APP_CONFIG='/app/execution/app_runner_workflow_hmc.json' \
  --env-file app_entrypoint.env \
  -v /path/to/app_runner_workflow_hmc.json:/app/execution/app_runner_workflow_hmc.json:ro \
  -v case_study_in:/app/mnt_in:rw \
  -v case_study_out:/app/mnt_out:rw \
  runner:dev
```

### Option B: Normal Mode (use image entrypoint)
```bash
docker run -it \
  -e TIME_RUN='2021-11-26 00:00' \
  -e TIME_RESTART='2021-11-25 23:00' \
  -e TIME_PERIOD=5 \
  -e DOMAIN_NAME='marche' \
  -e APP_CONFIG='/app/execution/app_runner_workflow_hmc.json' \
  --env-file app_entrypoint.env \
  -v /path/to/app_runner_workflow_hmc.json:/app/execution/app_runner_workflow_hmc.json:ro \
  -v case_study_in:/app/mnt_in:rw \
  -v case_study_out:/app/mnt_out:rw \
  runner:dev
```

---

## 6. Debugging Tips
Inside the container (debug mode):
```bash
env | sort | head -n 20
ls -al /app/mnt_in /app/mnt_out
test -r /app/execution/app_runner_workflow_hmc.json && echo "Config OK"
```
