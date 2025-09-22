# ============================================================
# Stage 0: CLEANER
# 
# Clean all images:
# 	docker system prune -a -f
# ============================================================

# ============================================================
# Stage 1a: Libs BUILDER
#
# Build the debugger image:
# 	DOCKER_BUILDKIT=1 docker build --progress=plain --no-cache -f Dockerfile --target app-builder-libs -t libs-builder:dev .
# ============================================================

# ------------------------------------------------------------
# settings generic
FROM ubuntu:24.04 AS app-builder-libs
ARG DEBIAN_FRONTEND=noninteractive

# set geographical area
ENV TZ=Europe/Rome 
# UID of the non-root user 'app'
ENV APP_UID=1456

# set environment folder(s)
ENV	ENTRYPOINT_DIR=/app/entrypoint/ 

ENV	LIBS_TMP_DIR=/app/library/libs_tmp/ 
ENV	LIBS_INSTALLER_DIR=/app/library/libs_installer/ 
ENV	LIBS_SYSTEM_DIR=/app/library/libs_system/ 
ENV	PACKAGE_HMC_DIR=/app/library/package_hmc/ 

# set environment filename(s)
ENV	SYSTEM_ENV_FILE=fp_env_system 

# prepare share folder(s) to mount external repositories
ENV	INPUT_DIR=/app/mnt_in/ 
ENV	OUTPUT_DIR=/app/mnt_out/ 
ENV	INPUT_STATIC_DIR=/app/mnt_in/data_static/ 
ENV	INPUT_DYNAMIC_DIR=/app/mnt_in/data_dynamic/ 
ENV	INPUT_MODEL_RESTART_DIR=/app/mnt_in/model_restart/ 
ENV	OUTPUT_MODEL_RESULTS_DIR=/app/mnt_out/model_results/ 
ENV	OUTPUT_MODEL_STATE_DIR=/app/mnt_out/model_state/

# install system-wide deps 
RUN apt-get update && apt-get install -y \
	git \
	gfortran \
	gcc \
	m4 \
	g++ \
	make \
	mc \
	curl \
	build-essential \
	wget \
	cmake \
	libcurl4-openssl-dev \
	tzdata \
	openjdk-8-jdk \
	bash-completion

# create a non-root user and group
RUN groupadd \
        --gid=$APP_UID \
        app \
    && useradd -l \
        --uid=$APP_UID \
        --gid=$APP_UID \
        --create-home \
        app  

# activate the application user
WORKDIR /app
RUN chown -R app:app /app
USER app

# create the required folders
RUN mkdir -p ${LIBS_INSTALLER_DIR} \
	&& mkdir -p ${LIBS_TMP_DIR} \
	&& mkdir -p ${PACKAGE_HMC_DIR} \
	&& mkdir -p ${ENTRYPOINT_DIR} \
	&& mkdir -p ${INPUT_DIR} \
	&& mkdir -p ${OUTPUT_DIR} \
	&& mkdir -p ${INPUT_STATIC_DIR} \
	&& mkdir -p ${INPUT_DYNAMIC_DIR} \
	&& mkdir -p ${INPUT_MODEL_RESTART_DIR} \
	&& mkdir -p ${OUTPUT_MODEL_RESULTS_DIR} \
	&& mkdir -p ${OUTPUT_MODEL_STATE_DIR}

# change default shell (from sh to bash)
SHELL ["/bin/bash", "-c"]
# ------------------------------------------------------------

# ------------------------------------------------------------
# INSTALL - SYSTEM LIBRARIES (DEPS)
# remove tmp folder 
RUN rm -rf ${LIBS_TMP_DIR}
# clone library from github repository
RUN git clone https://github.com/c-hydro/fp-system-library.git --branch main --single-branch ${LIBS_TMP_DIR}
# copy files from tmp to container folder(s)
RUN cp -r ${LIBS_TMP_DIR}. ${LIBS_INSTALLER_DIR} 
  
# start installer script for system libraries
WORKDIR ${LIBS_INSTALLER_DIR}
RUN pwd
RUN ./setup_fp_system_library_hmc.sh ${LIBS_SYSTEM_DIR} ${SYSTEM_ENV_FILE}
# ------------------------------------------------------------

# ------------------------------------------------------------
# INSTALL - SYSTEM APPS (HMC)
# remove tmp folder 
RUN rm -rf ${LIBS_TMP_DIR}
# clone library from github repository
RUN git clone https://github.com/c-hydro/fp-system-app.git --depth 1 --branch main --single-branch ${LIBS_TMP_DIR}
# copy files from tmp to container folder(s)
RUN cp -rf ${LIBS_TMP_DIR}. ${LIBS_INSTALLER_DIR} 

# start installer script for hmc libraries
WORKDIR ${LIBS_INSTALLER_DIR}
RUN pwd
RUN ./setup_fp_system_app_hmc.sh ${LIBS_SYSTEM_DIR} ${SYSTEM_ENV_FILE} ${PACKAGE_HMC_DIR}
# ------------------------------------------------------------

# ------------------------------------------------------------
# CLEAN
# remove tmp folder 
RUN rm -rf ${LIBS_TMP_DIR}
# ------------------------------------------------------------

# ============================================================
# Stage 1b: Libs DEBUGGER
# 
# Build the debugger image (NO-CACHE): 
#   DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-debugger-libs -t libs-debugger:dev .
#
# Run interactively:
#   docker run -it --rm libs-debugger:dev
# ============================================================

# ------------------------------------------------------------
# settings generic
FROM app-builder-libs AS app-debugger-libs

# Install ONLY extra debugging tools (need root just for this layer)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      vim nano less htop tree mc \
      iputils-ping net-tools procps lsof strace \
      gdb gdbserver \
      curl wget unzip zip \
      python3-ipython \
    && rm -rf /var/lib/apt/lists/*

# Return to the non-root user defined in app-builder-libs
USER app
WORKDIR /app

# Nice interactive defaults (bash login reads /etc/bash.bashrc and user bashrcs)
CMD ["/bin/bash", "-l"]
# ------------------------------------------------------------


# ============================================================
# Stage 2a: Shybox BUILDER

# Build the debugger image (NO-CACHE):
# 	DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .
#
# Run interactively:
# 	docker run -it --rm shybox-builder:dev bash -l
# ============================================================

# ------------------------------------------------------------
# settings generic
FROM app-builder-libs AS app-builder-shybox
#FROM ubuntu:24.04 AS app-builder-shybox
ARG DEBIAN_FRONTEND=noninteractive

# repository and branch
ARG SHYBOX_REPO=https://github.com/c-hydro/shybox.git
ARG SHYBOX_REF=destine

# UID of the non-root user 'app'
ENV APP_UID=1456

# environment variable(s)
ENV PACKAGE_SHYBOX_DIR="/app/library/package_shybox/" 
ENV CONDA_SHYBOX_NAME="shybox_base" 
ENV CONDA_SHYBOX_LIBS="shybox_base_libraries" 
ENV CONDA_SHYBOX_DIR="/app/library/conda_shybox/" 
ENV PYTHONUNBUFFERED=1

# environment conda
ENV PATH="${CONDA_SHYBOX_DIR}/bin:${PATH}" \
    PYTHONPATH="${PACKAGE_SHYBOX_DIR}:${PYTHONPATH}"

# use bash for consistent conda behavior
SHELL ["/bin/bash", "-lc"]

# base OS deps (only what builder needs)
USER root
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      git curl wget ca-certificates bash bzip2 tini && \
    rm -rf /var/lib/apt/lists/*
USER app

# workspace
RUN mkdir -p "${PACKAGE_SHYBOX_DIR}" "${CONDA_SHYBOX_DIR}"
WORKDIR "${PACKAGE_SHYBOX_DIR}"

# fetch shybox (shallow + submodules)
RUN git init . && \
    git remote add origin "${SHYBOX_REPO}" && \
    git fetch --depth 1 origin "${SHYBOX_REF}" && \
    git checkout -q FETCH_HEAD && \
    git submodule update --init --depth 1

# install conda environment
# Expects script at: ${PACKAGE_SHYBOX_DIR}/setup_conda_shybox_base.sh
RUN "${PACKAGE_SHYBOX_DIR}/setup_conda_shybox_base.sh" \
      "${CONDA_SHYBOX_NAME}" \
      "${CONDA_SHYBOX_DIR}" \
      "${CONDA_SHYBOX_LIBS}"

# install python extras into conda env
RUN conda run -n "${CONDA_SHYBOX_LIBS}" python -m pip install --no-cache-dir --upgrade pip && \
    conda run -n "${CONDA_SHYBOX_LIBS}" pip install --no-cache-dir \
      tabulate rioxarray pyresample repurpose
# ------------------------------------------------------------

# ============================================================
# Stage 2b: Shybox DEBUGGER
# 
# Build the debugger image (NO-CACHE):
#   DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-debugger-shybox -t shybox-debugger:dev .
#
# Run interactively:
#   docker run -it --rm shybox-debugger:dev
# ============================================================

# ------------------------------------------------------------
# settings generic
FROM app-builder-shybox AS app-debugger-shybox

# Install ONLY extra debugging tools (do not duplicate builder deps)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      vim nano less iputils-ping procps htop net-tools \
      lsof strace tree python3-ipython \
      && rm -rf /var/lib/apt/lists/*


# Auto-activate the libraries env for interactive shells
# 1) Source conda shell hook for bash
RUN echo "source \"${CONDA_SHYBOX_DIR}/etc/profile.d/conda.sh\" 2>/dev/null || true" \
      >> /etc/bash.bashrc
# 2) Activate the desired env on login
RUN echo "conda activate ${CONDA_SHYBOX_LIBS} 2>/dev/null || true" \
      >> /etc/bash.bashrc
USER app

# Friendly working dir for exploration
WORKDIR /app

# Default: interactive login shell (bash reads /etc/bash.bashrc)
CMD ["/bin/bash", "-l"]
# ------------------------------------------------------------


# ============================================================
# Stage 2: Runtime
# 
# Build the debugger image (NO-CACHE):
#   DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-runner -t runner:dev .
#
# Run interactively using entrypoint PYTHON:
# 	docker run --rm -it runner:dev bash -l
# Run interactively using entrypoint BASH:
#   docker run --rm -it -e ENV_MODE=bash runner:dev 
# ============================================================

# ------------------------------------------------------------
# Stage aliases that point to your prebuilt images
FROM app-builder-shybox AS app-runner
ARG DEBIAN_FRONTEND=noninteractive

# environmental variable(s)
ENV VAR='ciao'
ENV	EXECUTION_DIR=/app/execution/ 
ENV	DATA_DIR=/app/mnt_data/ 
ARG APP_UID=1456
ARG APP_GID=1456
USER root
RUN groupmod -g ${APP_GID} app || true \
 && usermod -u ${APP_UID} app || true
USER app

# copy libs, variables from images
COPY --from=app-builder-libs ${LIBS_SYSTEM_DIR} ${LIBS_SYSTEM_DIR}
COPY --from=app-builder-libs ${PACKAGE_HMC_DIR} ${PACKAGE_HMC_DIR}
COPY --from=app-builder-libs ${ENTRYPOINT_DIR} ${ENTRYPOINT_DIR} 
COPY --from=app-builder-libs ${INPUT_STATIC_DIR} ${INPUT_STATIC_DIR} 
COPY --from=app-builder-libs ${INPUT_DYNAMIC_DIR} ${INPUT_DYNAMIC_DIR} 

# Minimal OS deps you actually need at runtime
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
	 ca-certificates bash tini \
    && rm -rf /var/lib/apt/lists/*
# change default shell (from sh to bash)
SHELL ["/bin/bash", "-c"]

RUN rm -rf ${INPUT_DYNAMIC_DIR}
RUN rm -rf ${INPUT_STATIC_DIR}

#RUN set -eux; \
#  groupmod -g "${APP_GID}" app || true; \
#  usermod  -u "${APP_UID}" -g "${APP_GID}" app || true
    
# change user (rootless)
USER app

# create folder 
RUN mkdir -p ${EXECUTION_DIR}
RUN mkdir -p ${DATA_DIR}
RUN mkdir -p ${INPUT_STATIC_DIR}
RUN mkdir -p ${INPUT_DYNAMIC_DIR}

# Friendly working dir for exploration
WORKDIR /app
# Copy entrypoint application (owned by app)
COPY --chown=app:app app_entrypoint.py app_entrypoint.py
COPY --chown=app:app app_entrypoint.sh app_entrypoint.sh
RUN chmod +x app_entrypoint.sh

# required for point netcdf libs
ENV LD_LIBRARY_PATH="/app/library/libs_system:/app/library/libs_system/source/nc4_fortran/fortran/.libs/"

# set environmental variable(s) appending in .bashrc
RUN echo "source ${LIBS_SYSTEM_DIR}/${SYSTEM_ENV_FILE}" >> /home/app/.bashrc
# set environmental variable(s) appending in .profile
RUN echo "source ${LIBS_SYSTEM_DIR}/${SYSTEM_ENV_FILE}" >> /home/app/.profile

# Friendly working dir for exploration
USER app
WORKDIR /app
# use Python script as entrypoint
ENTRYPOINT ["python", "app_entrypoint.py"]

# Nice interactive defaults (bash login reads /etc/bash.bashrc and user bashrcs)
# CMD ["/bin/bash", "-l"]
# -----------------------------------------------------------------

