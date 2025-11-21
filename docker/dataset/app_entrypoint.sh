#!/usr/bin/env bash
set -e

if [ "$ENV_MODE" = "app" ]; then
    echo " ---> START ENTRYPOINT PYTHON "
    exec python app_entrypoint.py "$@"
elif [ "$ENV_MODE" = "bash" ]; then
    echo " ---> START ENTRYPOINT BASH "
    exec /bin/bash -l
else
    echo " ===> EXIT: UNKNOWN ENV_MODE: $ENV_MODE"
    exit 1
fi
