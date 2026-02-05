# ---------------------------------------------------------
# Libraries
import os
import sys
import subprocess
import logging

from datetime import datetime
# ---------------------------------------------------------

# ---------------------------------------------------------
# 0. Configure logging (console + file)
# ---------------------------------------------------------
log_file = "app_entrypoint.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),   # console
        logging.FileHandler(log_file, mode="w")  # log file
    ]
)
logger = logging.getLogger(__name__)

logger.info(" --> ENTRYPOINT APP ... START")

# ---------------------------------------------------------
# 1. Define required environment variables
# ---------------------------------------------------------
logger.info(" ---> Defined required environment variables ... ")

required_vars = [
    "APP_MAIN", "APP_CONFIG",
    "TIME_PERIOD", "TIME_RUN", "TIME_RESTART",
    "PATH_GEO", "PATH_RESTART",
    "PATH_SRC", "PATH_DST", "DOMAIN_NAME",
    "PATH_TMP", "PATH_LOG", "PATH_INFO"
]
logger.info(" ---> Defined required environment variables ... DONE")

# ---------------------------------------------------------
# 2. Check all required environment variables are set
# ---------------------------------------------------------
logger.info(" ---> Check required environment variables ... ")

missing = [var for var in required_vars if os.environ.get(var) is None]
if missing:
    logger.info(" ---> Check required environment variables ... FAILED")
    logger.error(f" ===> Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

logger.info(" ---> Check required environment variables ... DONE")

# ---------------------------------------------------------
# 3. Load values from environment (with defaults if needed)
# ---------------------------------------------------------
logger.info(" ---> Get APP_MAIN and APP_CONFIG ... ")

app_main = os.environ.get("APP_MAIN", "default_main.py").strip("'\"")
app_config = os.environ.get("APP_CONFIG", "default_config.yaml").strip("'\"")
logger.info(f" :: APP_MAIN = {app_main}")
logger.info(f" :: APP_CONFIG = {app_config}")

logger.info(" ---> Get APP_MAIN and APP_CONFIG ... DONE")

# ---------------------------------------------------------
# 4. Validate TIME_RUN and TIME_RESTART as datetime
# ---------------------------------------------------------
logger.info(" ---> Get TIME_START ... ")
try:
    time_run_str = os.environ["TIME_RUN"].strip("'\"")
    time_run = datetime.strptime(time_run_str, "%Y-%m-%d %H:%M")
    logger.info(f" :: TIME_RUN = {time_run}")
except (KeyError, ValueError):
    logger.info(" ---> Get TIME_START ... FAILED")
    logger.error(" ===> TIME_RUN must be set and must be a valid datetime in YYYY-MM-DD HH:MM format")
    sys.exit(1)
logger.info(" ---> Get TIME_START ... DONE")

logger.info(" ---> Get TIME_RESTART ... ")
try:
    time_restart_str = os.environ["TIME_RESTART"].strip("'\"")
    time_restart = datetime.strptime(time_restart_str, "%Y-%m-%d %H:%M")
    logger.info(f" :: TIME_RESTART = {time_run}")
except (KeyError, ValueError):
    logger.info(" ---> Get TIME_RESTART ... FAILED")
    logger.error(" ===> TIME_RESTART must be set and must be a valid datetime in YYYY-MM-DD HH:MM format")
    sys.exit(1)
logger.info(" ---> Get TIME_RESTART ... DONE")

logger.info(" ---> Get TIME_PERIOD ... ")
try:
    time_period = int(os.environ["TIME_PERIOD"])
    logger.info(f" :: TIME_PERIOD = {time_period}")
except (KeyError, ValueError):
    logger.info(" ---> Get TIME_PERIOD ... FAILED")
    logger.error(" ===> TIME_PERIOD must be set and must be in integer format")
    sys.exit(1)
logger.info(" ---> Get TIME_PERIOD ... DONE")

# ---------------------------------------------------------
# 5. Check APP_MAIN exists on disk
# ---------------------------------------------------------
#if not os.path.exists(app_main):
#    logger.error(f"APP_MAIN not found: {app_main}")
#    sys.exit(1)
#logger.info("APP_MAIN exists on disk.")

# ---------------------------------------------------------
# 6. Print summary of environment configuration (for debugging)
# ---------------------------------------------------------
logger.info(" ---> Summary of environment variables ... ")
for var in required_vars:
    logger.info(f" :: {var} = {os.environ.get(var)}")
logger.info(" ---> Summary of environment variables ... DONE")

# ---------------------------------------------------------
# 7. Run the main application with command-line arguments
#    - Stream output to console + log
# ---------------------------------------------------------
logger.info(" ---> Run application ... ")

try:
    process = subprocess.Popen(
        [
            sys.executable,       # use current Python interpreter
            app_main,
            "-settings_file", app_config,
            "-time", str(time_run)
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Stream line-by-line to both console and log file
    for line in process.stdout:
        logger.info(line.strip())

    process.wait()
    if process.returncode != 0:
        logger.error(f" ===> Application failed with exit code {process.returncode}")
        sys.exit(process.returncode)

    logger.info(" ---> Run application ... DONE")

except Exception as e:
    logger.exception(f" ===> Run application ... FAILED with error {e}")
    sys.exit(1)

logger.info(" --> ENTRYPOINT APP ... END")
