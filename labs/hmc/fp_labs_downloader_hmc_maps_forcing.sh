#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='FP LABS - DOWNLOADER - MAPS FORCING - HMC'
script_version="1.2.0"
script_date='2022/09/26'

script_folder_labs=$HOME'/fp_labs_apps/'

### DEBUG START
#script_folder_labs='/home/fabio/Desktop/PyCharm_Notebook/fp-labs-hmc-master/'
### DEBUG END

fp_conda_folder=$HOME'/fp_system_conda/'
fp_conda_libs='fp_system_conda_jupyter_labs_libraries'

# Command-lines:
cmd_runner_script="fp_labs_downloader_hmc_maps.py"
cmd_runner_settings='fp_labs_downloader_hmc_maps_forcing_20221121_1200.json'
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate virtualenv
export PATH=$fp_conda_folder/bin:$PATH
source activate $fp_conda_libs

# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder_labs"
#-----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."
echo " ===> EXECUTION ..."

time_now=$(date -d "$time_now" +'%Y-%m-%d %H:00')
# ----------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------
# Check root library installation
echo " ====> CHECK ROOT LIBRARY INSTALLATION ..."

if [ -d "$script_folder_labs" ]; then
  echo " ====> CHECK ROOT LIBRARY INSTALLATION ... DONE"
else
  echo " ====> CHECK ROOT LIBRARY INSTALLATION ... FAILED IN $script_folder_labs. EXIT WITH ERROR(S)"
  exit 1
fi
# ----------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------
# Run downloader script
echo " ====> RUN DOWNLOADER SCRIPT ... "
python3 ${cmd_runner_script} -settings_file ${cmd_runner_settings}
echo " ====> RUN DOWNLOADER SCRIPT ... DONE"
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script end
echo " ===> EXECUTION ... DONE"
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------



