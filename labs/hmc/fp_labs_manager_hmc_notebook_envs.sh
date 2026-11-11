#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='FP LABS - MANAGER - NOTEBOOK ENVS - HMC'
script_version="1.2.0"
script_date='2022/09/26'

script_folder_labs_root=$HOME'/fp_labs_apps/'
script_folder_labs_library_generic=$HOME'/fp_labs_apps/library/common/'
script_folder_labs_library_hmc=$HOME'/fp_labs_apps/library/jupyter/'

### DEBUG START
#script_folder_labs_root='/home/fabio/Desktop/PyCharm_Notebook/fp-labs-hmc-master/'
#script_folder_labs_library_generic='/home/fabio/Desktop/PyCharm_Notebook/fp-labs-hmc-master/library/common/'
#script_folder_labs_library_hmc='/home/fabio/Desktop/PyCharm_Notebook/fp-labs-hmc-master/library/jupyter/'
### DEBUG END

fp_conda_folder=$HOME'/fp_system_conda/'
fp_conda_libs='fp_system_conda_jupyter_labs_libraries'

# Command-lines:
cmd_runner_jupyter="jupyter-lab"
cmd_installer_hmc="git clone https://github.com/c-hydro/hmc.git --branch v3.1.5 --single-branch $script_folder_labs_library_hmc"

# Jupyter lab interactive graph:
# https://towardsdatascience.com/how-to-produce-interactive-matplotlib-plots-in-jupyter-environment-1e4329d71651
# NodeJS=12
# https://github.com/jupyterlab/jupyterlab/issues/7526
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
# Check jupyter-lab env installation
echo " ====> CHECK JUPYTER-LAB ENV INSTALLATION ..."
if ! type "$cmd_runner_jupyter" > /dev/null; then
    echo " ====> CHECK JUPYTER-LAB ENV INSTALLATION ... FAILED. TRY TO INSTALL THE JUPYTER-LAB IN THE VIRTUAL ENVIRONMENT"
    echo " =====> INSTALL JUPYTER-LAB ENV ... "
    #conda install -y -n $fp_conda_libs -c conda-forge cartopy=0.17
    conda install -y -n $fp_conda_libs -c conda-forge jupyterlab
    conda install -y -n $fp_conda_libs -c conda-forge ipywidgets
    conda install -y -n $fp_conda_libs -c conda-forge ipympl
    conda install -y -n $fp_conda_libs -c conda-forge nodejs=12
    jupyter labextension install @jupyter-widgets/jupyterlab-manager
    jupyter lab build
    echo " =====> INSTALL JUPYTER-LAB ENV ... DONE"
else
    echo " ====> CHECK JUPYTER-LAB ENV INSTALLATION ... DONE"
fi
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Check jupyter-lab generic library installation
echo " ====> CHECK JUPYTER GENERIC LIBRARY INSTALLATION ..."

if [ -d "$script_folder_labs_library_generic" ]; then
  echo " ====> CHECK JUPYTER GENERIC LIBRARY INSTALLATION ... DONE"
else
  echo " ====> CHECK JUPYTER GENERIC LIBRARY INSTALLATION ... FAILED IN $script_folder_labs_library_generic. EXIT WITH ERROR(S)"
  exit 1
fi
# ----------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------
# Check jupyter-lab generic library installation
echo " ====> CHECK JUPYTER HMC LIBRARY INSTALLATION ..."

if [ -d "$script_folder_labs_library_hmc" ]; then
  echo " ====> CHECK JUPYTER HMC LIBRARY INSTALLATION ... DONE"
else
  echo " ====> CHECK JUPYTER HMC LIBRARY INSTALLATION ... NOT AVAILABLE. TRY TO DOWNLOAD FROM GITHUB REPOSITORY"
  echo " =====> INSTALL JUPYTER HMC LIBRARY ... "
  ${cmd_installer_hmc}
  echo " =====> INSTALL JUPYTER HMC LIBRARY ... "
fi
# ----------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------
# Run jupiter-lab
echo " ====> RUN JUPYTER NOTEBOOK ... "
${cmd_runner_jupyter}
echo " ====> RUN JUPYTER NOTEBOOK ... DONE"
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script end
echo " ===> EXECUTION ... DONE"
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------



