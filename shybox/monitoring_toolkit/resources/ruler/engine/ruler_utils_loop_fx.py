"""
RULER LIBRARY - UTILS - Loop process

__date__ = '20221227'
__version__ = '1.0.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org)'
__library__ = 'ruler'

General command line:
python ruler_utils_loop_fx.py

Version(s):
20221227 (1.0.0) --> Beta release based on https://github.com/giampaolo/psutil
"""
#######################################################################################
# Libraries
import time
from datetime import datetime
#######################################################################################

# -------------------------------------------------------------------------------------
# Algorithm information
project_name = 'RULER'
alg_name = 'LOOP PROCESS UTILS'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2022-12-27'

time_format = '%Y-%m-%d %H:%M'
memory_test = False
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# process to execute process in a while loop
def main():
    time_sleep = 10
    memory_eat = []
    while True:

        # define time now - start
        time_now = datetime.now()
        time_run = time_now.strftime(time_format)
        print(' ---> Time ' + time_run + ' ... ')

        # define time start
        time_start = time.time()

        # simulate memory consumption
        if memory_test:
            memory_eat.append(' ' * 2)
            print(' ----> Memory eat ... "' + str(len(memory_eat)) + ' LENGTH"')

        print(' ----> Sleep time ... "' + str(time_sleep) + ' SECONDS"')
        time.sleep(time_sleep)

        # define time elapsed
        time_elapsed = round(time.time() - time_start, 1)
        print(' ----> Elapsed time ... "' + str(time_elapsed) + ' SECONDS"')

        # define time now - end
        print(' ---> Time ' + time_run + ' ... DONE')
    # -------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
