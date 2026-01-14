"""
Class Features

Name:          orchestrator_handler_timeseries
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260114'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries

from __future__ import annotations

from copy import deepcopy
from typing import Union

from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.orchestrator_toolkit.orchestrator_handler_base import (
    OrchestratorBase, as_list, remove_none, ensure_variables, normalize_deps)
from shybox.orchestrator_toolkit.mapper_handler import Mapper, build_pairs_and_process, extract_tag_value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class orchestrator grid
class OrchestratorTimeSeries(OrchestratorBase):

    #def grouping_tag(self) -> str:
    #    return "reference"

    # ------------------------------------------------------------------------------------------------------------------
    # class method ts discharge
    @classmethod
    def time_series_discharge(
            cls,
            data_package_in: Union[DataLocal, dict, list], data_package_out: Union[DataLocal, dict, list] = None,
            data_ref: DataLocal = None,
            priority: list = None,
            configuration: dict = None, logger: LoggingManager = None) -> "Orchestrator":

        # define logger (local or external)
        logger = logger or LoggingManager(name="OrchestratorTimeSeries")

        # info orchestrator start
        logger.info_up('Organize orchestrator [time-series] ...', tag="ow")

        # get workflow functions and options
        workflow_fx = configuration.get("process_list", None)
        workflow_options = configuration.get("options", [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # normalize input/output data packages
        if not isinstance(data_package_in, list):
            data_package_in = [data_package_in]
        if not isinstance(data_package_out, list):
            data_package_out = [data_package_out]

        # ensure data collections in
        fx_collections = {}
        if isinstance(data_package_in, list):

            # iterate over data package in
            data_collections_in = {}
            for data_id, data_obj in enumerate(data_package_in):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                # iterate over variable tags and processes
                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_name = extract_tag_value(fx_collections[var_tag], 'function')
                        for tmp_process in var_process:
                            tmp_name = tmp_process['function']
                            if tmp_name not in fx_name:
                                fx_collections[var_tag].append(tmp_process)

                    if var_tag not in data_collections_in:
                        data_collections_in[var_tag] = {}
                        data_collections_in[var_tag] = [data_obj]
                    else:
                        data_collections_in[var_tag].append(data_obj)

        else:
            logger.error('Data package in must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections in and workflow have the same keys
        check_variables_in = ensure_variables(data_collections_in, fx_collections, mode='strict')
        if not check_variables_in:
            logger.error(
                'Input data collections do not cover the workflow variables as defined by the check rule.')
            raise RuntimeError(
                'Input data collections do not cover the workflow variables as defined by the check rule.')

        # ensure data collections out
        if isinstance(data_package_out, list):

            # iterate over data package out
            data_collections_out = {}
            for data_id, data_obj in enumerate(data_package_out):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                for step_variable, step_namespace in zip(file_variable, file_namespace):

                    # build pairs tag and process
                    pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                        workflow_fx, step_variable, step_namespace)

                    # iterate over variable tags and processes
                    for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                        if var_tag not in fx_collections:
                            fx_collections[var_tag] = {}
                            fx_collections[var_tag] = [var_process]
                        else:

                            fx_name = extract_tag_value(fx_collections[var_tag], 'function')
                            for tmp_process in var_process:
                                tmp_name = tmp_process['function']
                                if tmp_name not in fx_name:
                                    fx_collections[var_tag].append(tmp_process)

                        if var_tag not in data_collections_out:
                            data_collections_out[var_tag] = {}
                            data_collections_out[var_tag] = [data_obj]
                        else:
                            data_collections_out[var_tag].append(data_obj)
        else:
            logger.error('Data package out must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections and workflow have the same keys
        check_variables_out = ensure_variables(data_collections_out, fx_collections, mode='lazy')
        if not check_variables_out:
            logger.error(
                'Output data collections do not cover the workflow variables as defined by the check rule.')
            raise RuntimeError(
                'Output data collections do not cover the workflow variables as defined by the check rule.')


        # method to remap variable tags, in and out
        workflow_mapper = Mapper(data_collections_in, data_collections_out, logger=logger)

        # organize deps collections in
        deps_collections_in, args_collections_in = {}, {}
        for data_key, data_config in data_collections_in.items():

            configs, is_sequence = as_list(data_config)

            data_args_common, args_deps_common = {}, {}
            data_deps_step, args_deps_step = {}, {}
            for cfg in configs:

                data_deps_step = getattr(cfg, "file_deps", {})
                data_deps_step = normalize_deps(data_deps_step)

                args_deps_step = getattr(cfg, "args_deps", {})
                args_deps_step = normalize_deps(args_deps_step)

            if not data_args_common:
                data_args_common = data_deps_step
            else:
                data_args_common = {
                    key: value for key, value in data_args_common.items()
                    if key in data_deps_step and data_deps_step[key] == value
                }
            if not args_deps_common:
                args_deps_common = args_deps_step
            else:
                args_deps_common = {
                    key: value for key, value in args_deps_common.items()
                    if key in args_deps_step and args_deps_step[key] == value
                }

            if not is_sequence:
                deps_collections_in[data_key] = data_args_common
                args_collections_in[data_key] = args_deps_common
            else:
                deps_collections_in[data_key] = data_args_common
                args_collections_in[data_key] = args_deps_common

        # organize deps collections out
        deps_collections_out, args_collections_out = {}, {}
        for data_key, data_config in data_collections_out.items():

            configs, is_sequence = as_list(data_config)

            data_args_common, args_deps_common = {}, {}
            data_deps_step, args_deps_step = {}, {}
            for cfg in configs:
                data_deps_step = getattr(cfg, "file_deps", {})
                data_deps_step = normalize_deps(data_deps_step)

                args_deps_step = getattr(cfg, "args_deps", {})
                args_deps_step = normalize_deps(args_deps_step)

            if not data_args_common:
                data_args_common = data_deps_step
            else:
                data_args_common = {
                    key: value for key, value in data_args_common.items()
                    if key in data_deps_step and data_deps_step[key] == value
                }
            if not args_deps_common:
                args_deps_common = args_deps_step
            else:
                args_deps_common = {
                    key: value for key, value in args_deps_common.items()
                    if key in args_deps_step and args_deps_step[key] == value
                }

            if not is_sequence:
                deps_collections_out[data_key] = data_args_common
                deps_collections_out[data_key] = args_deps_common
            else:
                deps_collections_out[data_key] = data_args_common
                deps_collections_out[data_key] = args_deps_common

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorBase(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=deps_collections_in, deps_out=deps_collections_out,
            args_in=args_collections_in, args_out=args_collections_out,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflow_configuration = workflow_mapper.get_rows_by_priority(priority_vars=priority, field="tag")
        for workflow_row in workflow_configuration:

            # get workflow information by tag
            workflow_tag = workflow_row["tag"]
            workflow_name = workflow_row["workflow"]

            # info workflow start
            logger.info_up(f'Configure workflow "{workflow_name}" ... ', tag="ow")

            # iterate over the defined process(es)
            process_fx_var = deepcopy(workflow_fx[workflow_tag])
            process_fx_n = len(process_fx_var)
            for process_fx_tmp in process_fx_var:

                # get process name and object
                process_fx_name = process_fx_tmp.pop("function")
                process_fx_obj = PROCESSES[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}

                # add the process to the workflow
                workflow_common.add_process(
                    process_fx_obj, process_n=process_fx_n, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag="ow")

        # info orchestrator end
        logger.info_down('Organize orchestrator [time-series] ... DONE', tag="ow")

        return workflow_common
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
