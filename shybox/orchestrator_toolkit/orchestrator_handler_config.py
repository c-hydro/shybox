"""
Class Features

Name:          orchestrator_handler_points
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260114'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

from copy import deepcopy
from typing import Union

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes_OLD import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES as PROCESSES_UPD
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorBase
from shybox.orchestrator_toolkit.lib_orchestrator_utils_workflow import (
    as_list, remove_none, ensure_variables, normalize_deps, count_variables)
from shybox.orchestrator_toolkit.mapper_handler import Mapper, build_pairs_and_process, extract_tag_value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class orchestrator points
class OrchestratorConfig(OrchestratorBase):

    # ------------------------------------------------------------------------------------------------------------------
    # class method ts discharge
    @classmethod
    def configure(
            cls,
            data_package_in: Union[DataLocal, dict, list], data_package_out: Union[DataLocal, dict, list] = None,
            data_ref: Union[dict, DataLocal, None] = None,
            priority: list = None,
            configuration: dict = None,
            tag_orchestrator: str = 'generic', tag_logger: str = 'OrchestratorGeneric',
            logger: LoggingManager = None) -> "OrchestratorConfig":

        # define logger (local or external)
        logger = logger or LoggingManager(name=tag_logger)

        # info orchestrator start
        logger.info_up(f'Organize orchestrator [{tag_orchestrator}] ...', tag="ow")

        # get workflow functions and options
        workflow_fx = configuration.get("process_list", None)
        workflow_options = configuration.get("options", [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # count input/output variables
        data_count_in, data_count_out, data_count_code = count_variables(
            data_package_in, data_package_out, options=workflow_options)

        # normalize input/output data packages
        data_package_in = _normalize_data_package(data_package_in)
        data_package_out = _normalize_data_package(data_package_out)

        # ensure data collections in
        fx_collections = {}
        if isinstance(data_package_in, dict):

            # iterate over data package in
            data_collections_in = {}
            for data_id, (data_key, data_obj) in enumerate(data_package_in.items()):

                # get file variable and namespace
                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_namespace)

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
        if isinstance(data_package_out, dict):

            # iterate over data package out
            data_collections_out = {}
            for data_id, (data_key, data_obj) in enumerate(data_package_out.items()):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_namespace)

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

        # organize deps collections in
        deps_collections_in, args_collections_in = {}, {}
        for data_key, data_config in data_collections_in.items():

            configs, is_sequence = as_list(data_config)

            data_deps_common, args_deps_common = {}, {}
            data_deps_step, args_deps_step = {}, {}
            for cfg in configs:

                data_deps_step = getattr(cfg, "file_deps", {})
                data_deps_step = normalize_deps(data_deps_step)

                args_deps_step = getattr(cfg, "args_deps", {})
                args_deps_step = normalize_deps(args_deps_step)

                data_deps_common.update(data_deps_step)
                args_deps_common.update(args_deps_step)

            if not is_sequence:
                deps_collections_in[data_key] = data_deps_common
                args_collections_in[data_key] = args_deps_common
            else:
                deps_collections_in[data_key] = data_deps_common
                args_collections_in[data_key] = args_deps_common

        # organize deps collections out
        deps_collections_out, args_collections_out = {}, {}
        for data_key, data_config in data_collections_out.items():

            configs, is_sequence = as_list(data_config)

            data_deps_common, args_deps_common = {}, {}
            data_deps_step, args_deps_step = {}, {}
            for cfg in configs:
                data_deps_step = getattr(cfg, "file_deps", {})
                data_deps_step = normalize_deps(data_deps_step)

                args_deps_step = getattr(cfg, "args_deps", {})
                args_deps_step = normalize_deps(args_deps_step)

                data_deps_common.update(data_deps_step)
                args_deps_common.update(args_deps_step)

            if not is_sequence:
                deps_collections_out[data_key] = data_deps_common
                deps_collections_out[data_key] = args_deps_common
            else:
                deps_collections_out[data_key] = data_deps_common
                deps_collections_out[data_key] = args_deps_common

        # method to remap variable tags, in and out
        workflow_mapper = Mapper(
            data_collections_in=data_collections_in, data_collections_out=data_collections_out,
            workflow_collections=workflow_fx,
            data_count=data_count_code, logger=logger)

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorBase(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=deps_collections_in, deps_out=deps_collections_out,
            args_in=args_collections_in, args_out=args_collections_out,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflows_configuration = workflow_mapper.order_workflows_by_priority(priority=priority)

        # iterate over workflow(s)
        for workflow_id, (workflow_name, workflow_obj) in enumerate(workflows_configuration.items()):

            # iterate over process(es)
            for process_id, process_obj in enumerate(workflow_obj):

                # get process and workflows information
                process_fx_name =  process_obj["function"]
                workflow_name, workflow_id = process_obj["workflow_name"], process_obj["workflow_id"]

                # info workflow and process start
                logger.info_up(
                    f'Configure workflow "{workflow_name}" process "{process_fx_name}" ... ', tag="ow")

                # check process in process registry
                if process_fx_name in PROCESSES_UPD:

                    # get process obj
                    process_fx_obj = PROCESSES_UPD[process_fx_name]

                    # define process arguments
                    process_fx_args = {**process_obj}

                    # add the process to the workflow
                    workflow_common.add_process(
                        function_obj=process_fx_obj, ref=data_ref, **process_fx_args)

                    # info workflow and process end (done)
                    logger.info_down(
                        f'Configure workflow "{workflow_name}" process "{process_fx_name}" ... DONE', tag="ow")

                else:

                    # info function end (failed)
                    logger.error(f'Process "{process_fx_name}" not found in {PROCESSES_UPD}"')
                    logger.info_down(
                        f'Configure workflow "{workflow_name}" process "{process_fx_name}" ... FAILED', tag="ow")
                    raise RuntimeError('All process(es) must be correctly configured. Exit')

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [{tag_orchestrator}] ... DONE', tag="ow")

        return workflow_common
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize data package to dictionary (if not)
def _normalize_data_package(data_package_in, default_key='var'):

    # already a dictionary
    if isinstance(data_package_in, dict):
        return data_package_in
    # normalize to list
    if not isinstance(data_package_in, (list, tuple)):
        data_package_in = [data_package_in]

    # convert packages to dictionary
    data_package_out = {}
    for idx, package in enumerate(data_package_in, start=1):
        package_key = f'{default_key}_{idx}'
        data_package_out[package_key] = package

    return data_package_out
# ----------------------------------------------------------------------------------------------------------------------
