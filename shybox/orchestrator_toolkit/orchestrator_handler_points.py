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

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes_upd import PROCESSES as PROCESSES_UPD
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorBase
from shybox.orchestrator_toolkit.lib_orchestrator_utils_workflow import (
    as_list, remove_none, ensure_variables, normalize_deps, count_variables)
from shybox.orchestrator_toolkit.mapper_handler import Mapper, build_pairs_and_process, extract_tag_value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class orchestrator points
class OrchestratorPoints(OrchestratorBase):

    # ------------------------------------------------------------------------------------------------------------------
    # class method ts discharge
    @classmethod
    def points(
            cls,
            data_package_in: Union[DataLocal, dict, list], data_package_out: Union[DataLocal, dict, list] = None,
            data_ref: Union[dict, DataLocal, None] = None,
            priority: list = None,
            configuration: dict = None,
            tag_orchestrator: str = 'generic', tag_logger: str = 'OrchestratorGeneric',
            logger: LoggingManager = None) -> "Orchestrator":

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

        # method to remap variable tags, in and out
        workflow_mapper = Mapper(data_collections_in, data_collections_out, data_count=data_count_code, logger=logger)

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

        # define variables_mapping_in (datasets)
        variables_mapping_in = _check_method_mapping(
            workflow_fx=workflow_fx,
            data_collections=data_collections_in,
            mapping_key='datasets', mandatory=True,
        )

        # define variables_mapping_out (results)
        datasets_mapping_out = _check_method_mapping(
            workflow_fx=workflow_fx,
            data_collections=data_collections_out,
            mapping_key='results', mandatory=False
        )

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorBase(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=deps_collections_in, deps_out=deps_collections_out,
            args_in=args_collections_in, args_out=args_collections_out,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflow_configuration = workflow_mapper.get_rows_by_priority(priority_vars=priority, field="tag", )
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

                # get process name, datasets and object
                process_fx_name = process_fx_tmp.pop("function")
                process_fx_datasets = process_fx_tmp.pop("datasets", None)
                process_fx_obj = PROCESSES_UPD[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}

                # add the process to the workflow
                workflow_common.add_process(
                    process_fx_obj, datasets=process_fx_datasets, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag="ow")

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [{tag_orchestrator}] ... DONE', tag="ow")

        return workflow_common
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check methods data args
def _check_method_mapping(
        workflow_fx,data_collections,
        mapping_key='datasets', mandatory=True,
        logger: LoggingManager = None):

    # define logger (local or external)
    logger = logger or LoggingManager(name="_check_method_mapping")

    # check mapping key value
    if mapping_key not in ['datasets', 'results']:
        raise RuntimeError(f"Mapping key '{mapping_key}' is invalid. Select 'datasets' or 'results'.")

    # initialize collection
    dataset_summary = {}
    # iterate over workflows
    for wf_ref, wf_opts in workflow_fx.items():

        # get function
        wf_function = next((item["function"] for item in wf_opts if "function" in item), None)
        # get selected mapping: datasets or results
        wf_mapping = next((item[mapping_key] for item in wf_opts if mapping_key in item), None)

        # skip if function or mapping is not defined
        if wf_function is None or wf_mapping is None:
            continue

        # initialize workflow/function
        dataset_summary[wf_ref] = {}
        dataset_summary[wf_ref][wf_function] = {"mapping": {}, "valid": True}
        # shortcuts
        function_summary = dataset_summary[wf_ref][wf_function]
        function_mapping = function_summary["mapping"]

        # collect available variables
        variables_available = []
        # iterate over mapping pairs
        for dataset_key, dataset_value in wf_mapping.items():

            # initialize dataset as unresolved
            function_mapping[dataset_key] = None

            # iterate over data collections
            for data_key, data_obj in data_collections.items():

                # get workflow and variable
                data_workflow, data_variable = data_key.split(":", 1)

                # use only data related to current workflow
                if data_workflow != wf_ref:
                    continue

                # normalize data objects
                if isinstance(data_obj, (list, tuple)):
                    data_list = data_obj
                else:
                    data_list = [data_obj]

                # iterate over data objects
                for step_data in data_list:

                    # get available variables
                    step_vars_obj = step_data.variable_template["vars_data"]

                    # select input or output variable namespace
                    if mapping_key == "datasets":
                        step_vars_list = list(step_vars_obj.values())

                    elif mapping_key == "results":
                        step_vars_list = list(step_vars_obj.keys())

                    else:
                        raise ValueError(
                            f"Mapping key '{mapping_key}' is not supported. "
                            f"Expected 'datasets' or 'results'."
                        )

                    # collect available variables
                    for step_var in step_vars_list:
                        if step_var not in variables_available:
                            variables_available.append(step_var)

                    # check dataset availability
                    if dataset_value in step_vars_list:
                        function_mapping[dataset_key] = dataset_value
                        break

                # stop collection search if found
                if function_mapping[dataset_key] is not None:
                    break

        # check function validity
        is_valid = all(
            dataset_value is not None
            for dataset_value in function_mapping.values()
        )

        # store validity
        function_summary["valid"] = is_valid

        # check unresolved mappings
        if not is_valid:

            # unresolved argument keys
            missing_keys = [
                dataset_key
                for dataset_key, dataset_value in function_mapping.items()
                if dataset_value is None
            ]

            # unresolved variable names declared in JSON
            missing_variables = [
                wf_mapping[dataset_key]
                for dataset_key in missing_keys
            ]

            # variables declared in datasets/results
            variables_declared = list(wf_mapping.values())

            # warning message
            warning_msg = (
                f"Workflow '{wf_ref}', function '{wf_function}' has unresolved "
                f"'{mapping_key}'. "
                f"Missing variables: {missing_variables}. "
                f"Variables declared in '{mapping_key}': {variables_declared}. "
                f"Variables available from data collections: {variables_available}. "
                f"Check and edit the '{mapping_key}' mapping in the JSON configuration."
            )

            # always warn
            logger.warning(warning_msg)

            # raise only if mandatory
            if mandatory:
                logger.error(f'Define dataset mapping is mandatory for {mapping_key}.')
                raise RuntimeError(warning_msg)

    return dataset_summary
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
