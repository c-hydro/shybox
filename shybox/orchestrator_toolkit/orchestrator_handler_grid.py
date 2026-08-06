"""
Class Features

Name:          orchestrator_handler_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

from copy import deepcopy

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_on_demand import DataOnDemand
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorBase
from shybox.orchestrator_toolkit.lib_orchestrator_utils_workflow import (
    as_list, remove_none, ensure_variables, ensure_workflows)
from shybox.orchestrator_toolkit.mapper_handler import Mapper, build_pairs_and_process, extract_tag_value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class orchestrator grid
class OrchestratorGrid(OrchestratorBase):

    # ------------------------------------------------------------------------------------------------------------------
    # class method multi times
    @classmethod
    def multi_time(cls,
                   data_package_in: (dict, list), data_package_out: (DataLocal, dict, list) = None, data_ref: DataLocal = None,
                   priority: list = None,
                   configuration: dict = None, logger: LoggingManager = None ) -> 'Orchestrator':

        # initialize multi-tile orchestrator
        class_obj = cls.multi_tile(
            data_package_in=data_package_in, data_package_out=data_package_out,
            data_ref=data_ref, configuration=configuration,
            priority=priority, logger=logger, description='multi_time')

        return class_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class method multi tiles
    @classmethod
    def multi_tile(cls,
                   data_package_in: (dict, list), data_package_out: (DataLocal, dict, list) = None,
                   data_ref: (DataLocal, DataOnDemand) = None,
                   priority: list = None,
                   configuration: dict = None, logger: LoggingManager = None,
                   description: str = 'multi_tile') -> 'Orchestrator':

        # define logger (local or external)
        logger = logger or LoggingManager(name="OrchestratorGrid")

        # info orchestrator start
        logger.info_up(f'Organize orchestrator [{description}] ...', tag='ow')

        # get workflow functions and options
        workflow_fx = configuration.get('process_list', None)
        workflow_options = configuration.get('options', [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # normalize input/output data packages
        if not isinstance(data_package_in, list):
            data_package_in = [data_package_in]
        if not isinstance(data_package_out, list):
            data_package_out = [data_package_out]

        # get and ensure workflow functions (check data package in and out and workflows fx)
        ensure_workflows(data_package_in, data_package_out, workflow_fx)

        # ensure data collections in
        if isinstance(data_package_in, list):

            # iterate over data package in
            fx_collections, data_collections_in = {}, {}
            for data_id, data_obj in enumerate(data_package_in):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_collections[var_tag].append(var_process)

                    if var_tag not in data_collections_in:
                        data_collections_in[var_tag] = {}
                        data_collections_in[var_tag] = [data_obj]
                    else:
                        data_collections_in[var_tag].append(data_obj)

        else:
            logger.error('Data package in must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections in and workflow have the same keys
        assert data_collections_in.keys() == fx_collections.keys(), \
            'Data collections and workflow functions must have the same keys.'

        # ensure data collections out
        if isinstance(data_package_out, list):

            # iterate over data package out
            fx_collections, data_collections_out = {}, {}

            for data_id, data_obj in enumerate(data_package_out):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_collections[var_tag].append(var_process)

                    if var_tag not in data_collections_out:
                        data_collections_out[var_tag] = {}
                        data_collections_out[var_tag] = [data_obj]
                    else:
                        data_collections_out[var_tag].append(data_obj)
        else:
            logger.error('Data package out must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections and workflow have the same keys
        assert data_collections_out.keys() == fx_collections.keys(), \
            'Data collections out and workflow functions must have the same keys.'

        # method to remap variable tags, in and out
        workflow_mapper = Mapper(data_collections_in, data_collections_out)

        # organize deps collections in
        deps_collections_in, args_collections_in = {}, {}
        for data_key, data_config in data_collections_in.items():

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:

                if len(deps_list) == 1:
                    deps_collections_in[data_key] = deps_list[0]
                    args_collections_in[data_key] = args_list[0]
                elif len(deps_list) > 1:
                    deps_collections_in[data_key] = deps_list
                    args_collections_in[data_key] = args_list
            else:
                deps_collections_in[data_key] = deps_list
                args_collections_in[data_key] = args_list

        # organize deps collections out
        deps_collections_out, args_collections_out = {}, {}
        for data_key, data_config in data_collections_out.items():

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:
                if len(deps_list) == 1:
                    deps_collections_out[data_key] = deps_list[0]
                    args_collections_out[data_key] = args_list[0]
                elif len(deps_list) > 1:
                    deps_collections_out[data_key] = deps_list
                    args_collections_out[data_key] = args_list
            else:
                deps_collections_out[data_key] = deps_list
                args_collections_out[data_key] = args_list

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorBase(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in = deps_collections_in, deps_out = None,
            args_in=None, args_out=None,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflow_configuration = workflow_mapper.get_rows_by_priority(priority_vars=priority, field='tag')
        for workflow_row in workflow_configuration:

            # get workflow information by tag
            workflow_tag, workflow_name = workflow_row['tag'], workflow_row['workflow']

            # info workflow start
            logger.info_up(f'Configure workflow "{workflow_name}" ... ', tag='ow')

            # iterate over the defined process(es)
            process_fx_var = deepcopy(workflow_fx[workflow_tag])
            for process_fx_id, process_fx_tmp in enumerate(process_fx_var):

                # get process name and object
                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}
                # add the process to the workflow
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag='ow')

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [{description}] ... DONE', tag='ow')

        return workflow_common
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class method multi variable
    @classmethod
    def multi_variable(cls,
                       data_package_in: (dict, list), data_package_out: DataLocal = None, data_ref: DataLocal = None,
                       priority: list = None,
                       configuration: dict = None, logger: LoggingManager = None,
                       description: str = 'multi_variable') -> 'Orchestrator':

        # define logger (local or external)
        logger = logger or LoggingManager(name="OrchestratorGrid")

        # info orchestrator start
        logger.info_up(f'Organize orchestrator [{description}] ...', tag='ow')

        # get workflow functions and options
        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # normalize input/output data packages
        if not isinstance(data_package_in, list):
            data_package_in = [data_package_in]
        if not isinstance(data_package_out, list):
            data_package_out = [data_package_out]

        # get and ensure workflow functions (check data package in and out and workflows fx)
        workflows_checks = ensure_workflows(data_package_in, data_package_out, workflow_fx, show_report=True)

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

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:
                deps_collections_in[data_key] = deps_list[0]
                args_collections_in[data_key] = args_list[0]
            else:
                deps_collections_in[data_key] = deps_list
                args_collections_in[data_key] = args_list

        # organize deps collections out
        deps_collections_out, args_collections_out = {}, {}
        for data_key, data_config in data_collections_out.items():

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:
                deps_collections_out[data_key] = deps_list[0]
                args_collections_out[data_key] = args_list[0]
            else:
                deps_collections_out[data_key] = deps_list
                args_collections_out[data_key] = args_list

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorBase(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=deps_collections_in, deps_out=deps_collections_out,
            args_in=args_collections_in, args_out=args_collections_out,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflow_configuration = workflow_mapper.get_rows_by_priority(priority_vars=priority, field='tag')
        for workflow_row in workflow_configuration:

            # get workflow information by tag
            workflow_tag = workflow_row['tag']
            workflow_name = workflow_row['workflow']

            # info workflow start
            logger.info_up(f'Configure workflow "{workflow_name}" ... ', tag='ow')

            # iterate over the defined process(es)
            process_fx_var = deepcopy(workflow_fx[workflow_tag])
            for process_fx_id, process_fx_tmp in enumerate(process_fx_var):

                # get process name and object
                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}
                # add the process to the workflow
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag='ow')

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [{description}] ... DONE', tag='ow')

        return workflow_common
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
