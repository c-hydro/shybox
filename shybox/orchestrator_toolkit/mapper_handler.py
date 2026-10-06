"""
Class Features

Name:          mapper_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '202600903'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from collections.abc import Mapping as AbcMapping

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class Mapper
class Mapper:

    # initialize class
    def __init__(
        self,
        data_collections_in: Mapping[str, Union[Any, List[Any]]],
        data_collections_out: Mapping[str, Union[Any, List[Any]]],
        workflow_collections: Mapping[str, Mapping[str, Mapping[str, Mapping[str, Any]]]],
        data_count: Optional[int] = 1, # remove?
        logger: LoggingManager = None,) -> None:

        self.logger = logger or LoggingManager(name="Mapper")
        self._data_in = data_collections_in
        self._data_out = data_collections_out
        self._workflow = workflow_collections
        self._mapping: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
        self._data_count: int = data_count

    @staticmethod
    def _normalize_variables(mapping: dict) -> dict:
        mapping_flat = {}
        for root_name, mapping_obj in mapping.items():
            # simple variable
            if isinstance(mapping_obj, str):
                mapping_flat[root_name] = mapping_obj
            # structured variable/container
            elif isinstance(mapping_obj, dict):
                for sub_name, variable_name in mapping_obj.items():
                    variable_key = f'{root_name}|{sub_name}'
                    mapping_flat[variable_key] = variable_name

            else:
                raise TypeError(
                    f'Mapping "{root_name}" must be defined by '
                    f'string or dictionary. '
                    f'Found "{type(mapping_obj).__name__}".'
                )

        return mapping_flat

    # ----------------------------------------------------------------------------------------------------------------------
    # method to validate workflow
    def validate_workflows(
            self,
            sep_root: str = '|', sep_sub: str = ':', ) -> Dict[str, List[Dict[str, Any]]]:

        # -------------------------------------------------------------------------
        # get data from commons
        workflow_fx = self._workflow
        data_collections_in = self._data_in
        data_collections_out = self._data_out
        # -------------------------------------------------------------------------

        # -------------------------------------------------------------------------
        # method to search a workflow variable in data collections
        def find_in_collection(
                workflow_name: str, variable_name: str, data_collections: Dict[str, Any],
                mode: Union[str, None] = None, side: Union[str, None] = None,) -> Dict[str, Any]:

            # initialize variable information
            variable_info = {"key_var": None, "dataset": None, "collection": None, "valid": False,}

            # check args
            if mode is None:
                raise RuntimeError("Mode must be specified by string")

            if side is None:
                raise RuntimeError("Side must be specified by string")

            # iterate over collections
            for collection_key, data_objects in data_collections.items():

                # normalize data objects to list
                if not isinstance(data_objects, (list, tuple)):
                    data_objects = [data_objects]

                # iterate over data objects
                for data_obj in data_objects:

                    # get workflow
                    data_workflow = getattr(data_obj, "file_workflow",None,)

                    # skip objects belonging to another workflow
                    if data_workflow != workflow_name:
                        continue

                    # get dataset
                    data_variable = getattr(data_obj, "file_variable", None,)
                    # get variable template
                    variable_template = getattr(data_obj, "variable_template", {},) or {}
                    # get data variables
                    vars_data = variable_template.get("vars_data", {},) or {}

                    # get data deps
                    data_deps = getattr(data_obj, "file_deps", {},)
                    for deps_key, deps_obj in data_deps.items():
                        # get variable template
                        deps_template = getattr(deps_obj, "variable_template", {}, ) or {}
                        # get data variables
                        vars_deps = deps_template.get("vars_data", {}, ) or {}
                        # add deps to variable data
                        vars_data.update(vars_deps)

                    # search workflow variable
                    key_var = next(
                        (variable_key
                         for variable_key, workflow_variable in vars_data.items() if workflow_variable == variable_name),
                        None,
                    )

                    # (1) standard case
                    # variable must exist inside vars_data
                    if key_var is not None:
                        variable_info.update(
                            {"key_var": key_var, "dataset": data_variable, "collection": collection_key, "valid": True,}
                        )
                        return variable_info

                    # (2) special output case
                    # In unique mode an output variable can be new and therefore
                    # not yet available inside vars_data. The output is still valid because the unique collection
                    # identifies the destination dataset.
                    if mode == "unique" and side == "out":
                        variable_info.update(
                            {"key_var": None, "dataset": data_variable, "collection": collection_key, "valid": True,}
                        )

                        return variable_info

            return variable_info

        # method to create input mapping
        def create_mapping_in(
                mapping_type: str = None,
                var_in: Any = None, var_workflow: Any = None, var_fx: Any = None,
                dataset: Any = None, collection: Any = None, valid: Any = None,) -> Dict[str, Any]:

            return {
                "type": mapping_type,
                "var_in": var_in, "var_workflow": var_workflow, "var_fx": var_fx,
                "dataset": dataset, "collection": collection, "valid": valid,
            }

        # method to create output mapping
        def create_mapping_out(
                mapping_type: str = None,
                var_fx: Any = None, var_workflow: Any = None, var_out: Any = None,
                dataset: Any = None, collection: Any = None, valid: Any = None,) -> Dict[str, Any]:

            return {
                "type": mapping_type,
                "var_fx": var_fx, "var_workflow": var_workflow, "var_out": var_out,
                "dataset": dataset, "collection": collection, "valid": valid,
            }

        # initialize workflow summary and errors
        variable_workflow, workflow_errors = {}, {}
        # iterate over workflows
        for workflow_id, (workflow_key, workflow_structure) in enumerate(
                workflow_fx.items(), start=0):

            # check workflow structure
            if not isinstance(workflow_structure, list):
                raise TypeError(f'Workflow "{workflow_key}" must be defined by a list.')

            # initialize workflow collections
            workflow_steps = []
            workflow_errors[workflow_key] = []

            # get number of steps
            n_steps = len(workflow_structure)
            # iterate over methods in declared order
            for step_id, step_obj in enumerate(workflow_structure):

                # get function
                fx_name = step_obj.get("function")

                # get datasets and results
                datasets_raw = step_obj.get("datasets", {}) or {}
                results_raw = step_obj.get("results", {}) or {}

                # normalize datasets and results
                datasets = self._normalize_variables(datasets_raw)
                results = self._normalize_variables(results_raw)

                # identify step position
                is_first = step_id == 0
                is_last = step_id == n_steps - 1

                # define mode for each step
                step_tot = n_steps - 1
                if step_id == 0 == step_tot == 0:
                    mode = 'unique'
                elif step_id == step_tot:
                    mode = 'end'
                elif step_id == 0:
                    mode = 'start'
                else:
                    mode = 'intermediate'

                # define process name
                step_tag = f"workflow.{workflow_key}{sep_root}process.{fx_name}"

                # initialize step summary
                step_summary = {
                    "mode": mode,
                    "workflow_id": workflow_id, "workflow_name": workflow_key,
                    "tag": step_tag, "step": step_id + 1,
                    "function": fx_name,
                    "datasets": datasets, "results": results,
                    "mapping_in": {}, "mapping_out": {},
                    "variables_in_structure": {}, "variables_out_structure": {},
                    "variables_in_left": [], "variables_in_right": [],
                    "variables_out_left": [], "variables_out_left": [],
                    "links": [], "valid": True,
                }

                # FIRST STEP - MAPPING IN
                if is_first:

                    # iterate over datasets
                    for var_id, (key_var_fx, key_var_workflow) in enumerate(
                            datasets.items(), start=1):

                        # define mapping key
                        mapping_key = f"var{var_id}"

                        # search variable in input collections
                        data_info = find_in_collection(
                            workflow_name=workflow_key, variable_name=key_var_workflow,
                            data_collections=data_collections_in,
                            mode=mode, side='in',
                        )

                        # organize input mapping
                        step_summary["mapping_in"][mapping_key] = create_mapping_in(
                            mapping_type="input",
                            var_in=data_info["key_var"],
                            var_workflow=key_var_workflow,
                            var_fx=key_var_fx,
                            dataset=data_info["dataset"],
                            collection=data_info["collection"],
                            valid=data_info["valid"],
                        )

                        # add link
                        if data_info["valid"]:
                            step_summary["links"].append(
                                {
                                    "type": "input",
                                    "collection": data_info["collection"],
                                    "dataset": data_info["dataset"],
                                    "var_in": data_info["key_var"],
                                    "var_workflow": key_var_workflow,
                                    "var_fx": key_var_fx,
                                }
                            )

                    # all first-step variables must be resolved
                    mapping_in_valid = all(
                        mapping_obj["valid"] is True
                        for mapping_obj
                        in step_summary["mapping_in"].values()
                    )

                    # check input mapping
                    if not mapping_in_valid:
                        step_summary["valid"] = False

                        # get unresolved mappings
                        missing_variables = {
                            mapping_key: mapping_obj
                            for mapping_key, mapping_obj
                            in step_summary["mapping_in"].items()
                            if mapping_obj["valid"] is not True
                        }

                        # collect error
                        workflow_errors[workflow_key].append(
                            {
                                "type": "input_mapping",
                                "step": step_id + 1,
                                "function": fx_name,
                                "mapping": missing_variables,
                            }
                        )

                # INTERMEDIATE AND LAST STEPS
                else:

                    # get previous step
                    previous_obj = workflow_structure[step_id - 1]
                    # get previous function
                    previous_fx = previous_obj.get("function")
                    # get previous results
                    previous_results = previous_obj.get("results", {}) or {}

                    # iterate over expected current datasets
                    for var_id, (key_var_fx, key_var_workflow) in enumerate(datasets.items(), start=1):

                        # define mapping key
                        mapping_key = f"var{var_id}"

                        # search workflow variable in previous results
                        previous_key = next(
                            (
                                result_key
                                for result_key, result_value
                                in previous_results.items()
                                if result_value == key_var_workflow
                            ),
                            None,
                        )

                        # check internal link
                        variable_valid = previous_key is not None
                        # define collection and dataset (for intermediate step)
                        variable_collection = f'{workflow_key}{sep_root}{key_var_workflow}'
                        variable_dataset = key_var_workflow

                        # organize mapping
                        step_summary["mapping_in"][mapping_key] = create_mapping_in(
                            mapping_type="workflow",
                            var_in=key_var_workflow, var_workflow=key_var_workflow, var_fx=key_var_fx,
                            dataset=variable_dataset, collection=variable_collection, valid=variable_valid,
                        )

                        # add workflow link
                        if variable_valid:
                            step_summary["links"].append(
                                {
                                    "type": "workflow",
                                    "function_previous": previous_fx,
                                    "var_previous": previous_key,
                                    "var_in": key_var_workflow,
                                    "var_workflow": key_var_workflow,
                                    "var_fx": key_var_fx,
                                }
                            )

                    # all expected datasets must be linked
                    mapping_in_valid = all(
                        mapping_obj["valid"] is True
                        for mapping_obj
                        in step_summary["mapping_in"].values()
                    )

                    # check internal workflow mapping
                    if not mapping_in_valid:
                        step_summary["valid"] = False

                        # get unresolved variables
                        missing_variables = {
                            mapping_key: mapping_obj
                            for mapping_key, mapping_obj
                            in step_summary["mapping_in"].items()
                            if mapping_obj["valid"] is not True
                        }

                        # collect error
                        workflow_errors[workflow_key].append(
                            {
                                "type": "workflow_link",
                                "step_from": step_id, "step_to": step_id + 1,
                                "function_from": previous_fx, "function_to": fx_name,
                                "previous_results": previous_results,
                                "mapping": missing_variables,
                            }
                        )

                # MAPPING OUT
                # define output mapping type
                mapping_out_type = ("output" if is_last else "workflow")

                # iterate over results
                for var_id, (key_var_fx, key_var_workflow) in enumerate(results.items(), start=1):

                    # define mapping key
                    mapping_key = f"var{var_id}"

                    # create mapping
                    step_summary["mapping_out"][mapping_key] = create_mapping_out(
                        mapping_type=mapping_out_type,
                        var_fx=key_var_fx,
                        var_workflow=key_var_workflow,
                        var_out=key_var_workflow,
                        dataset=None,
                        collection=None,
                        valid=(None if is_last else True),
                    )

                # LAST STEP
                if is_last:

                    # iterate over last-step outputs
                    for mapping_key, mapping_obj in (step_summary["mapping_out"].items()):

                        # get workflow variable
                        key_var_workflow = mapping_obj["var_workflow"]

                        # search variable in output collections
                        data_info = find_in_collection(
                            workflow_name=workflow_key,
                            variable_name=key_var_workflow,
                            data_collections=data_collections_out,
                            mode=mode, side='out'
                        )

                        # update mapping
                        mapping_obj.update(
                            {
                                "var_out": key_var_workflow,
                                "dataset": data_info["dataset"], "collection": data_info["collection"],
                                "valid": data_info["valid"],
                            }
                        )

                        # add output link
                        if data_info["valid"]:
                            step_summary["links"].append(
                                {
                                    "type": "output",
                                    "var_fx": mapping_obj["var_fx"], "var_workflow": key_var_workflow,
                                    "var_out": key_var_workflow,
                                    "dataset": data_info["dataset"], "collection": data_info["collection"],
                                }
                            )

                    # at least one result must be available
                    # in output collections
                    mapping_out_valid = any(mapping_obj["valid"] is True for mapping_obj
                                            in step_summary["mapping_out"].values())

                    # check output mapping
                    if not mapping_out_valid:
                        step_summary["valid"] = False

                        # collect error
                        workflow_errors[workflow_key].append(
                            {
                                "type": "output_mapping",
                                "step": step_id + 1, "function": fx_name,
                                "mapping": step_summary["mapping_out"],
                            }
                        )

                # create the variable maps
                mapping_variables = self._mapping_variables(
                    datasets=step_summary['datasets'], results=step_summary['results'])
                step_summary['variables_in_structure'] = mapping_variables['in']
                step_summary['variables_out_structure'] = mapping_variables['out']

                # create the variable names in right/left
                variable_in_left, variable_in_right = self._listing_variables(mapping_variables['in'])
                step_summary['variables_in_left'] = variable_in_left
                step_summary['variables_in_right'] = variable_in_right
                # create the variable names out right/left
                variable_out_left, variable_out_right = self._listing_variables(mapping_variables['out'])
                step_summary['variables_out_left'] = variable_out_left
                step_summary['variables_out_right'] = variable_out_right

                # store current step
                workflow_steps.append(step_summary)

            # store workflow
            variable_workflow[workflow_key] = workflow_steps

        # remove workflows without errors
        workflow_errors = {workflow_key: errors for workflow_key, errors in workflow_errors.items() if errors}
        # block execution if workflows are invalid
        if workflow_errors:

            # initialize messages
            error_messages = []
            # iterate over workflow errors
            for workflow_key, errors in workflow_errors.items():

                # iterate over errors
                for error in errors:

                    # input mapping error
                    if error["type"] == "input_mapping":

                        missing_msg = "; ".join(
                            (
                                f'{mapping_key}: '
                                f'"{mapping_obj["var_fx"]}" -> '
                                f'"{mapping_obj["var_workflow"]}"'
                            )
                            for mapping_key, mapping_obj in error["mapping"].items()
                        )

                        error_messages.append(
                            f'Workflow "{workflow_key}", '
                            f'Step {error["step"]} '
                            f'({error["function"]}): '
                            f'input variable(s) not found in '
                            f'data_collections_in: {missing_msg}'
                        )

                    # internal workflow error
                    elif error["type"] == "workflow_link":

                        missing_msg = "; ".join(
                            (
                                f'{mapping_key}: '
                                f'"{mapping_obj["var_fx"]}" expects '
                                f'"{mapping_obj["var_workflow"]}"'
                            )
                            for mapping_key, mapping_obj in error["mapping"].items()
                        )

                        # available previous results
                        previous_msg = ", ".join(
                            (
                                f'"{result_key}" -> '
                                f'"{result_value}"'
                            )
                            for result_key, result_value
                            in error["previous_results"].items()
                        )

                        error_messages.append(
                            f'Workflow "{workflow_key}": '
                            f'Step {error["step_from"]} '
                            f'({error["function_from"]}) -> '
                            f'Step {error["step_to"]} '
                            f'({error["function_to"]}) is not linked. '
                            f'Missing: {missing_msg}. '
                            f'Available previous results: {previous_msg}'
                        )

                    # output mapping error
                    elif error["type"] == "output_mapping":

                        output_msg = "; ".join(
                            (
                                f'{mapping_key}: '
                                f'"{mapping_obj["var_fx"]}" -> '
                                f'"{mapping_obj["var_out"]}"'
                            )
                            for mapping_key, mapping_obj in error["mapping"].items()
                        )

                        error_messages.append(
                            f'Workflow "{workflow_key}", '
                            f'Step {error["step"]} '
                            f'({error["function"]}): '
                            f'none of the function outputs are available in '
                            f'data_collections_out: {output_msg}'
                        )

            # raise error
            raise RuntimeError("Workflow variables are not consistently linked:\n ".join(error_messages))

        return variable_workflow

    # ----------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    # method to collect workflow by priority
    def order_workflows_by_priority(
            self,
            priority: Optional[List[str]] = None,
            workflows: Optional[Dict[str, Any]] = None,
            sort_others: bool = True,) -> Dict[str, Any]:

        # get workflows if not provided
        if workflows is None:
            workflows = self.validate_workflows(sep_root='|',sep_sub=':',)

        # no priority defined
        if not priority:
            return workflows

        # normalize priority keys
        priority_keys = [str(workflow) for workflow in priority]

        # split workflows
        priority_workflows, other_workflows = [], []
        for workflow_key in workflows.keys():

            workflow_name = str(workflow_key)

            if workflow_name in priority_keys:
                priority_workflows.append(workflow_name)
            else:
                other_workflows.append(workflow_name)

        # sort priority workflows according to requested order
        priority_workflows.sort(key=lambda workflow_name: priority_keys.index(workflow_name))

        # optionally sort remaining workflows
        if sort_others:
            other_workflows.sort(key=str)

        # merge workflow order
        workflow_order = (priority_workflows+ other_workflows)
        # rebuild ordered dictionary
        workflows_sorted = {workflow_key: workflows[workflow_key] for workflow_key in workflow_order}

        return workflows_sorted
    # ------------------------------------------------------------------------------------------------------------------

    # method to create map of the variables
    @staticmethod
    def _mapping_variables(
            datasets: dict,
            results: dict) -> dict:

        # initialize mapping object
        mapping_variables = {
            'in': {'groups': {}},
            'out': {'groups': {}},
            'valid': True,
        }

        # ---------------------------------------------------------------------------------
        # organize INPUT variables
        for group_name, group_obj in datasets.items():

            # initialize group
            group_info = {
                'behaviour': None,
                'mapping': {},
            }

            # case 1: one variable
            #
            # "data": "air_temperature"
            if isinstance(group_obj, str):

                group_info['behaviour'] = 'one_to_one'
                group_info['mapping'][group_name] = group_obj

            # case 2: structured/grouped variables
            #
            # "data": {
            #     "data_1": "discharge_sim_openloop",
            #     "data_2": "discharge_sim_analysis"
            # }
            #
            # or:
            #
            # "data": {
            #     "collections": "soil_moisture"
            # }
            elif isinstance(group_obj, dict):

                if not group_obj:
                    mapping_variables['valid'] = False

                else:

                    group_info['mapping'] = dict(group_obj)

                    # collection of the same logical variable
                    if (
                            len(group_obj) == 1
                            and 'collections' in group_obj
                    ):
                        group_info['behaviour'] = 'collections'

                    # multiple variables packed into one fx argument
                    elif len(group_obj) > 1:
                        group_info['behaviour'] = 'multi_to_one'

                    # explicit single nested variable
                    else:
                        group_info['behaviour'] = 'one_to_one'

            else:
                mapping_variables['valid'] = False
                continue

            # store group
            mapping_variables['in']['groups'][group_name] = group_info

        # ---------------------------------------------------------------------------------
        # organize OUTPUT variables
        for group_name, group_obj in results.items():

            # initialize group
            group_info = {
                'behaviour': None,
                'mapping': {},
            }

            # case 1: one returned object -> one workflow variable
            #
            # "data": "air_temperature"
            if isinstance(group_obj, str):

                group_info['behaviour'] = 'one_to_one'
                group_info['mapping'][group_name] = group_obj

            # case 2: one returned container with internal variables
            #
            # "data": {
            #     "sim": "discharge_sim",
            #     "obs": "discharge_obs"
            # }
            #
            # or:
            #
            # "data": {
            #     "collections": "soil_moisture"
            # }
            elif isinstance(group_obj, dict):

                if not group_obj:
                    mapping_variables['valid'] = False

                else:

                    group_info['mapping'] = dict(group_obj)

                    # repeated objects representing the same variable
                    if (
                            len(group_obj) == 1
                            and 'collections' in group_obj
                    ):
                        group_info['behaviour'] = 'collections'

                    # one returned object containing multiple variables
                    elif len(group_obj) > 1:
                        group_info['behaviour'] = 'one_to_multi'

                    # explicit nested single variable
                    else:
                        group_info['behaviour'] = 'one_to_one'

            else:
                mapping_variables['valid'] = False
                continue

            # store group
            mapping_variables['out']['groups'][group_name] = group_info

        # ---------------------------------------------------------------------------------
        # final checks

        if not mapping_variables['in']['groups']:
            mapping_variables['valid'] = False

        if not mapping_variables['out']['groups']:
            mapping_variables['valid'] = False

        return mapping_variables

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to create map of the variables
    @staticmethod
    def _mapping_variables_OLD(
            datasets: dict, results: dict,
            root_separator: str = '|', variable_separator: str = ':') -> dict:

        # initialize mapping object
        mapping_variables = {'in': {'groups': {},}, 'out': {'groups': {},}, 'valid': True,}

        # organize input groups
        for dataset_key, dataset_name in datasets.items():

            # split group and variables
            if root_separator in dataset_key:
                try:
                    group_name, variable_name = dataset_key.split(root_separator, 1)
                except ValueError:
                    raise ValueError(f'Dataset key "{dataset_key}" must have format "group{root_separator}variable".')
            else:
                group_name, variable_name = dataset_key, dataset_name

            # initialize input group
            if group_name not in mapping_variables['in']['groups']:
                mapping_variables['in']['groups'][group_name] = {'behaviour': None, 'mapping': {},}

            # store variable mapping
            mapping_variables['in']['groups'][group_name]['mapping'][dataset_key] = dataset_name

        # define input group behaviours
        for group_info in mapping_variables['in']['groups'].values():

            # get number of variables
            n_variables = len(group_info['mapping'])
            # define behaviour
            if n_variables == 1:
                group_info['behaviour'] = 'one_to_one'
            elif n_variables > 1:
                group_info['behaviour'] = 'multi_to_one'
            else:
                group_info['behaviour'] = None
                mapping_variables['valid'] = False

        # create input variable lookup
        input_lookup = {}
        for group_info in mapping_variables['in']['groups'].values():

            for dataset_key, workflow_variable in group_info['mapping'].items():

                # check duplicated variable
                if dataset_key in input_lookup:
                    if input_lookup[dataset_key] != workflow_variable:
                        mapping_variables['valid'] = False
                # store variable
                input_lookup[dataset_key] = workflow_variable

        # organize output groups
        for result_key, result_name in results.items():

            # split group and variables
            if root_separator in result_key:
                try:
                    group_name, variables_name = result_key.split(root_separator, 1)
                except ValueError:
                    raise ValueError(f'Result key "{result_key}" must have format "group{root_separator}variable".')
            else:
                group_name, variables_name = result_key, result_name

            # split output variables
            variables_name = [variable_name
                              for variable_name in variables_name.split(variable_separator) if variable_name
            ]

            # initialize output group
            if group_name not in mapping_variables['out']['groups']:
                mapping_variables['out']['groups'][group_name] = {'behaviour': None, 'mapping': {},}

            # check variables
            if not variables_name:
                mapping_variables['valid'] = False
                continue

            # organize output variables
            for variable_name in variables_name:

                # split group and variables
                if root_separator in result_key:
                    dataset_key = root_separator.join([group_name,variable_name])
                    workflow_variable = input_lookup.get(dataset_key)
                else:
                    dataset_key = group_name
                    workflow_variable = group_name

                # create output reference
                # example: time_series:data_1
                if root_separator in result_key:
                    variable_reference = f'{result_name}{root_separator}{variable_name}'
                else:
                    variable_reference = result_name

                # store output mapping
                mapping_variables['out']['groups'][group_name]['mapping'][workflow_variable] = variable_reference

                # variable is not linked to an input variable
                if workflow_variable is None:
                    mapping_variables['valid'] = False

        # define output group behaviours
        for group_info in mapping_variables['out']['groups'].values():

            # get output mapping
            group_mapping = group_info['mapping']

            # check empty mapping
            if not group_mapping:
                group_info['behaviour'] = None
                mapping_variables['valid'] = False
                continue

            # get function result names
            result_names = {variable_reference.split(variable_separator, 1)[0] for variable_reference in group_mapping}

            # get number of results and variables
            n_results = len(result_names)
            n_variables = len(group_mapping)

            # define behaviour
            if n_results == 1 and n_variables == 1:
                group_info['behaviour'] = 'one_to_one'
            elif n_results == 1 and n_variables > 1:
                group_info['behaviour'] = 'one_to_multi'
            elif n_results > 1:
                group_info['behaviour'] = 'multi_to_multi'
            else:
                group_info['behaviour'] = None
                mapping_variables['valid'] = False

        # check input/output definitions
        if not mapping_variables['in']['groups']:
            mapping_variables['valid'] = False

        if not mapping_variables['out']['groups']:
            mapping_variables['valid'] = False

        return mapping_variables

    # method to create list of variables
    @staticmethod
    def _listing_variables(variable_obj: dict) -> tuple[list, list]:

        # initialize lists
        variable_keys, variable_names = [], []

        # get groups
        groups = variable_obj.get('groups', {})
        # iterate over groups
        for group_info in groups.values():
            # get mapping
            mapping = group_info.get('mapping', {})
            # preserve key/value position
            for variable_key, variable_name in mapping.items():
                variable_keys.append(variable_key)
                variable_names.append(variable_name)

        return variable_keys, variable_names

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to get pairs based on reference fields
    def get_pairs_OLD(
            self,
            field_value,
            field_key: str = "fx",
            rows: Optional[list] = None,) -> Union[List[Dict[str, Any]], Dict[str, Any]]:

        # map reference type to row field
        field_map = {
            "tag": "tag",
            "workflow": "workflow_name", "process": "process_name",
            "reference": "reference", "fx": "fx",
        }

        # check type
        if field_key not in field_map:
            raise ValueError(f"Type '{field_key}' is not supported. Expected one of {tuple(field_map.keys())}.")
        field_name = field_map[field_key]

        # get rows
        if rows is None:
            rows = self.compact_rows()

        rows_selected = []
        for row in rows:
            # filter rows by fx, if defined
            if field_name is not None:
                if row[field_name] == field_value:
                    rows_selected.append(row)
            else:
                rows_selected.append(row)

        # define rows for process start and row for process
        if len(rows_selected) == 1:
            return rows_selected[0]
        else:
            if field_key == 'tag':
                return rows_selected
            else:
                raise RuntimeError('Selected process must be singleton at each step')

    # ------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    # helpers
    # method to return list
    @staticmethod
    def _as_list_OLD(obj: Union[Any, List[Any]]) -> List[Any]:
        if obj is None:
            return []
        if isinstance(obj, (list, tuple, set)):
            return list(obj)
        return [obj]

    # method to get attributes or key
    @staticmethod
    def _getattr_or_key_OLD(partial: Any, key: str, default=None):
        if isinstance(partial, (dict, AbcMapping)):
            return partial.get(key, default)
        return getattr(partial, key, default)

    # method to get partial mapping
    def _get_partial_mapping_OLD(self, partial: Any, tag: str, side: str, index_in_tag: int):

        # check side
        if side not in ("in", "out"):
            raise ValueError(f"Unsupported side '{side}'. Expected 'in' or 'out'.")

        # get variables
        file_vars = self._getattr_or_key(partial,"file_variable",None)
        if file_vars is None:
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'file_variable'; skipping.")
            return {}

        # get workflows
        file_workflows = self._getattr_or_key(partial,"file_workflow",None)
        if file_workflows is None:
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'file_workflow'; skipping.")
            return {}

        # get variable template
        variable_template = self._getattr_or_key(partial,"variable_template",None)

        if not isinstance(variable_template, (dict, AbcMapping)):
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'variable_template'; skipping.")
            return {}

        # get vars data
        vars_data = variable_template.get("vars_data", None)
        if not isinstance(vars_data, (dict, AbcMapping)):
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} 'vars_data' is not a mapping; skipping.")
            return {}

        # normalize variables
        if not isinstance(file_vars, (list, tuple)):
            file_vars = [file_vars]

        # normalize workflows
        if not isinstance(file_workflows, (list, tuple)):
            file_workflows = [file_workflows]

        # convert to strings
        file_vars = [str(var) for var in file_vars]
        file_workflows = [str(workflow) for workflow in file_workflows]

        # initialize mapping
        partial_mapping = {}
        # organize mapping
        for variable in file_vars:

            partial_mapping[variable] = {}
            for template_key, template_value in vars_data.items():

                template_key = str(template_key)
                template_value = str(template_value)
                partial_mapping[variable][template_key] = template_value

        return partial_mapping
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract tag value from a list of dicts
def extract_tag_value(data, tag):

    # Normalize to a flat list of dicts
    if isinstance(data, dict):
        data = [data]
    elif isinstance(data, list):
        # Flatten any nested lists
        flat = []
        for item in data:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        data = flat
    else:
        raise TypeError("Input must be a dict or list of dicts (possibly nested).")

    # Extract tag values
    values = [d[tag] for d in data if isinstance(d, dict) and tag in d]

    return values if values else None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to build check variables and processes pairs

# check namespace fields
@with_logger(var_name='logger_stream')
def _ns_has_fields(ns):
    # namespace is not defined
    if ns is None:
        logger_stream.warning("Namespace is not defined.")
        return None
    # variable field is not defined
    if not hasattr(ns, "variable"):
        logger_stream.warning("Namespace does not contain the required field 'variable'.")
        return None
    # workflow field is not defined
    if not hasattr(ns, "workflow"):
        logger_stream.warning("Namespace does not contain the required field 'workflow'.")
        return None
    return ns

@with_logger(var_name='logger_stream')
def build_pairs_and_process(process_list, dataset_namespace,
                            key_separator='|', key_order='workflow_and_variable'):

    # check file variable as a list
    if not isinstance(dataset_namespace, list):
        dataset_namespace = [dataset_namespace]

    # iterate over namespace(s)
    process_selection, process_collections = [], {}
    pairs_list_str, pairs_list_tuple = [], []
    workflow_found, workflow_missed, workflow_collections, workflow_tags = [], [], {}, []
    for i, step_namespace in enumerate(dataset_namespace):

        # check namespace fields
        step_namespace = _ns_has_fields(step_namespace)
        if step_namespace is None:
            logger_stream.error('Namespace obj is defined by NoneType')
            raise TypeError("Namespace obj must be defined as (variable, workflow) pair")

        # get variable and workflow
        step_var, step_workflow = step_namespace.variable, step_namespace.workflow

        # warn if step workflow is not defined in process list
        if step_workflow not in process_list:
            workflow_missed.append(step_workflow)
            logger_stream.warning(
                f"Namespace workflow '{step_workflow}' for variable '{step_var}' not in process_list {process_list} ")
            continue
        else:
            workflow_found.append(step_workflow)

        # get key(s) by a default order
        if key_order == 'workflow_and_variable':
            step_key_str = f"{step_workflow}{key_separator}{step_var}"
            step_key_tuple = (step_workflow, step_var)
        elif key_order == 'variable_and_workflow':
            step_key_str = f"{step_var}{key_separator}{step_workflow}"
            step_key_tuple = (step_var, step_workflow)
        else:
            logger_stream.error('key order in define pairs must be "workflow_and_variable" or "variable_and_workflow"')
            raise NotImplementedError('Case not implemented yet')

        # get process(s)
        step_process = process_list[step_workflow]

        # organize pairs object(s)
        pairs_list_tuple.append(step_key_tuple)
        pairs_list_str.append(step_key_str)
        # organize process object(s)
        process_selection.append(step_process)
        process_collections[step_key_str] = step_process
        # organize workflow object(s)
        workflow_tags.append(step_workflow)
        workflow_collections[step_key_str] = step_workflow

    # pre-check workflows selected in namespaces
    if len(workflow_found) == 0:
        logger_stream.warning("No process workflow is selected in namespaces.")
    elif len(workflow_found) == 1:
        logger_stream.info(f"Process '{workflow_found[0]}' is selected in namespaces.")
    else:
        logger_stream.error(f"Only one process workflow can be selected in namespaces. "
                            f"Found {len(workflow_found)}: {', '.join(workflow_found)}")
        raise RuntimeError("Multiple process workflows are selected in namespaces.")

    # check workflows available in process_list but not used by namespaces
    workflow_unused = []
    for process_workflow in process_list.keys():
        if process_workflow not in workflow_found:
            workflow_unused.append(process_workflow)

    # info object
    info = {
        "available_in_process_list": workflow_found,
        "missing_in_process_list": workflow_missed,
        "extras_in_process_list": workflow_unused,
        "workflow_tags": workflow_tags,
    }

    return pairs_list_str, pairs_list_tuple, process_selection, info
# ----------------------------------------------------------------------------------------------------------------------
