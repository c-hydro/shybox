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

from shybox.orchestrator_toolkit.mapper_linker import Data
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
        fx_collections_in: Union(dict, None) = None,
        fx_collections_out: Union(dict, None) = None,
        data_count: Optional[int] = 1,
        logger: LoggingManager = None,) -> None:

        self.logger = logger or LoggingManager(name="Mapper")
        self._data_in = data_collections_in
        self._data_out = data_collections_out
        self._fx_in = fx_collections_in
        self._fx_out = fx_collections_out
        self._mapping: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
        self._data_count: int = data_count

    # method to build mapping in/out
    def build_mapping(self) -> Dict[str, Dict[str, Dict[str, Any]]]:

        # info start
        self.logger.info_up("Build input-output variables mapping ... ")

        # get fx configuration
        fx_in_by_process, fx_out_by_process = self._fx_in, self._fx_out

        # check cached mapping
        if self._mapping is not None:
            self.logger.info_down("Build input-output variables mapping ... ALREADY AVAILABLE. SKIPPED.")
            return self._mapping

        # get fx configuration
        fx_in_by_process, fx_out_by_process = self._fx_in or {}, self._fx_out or {}

        # initialize result
        result: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # organize input data by workflow/process
        data_in_by_process = {}
        for data_key, data_obj in self._data_in.items():

            # get process and variable
            process_name, variable_name = data_key.split(":", 1)
            # initialize process
            if process_name not in data_in_by_process:
                data_in_by_process[process_name] = {}
            # store variable object
            data_in_by_process[process_name][variable_name] = data_obj

        # organize output data by workflow/process
        data_out_by_process = {}
        for data_key, data_obj in self._data_out.items():

            # get process and variable
            process_name, variable_name = data_key.split(":", 1)
            # initialize process
            if process_name not in data_out_by_process:
                data_out_by_process[process_name] = {}

            # store variable object
            data_out_by_process[process_name][variable_name] = data_obj

        # get all available processes
        all_processes = (set(data_in_by_process) | set(data_out_by_process) |
                         set(fx_in_by_process) | set(fx_out_by_process))

        # check incomplete processes
        for process_name in all_processes:
            if process_name not in data_in_by_process:
                self.logger.warning(f"Process '{process_name}' has output variables but no input variables.")
            if process_name not in data_out_by_process:
                self.logger.warning(f"Process '{process_name}' has input variables but no output variables.")

        # iterate over processes
        for process_name in all_processes:

            # info start
            self.logger.info_up(f"Build mapping for process: '{process_name}' ... ")

            # get process input/output data
            process_data_in = data_in_by_process.get(process_name, {})
            process_data_out = data_out_by_process.get(process_name, {})
            # get process input/output fx
            process_fx_in = fx_in_by_process.get(process_name, {})
            process_fx_out = fx_out_by_process.get(process_name, {})

            # check input/output data availability
            if not process_data_in:
                self.logger.warning(f"Process '{process_name}' is not available in input data.")
            if not process_data_out:
                self.logger.warning(f"Process '{process_name}' is not available in output data.")
            # check input/output fx availability
            if not process_fx_in:
                self.logger.warning(f"Process '{process_name}' is not available in input fx.")
            if not process_fx_out:
                self.logger.warning(f"Process '{process_name}' is not available in output fx.")

            # initialize process mapping
            result[process_name] = {
                "in": {"data": {}, "fx": {},},
                "out": {"data": {}, "fx": {},},
            }
            # iterate over input/output sides
            for side, process_data in (("in", process_data_in), ("out", process_data_out)):

                # skip empty side
                if not process_data:
                    continue

                # destination data branch
                mapping_data = result[process_name][side]["data"]
                # iterate over process variables
                for variable_name, data_obj in process_data.items():

                    # define complete key
                    data_key = f"{process_name}:{variable_name}"

                    # normalize objects
                    obj_list = self._as_list(data_obj)
                    # iterate over partial objects
                    for idx, partial in enumerate(obj_list):

                        # get partial mapping
                        partial_mapping = self._get_partial_mapping(
                            partial=partial,tag=data_key, side=side,index_in_tag=idx,)

                        # skip empty mapping
                        if not partial_mapping:
                            continue

                        # merge partial mapping
                        for label, label_mapping in partial_mapping.items():

                            # initialize label
                            mapping_data.setdefault(label, {})

                            # iterate over template pairs
                            for tpl_key, tpl_value in label_mapping.items():

                                # get existing value
                                current_value = mapping_data[label].get(tpl_key,None,)

                                # warn about overwrite
                                if (
                                        current_value is not None
                                        and current_value != tpl_value):
                                    self.logger.warning(
                                        f"[{process_name}:{label}] "
                                        f"{side.upper()} DATA template key "
                                        f"'{tpl_key}' is being overwritten: "
                                        f"'{current_value}' -> '{tpl_value}'."
                                    )

                                # store mapping
                                mapping_data[label][tpl_key] = tpl_value

            # store fx mappings
            result[process_name]["in"]["fx"] = dict(process_fx_in)
            result[process_name]["out"]["fx"] = dict(process_fx_out)

            # info end
            self.logger.info_down(f"Build mapping for process: '{process_name}' ... DONE")

        # print mapping summary
        for process_name, process_mapping in result.items():

            # info summary start
            self.logger.info_up(f"Process '{process_name}' ... ")

            # iterate over sides
            for side in ("in", "out"):

                # get mappings
                data_mapping = process_mapping[side]["data"]
                fx_mapping = process_mapping[side]["fx"]

                # data mapping
                self.logger.info(f"  {side.upper()} DATA [{len(data_mapping)}]:")
                for variable_name, variable_mapping in data_mapping.items():
                    self.logger.info(f"    '{variable_name}' -> {variable_mapping}")
                # fx mapping
                self.logger.info(f"  {side.upper()} FX [{len(fx_mapping)}]:")
                for variable_name, variable_mapping in fx_mapping.items():
                    self.logger.info(f"    '{variable_name}' -> {variable_mapping}")

            # info summary end
            self.logger.info_down(f"Process '{process_name}' ... DEFINED")

        # cache mapping
        self._mapping = result
        # info end
        self.logger.info_down("Build input-output variables mapping ... DONE")

        return result

    # method to create compact rows
    def compact_rows(
            self,
            start_id: int = 1,
            process_tag: Optional[str] = "process") -> List[Dict[str, Any]]:

        # build input/output mapping
        mapping = self.build_mapping()

        # initialize links
        links: Dict[str, Data] = {}

        # initialize process id
        process_id = start_id

        # iterate over workflows
        for workflow_id, (workflow_name, workflow_mapping) in enumerate(
                mapping.items(), start=1):

            # normalize workflow name
            workflow_name = str(workflow_name)

            # get input/output sections
            in_section = workflow_mapping.get("in", {}) or {}
            out_section = workflow_mapping.get("out", {}) or {}

            # get data mappings
            in_data = in_section.get("data", {}) or {}
            out_data = out_section.get("data", {}) or {}

            # get fx mappings
            in_fx = in_section.get("fx", {}) or {}
            out_fx = out_section.get("fx", {}) or {}

            # collect all methods available for current workflow
            fx_names = list(dict.fromkeys([*in_fx.keys(),*out_fx.keys(),]))

            # collect workflow input keys
            variables_in = []
            # iterate over input variables
            for variable_key, variable_mapping in in_data.items():
                # skip empty mapping
                if not variable_mapping:
                    continue
                if variable_key not in variables_in:
                    variables_in.append(variable_key)
                else:
                    raise RuntimeError(f"Variable IN key '{variable_key}' is already defined. Change name in settings")
            # collect workflow output keys
            variables_out = []
            # iterate over input variables
            for variable_key, variable_mapping in out_data.items():
                # skip empty mapping
                if not variable_mapping:
                    continue
                if variable_key not in variables_out:
                    variables_out.append(variable_key)
                else:
                    raise RuntimeError(f"Variable OUT key '{variable_key}' is already defined. Change name in settings")

            # iterate over methods/processes
            for fx_name in fx_names:

                # get fx input/output mappings
                fx_mapping_in = in_fx.get(fx_name, {}) or {}
                fx_mapping_out = out_fx.get(fx_name, {}) or {}

                # define process tag
                tag = (str(process_tag) if process_tag is not None else str(fx_name))
                # define process name
                process_name = f"{tag}.{process_id}"
                # define link reference
                root_key = f"{workflow_name}:{fx_name}"

                # initialize link
                link = Data(
                    workflow_id=workflow_id,
                    workflow_name=workflow_name,
                    process_id=process_id,
                    process_name=process_name,
                    variables_in=variables_in.copy(),
                    variables_out=variables_out.copy(),
                    fx=str(fx_name),
                    fx_in=dict(fx_mapping_in),
                    fx_out=dict(fx_mapping_out),
                )

                # store link
                links[root_key] = link

                # update global process id
                process_id += 1

        # convert objects to compact rows
        rows = [link.to_dict() for link in links.values()]

        return rows

    # method to collect rows by priority (workflow first of all)
    def get_rows_by_priority(
            self,
            priority: Optional[List[str]] = None,
            rows: Optional[List[Dict[str, Any]]] = None,
            *,
            sort_others: bool = True,
            start_id: int = 1,
            field: str = "workflow_name",) -> List[Dict[str, Any]]:

        # check rows (and create them if not available)
        if rows is None:
            rows = self.compact_rows(start_id=start_id)

        # no priority defined
        if not priority:
            return rows

        # normalize priority workflows
        priority_workflows = [str(workflow) for workflow in priority]

        # initialize collections
        priority_rows: List[Dict[str, Any]] = []
        other_rows: List[Dict[str, Any]] = []

        # split rows by workflow priority
        for row in rows:
            # get workflow name
            workflow_name = str(row.get(field, ""))
            # collect priority rows
            if workflow_name in priority_workflows:
                priority_rows.append(row)
            else:
                other_rows.append(row)

        # sort priority rows according to requested workflow order
        priority_rows.sort(key=lambda row: priority_workflows.index(str(row.get(field, ""))))

        # optionally sort remaining workflows
        if sort_others:
            other_rows.sort(key=lambda row: str(row.get(field, "")))

        # merge priority + remaining rows
        rows_sorted = priority_rows + other_rows

        return rows_sorted
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to get pairs based on reference fields
    def get_pairs(
            self,
            field_value,
            field_key: str = "fx",
            rows: Optional[list] = None,) -> Union[List[Dict[str, Any]], Dict[str, Any]]:

        # map reference type to row field
        field_map = {
            "tag": "tag",
            "workflow": "workflow_name",
            "process": "process_name",
            "reference": "reference",
            "fx": "fx",
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

        if len(rows_selected) == 1:
            return rows_selected[0]
        else:
            raise RuntimeError('Selected process must be singleton at each step')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to get dictionary information by field and value
    @with_logger(var_name="logger_stream")
    def get_rows_by_field(self, data: Dict[str, Any],field: str, value: Any) -> Optional[Dict[str, Any]]:

        # check data
        if not data:
            logger_stream.warning("No data available")
            return None

        # check current dictionary
        if data.get(field) == value:
            return data

        # search nested objects
        for item in data.values():

            # nested dictionary
            if isinstance(item, dict):
                result = self.get_by_field(data=item, field=field,value=value)

                if result is not None:
                    return result

            # nested list or tuple
            elif isinstance(item, (list, tuple)):
                for element in item:

                    if isinstance(element, dict):
                        result = self.get_by_field(data=element, field=field, value=value)
                        if result is not None:
                            return result

        # field/value not found
        return None
    # ------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    # helpers
    # method to return list
    @staticmethod
    def _as_list(obj: Union[Any, List[Any]]) -> List[Any]:
        if obj is None:
            return []
        if isinstance(obj, (list, tuple, set)):
            return list(obj)
        return [obj]

    # method to get attributes or key
    @staticmethod
    def _getattr_or_key(partial: Any, key: str, default=None):
        if isinstance(partial, (dict, AbcMapping)):
            return partial.get(key, default)
        return getattr(partial, key, default)

    # method to get partial mapping
    def _get_partial_mapping(self, partial: Any, tag: str, side: str, index_in_tag: int):

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
                            key_separator=':', key_order='workflow_and_variable'):

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

    # check workflows available in process_list but not used by namespaces
    workflow_unused = []
    for process_workflow in process_list.keys():
        if process_workflow not in workflow_found:
            workflow_unused.append(process_workflow)

            logger_stream.warning(
                f"Process '{process_workflow}' is available in process_list but is not available in namespaces.")

    # info object
    info = {
        "available_in_process_list": workflow_found,
        "missing_in_process_list": workflow_missed,
        "extras_in_process_list": workflow_unused,
        "workflow_tags": workflow_tags,
    }

    return pairs_list_str, pairs_list_tuple, process_selection, info
# ----------------------------------------------------------------------------------------------------------------------
