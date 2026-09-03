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
        data_count: Optional[int] = 1,
        logger: LoggingManager = None,) -> None:

        self.logger = logger or LoggingManager(name="Mapper")
        self._data_in = data_collections_in
        self._data_out = data_collections_out
        self._mapping: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
        self._data_count: int = data_count

    # method to build mapping in/out
    def build_mapping(self) -> Dict[str, Dict[str, Dict[str, Any]]]:

        # info start
        self.logger.info_up("Build input-output variables mapping ... ")

        # check cached mapping
        if self._mapping is not None:
            self.logger.info_down("Build input-output variables mapping ... ALREADY AVAILABLE. SKIPPED.")
            return self._mapping

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
        processes_in, processes_out = set(data_in_by_process.keys()), set(data_out_by_process.keys())
        all_processes = processes_in | processes_out

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

            # get process input/output objects
            process_data_in = data_in_by_process.get(process_name, {})
            process_data_out = data_out_by_process.get(process_name, {})

            # check input/output availability
            if not process_data_in:
                self.logger.warning(f"Process '{process_name}' is not available in input data.")
            if not process_data_out:
                self.logger.warning(f"Process '{process_name}' is not available in output data.")

            # initialize process mapping
            result[process_name] = {"in": {}, "out": {}}
            # iterate over input/output sides
            for side, process_data in (("in", process_data_in), ("out", process_data_out)):

                # skip empty side
                if not process_data:
                    continue

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
                            partial=partial, tag=data_key, side=side, index_in_tag=idx)

                        # skip empty mapping
                        if not partial_mapping:
                            continue

                        # merge partial mapping
                        for label, label_mapping in partial_mapping.items():

                            # initialize label
                            if label not in result[process_name][side]:
                                result[process_name][side][label] = {}

                            # iterate over template pairs
                            for tpl_key, tpl_value in label_mapping.items():

                                # check existing value
                                current_value = result[process_name][side][label].get(
                                    tpl_key,
                                    None
                                )

                                # warn about overwrite
                                if (
                                        current_value is not None
                                        and current_value != tpl_value):
                                    self.logger.warning(
                                        f"[{process_name}:{label}] "
                                        f"{side.upper()} template key '{tpl_key}' "
                                        f"is being overwritten: "
                                        f"'{current_value}' -> '{tpl_value}'."
                                    )

                                # store mapping
                                result[process_name][side][label][tpl_key] = tpl_value

            # info end
            self.logger.info_down(
                f"Build mapping for process: '{process_name}' ... DONE")


        # print mapping summary
        for process_name, process_mapping in result.items():

            self.logger.info(f"Process '{process_name}':")

            # input mapping
            self.logger.info(f"  IN [{len(process_mapping['in'])}]:")

            for variable_name, variable_mapping in process_mapping["in"].items():
                self.logger.info(
                    f"    '{variable_name}' -> {variable_mapping}"
                )

            # output mapping
            self.logger.info(f"  OUT [{len(process_mapping['out'])}]:")

            for variable_name, variable_mapping in process_mapping["out"].items():
                self.logger.info(
                    f"    '{variable_name}' -> {variable_mapping}"
                )

        # cache mapping
        self._mapping = result

        # info end
        self.logger.info_down(
            "Build input-output variables mapping ... DONE"
        )

        return result
    # ----------------------------------------------------------------------------------------------------------------------

    # method to create a compact rows
    def compact_rows(self, start_id: int = 1) -> List[Dict[str, Any]]:

        mapping = self.build_mapping()
        rows: List[Dict[str, Any]] = []
        next_id = start_id

        for tag in sorted(mapping.keys(), key=str):
            in_map = mapping[tag].get("in", {}) or {}
            out_map = mapping[tag].get("out", {}) or {}

            for in_key, workflow in sorted(in_map.items(), key=lambda kv: str(kv[0])):
                out_val: Optional[Any] = out_map.get(workflow)
                if out_val is None:
                    self.logger.warning(
                        f"[{tag}] No matching OUT for workflow '{workflow}'. "
                        f"Available OUT keys: {list(out_map.keys())}"
                    )
                rows.append(
                    {
                        "tag": str(tag),
                        "in": str(in_key),
                        "workflow": str(workflow),
                        "out": (str(out_val) if out_val is not None else None),
                        "id": next_id,
                        "reference": f"{tag}:{workflow}",
                    }
                )
                next_id += 1
        return rows

    # method to collect rows by priority (variable first of all)
    def get_rows_by_priority(
        self,
        priority_vars: Optional[List[str]] = None,
        rows: Optional[List[Dict[str, Any]]] = None,
        code: Optional[int] = 1,
        *,
        sort_others: bool = True,
        start_id: int = 1,
        field: str = "in",
    ) -> List[Dict[str, Any]]:

        if rows is None:
            rows = self.compact_rows(start_id=start_id)
        if not priority_vars:
            return rows

        priority_vars_str = [str(v) for v in priority_vars]
        priority_part: List[Dict[str, Any]] = []
        others_part: List[Dict[str, Any]] = []

        for row in rows:
            var_name = str(row.get(field, ""))
            (priority_part if var_name in priority_vars_str else others_part).append(row)

        priority_part.sort(
            key=lambda r: priority_vars_str.index(str(r.get(field, "")))
            if str(r.get(field, "")) in priority_vars_str
            else len(priority_vars_str)
        )

        if sort_others:
            others_part.sort(key=lambda r: str(r.get(field, "")))

        return priority_part + others_part

    # method to get pairs
    def get_pairs(self, name: str, type: str = "workflow") -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        if type not in ("tag", "workflow", "reference"):
            raise ValueError("type must be 'tag', 'workflow' or 'reference'.")

        mapping = self.build_mapping()
        rows: List[Dict[str, Any]] = []

        if type == "tag":
            if name not in mapping:
                raise ValueError(f"Tag '{name}' not found.")
            tag = name
            in_map = mapping[tag].get("in", {}) or {}
            out_map = mapping[tag].get("out", {}) or {}

            for in_key, wf_name in sorted(in_map.items(), key=lambda kv: str(kv[0])):
                out_val = out_map.get(wf_name)
                rows.append(
                    {
                        "tag": str(tag),
                        "in": str(in_key),
                        "workflow": str(wf_name),
                        "reference": f"{tag}:{wf_name}",
                        "out": (str(out_val) if out_val is not None else None),
                    }
                )

        elif type == "reference":

            if ":" not in name:
                raise ValueError("Invalid reference. Expected 'tag:workflow'.")
            tag, wf_name = name.split(":", 1)

            if tag not in mapping:
                raise ValueError(f"Tag '{tag}' not found.")

            in_map = mapping[tag].get("in", {}) or {}
            out_map = mapping[tag].get("out", {}) or {}

            matched_in_keys = [k for k, v in in_map.items() if v == wf_name]
            if not matched_in_keys:
                raise ValueError(f"No IN entries found for workflow '{wf_name}' under tag '{tag}'.")

            for in_key in sorted(matched_in_keys, key=str):
                out_val = out_map.get(wf_name)
                rows.append(
                    {
                        "tag": str(tag),
                        "in": str(in_key),
                        "workflow": str(wf_name),
                        "reference": f"{tag}:{wf_name}",
                        "out": (str(out_val) if out_val is not None else None),
                    }
                )

        else:  # workflow
            target_wf = name
            for tag in sorted(mapping.keys(), key=str):
                in_map = mapping[tag].get("in", {}) or {}
                out_map = mapping[tag].get("out", {}) or {}
                for in_key, wf_name in sorted(in_map.items(), key=lambda kv: (str(tag), str(kv[0]))):
                    if wf_name != target_wf:
                        continue
                    out_val = out_map.get(wf_name)
                    rows.append(
                        {
                            "tag": str(tag),
                            "in": str(in_key),
                            "workflow": str(wf_name),
                            "reference": f"{tag}:{wf_name}",
                            "out": (str(out_val) if out_val is not None else None),
                        }
                    )

            if not rows:
                raise ValueError(f"No mapping rows found for workflow '{name}'.")

        return rows[0] if len(rows) == 1 else rows

    # -----------------------------
    # Internals
    # -----------------------------
    @staticmethod
    def _as_list(obj: Union[Any, List[Any]]) -> List[Any]:
        if obj is None:
            return []
        if isinstance(obj, (list, tuple, set)):
            return list(obj)
        return [obj]

    @staticmethod
    def _getattr_or_key(partial: Any, key: str, default=None):
        if isinstance(partial, (dict, AbcMapping)):
            return partial.get(key, default)
        return getattr(partial, key, default)

    # method to get partial mapping
    def _get_partial_mapping(
            self,
            partial: Any,
            tag: str,
            side: str,
            index_in_tag: int):

        # check side
        if side not in ("in", "out"):
            raise ValueError(
                f"Unsupported side '{side}'. Expected 'in' or 'out'."
            )

        # get variables
        file_vars = self._getattr_or_key(
            partial,
            "file_variable",
            None
        )

        if file_vars is None:
            self.logger.warning(
                f"[{tag}] {side.upper()} partial #{index_in_tag} "
                f"missing 'file_variable'; skipping."
            )
            return {}

        # get workflows
        file_workflows = self._getattr_or_key(
            partial,
            "file_workflow",
            None
        )

        if file_workflows is None:
            self.logger.warning(
                f"[{tag}] {side.upper()} partial #{index_in_tag} "
                f"missing 'file_workflow'; skipping."
            )
            return {}

        # get variable template
        variable_template = self._getattr_or_key(
            partial,
            "variable_template",
            None
        )

        if not isinstance(variable_template, (dict, AbcMapping)):
            self.logger.warning(
                f"[{tag}] {side.upper()} partial #{index_in_tag} "
                f"missing 'variable_template'; skipping."
            )
            return {}

        # get vars data
        vars_data = variable_template.get("vars_data", None)

        if not isinstance(vars_data, (dict, AbcMapping)):
            self.logger.warning(
                f"[{tag}] {side.upper()} partial #{index_in_tag} "
                f"'vars_data' is not a mapping; skipping."
            )
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
