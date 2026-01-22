from __future__ import annotations

import warnings
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from collections.abc import Mapping as AbcMapping

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.logging_toolkit.lib_logging_utils import with_logger


class Mapper:
    """
    Build a flat mapping between input and output variable templates, and produce
    compact or tag-specific rows.
    """

    def __init__(
        self,
        data_collections_in: Mapping[str, Union[Any, List[Any]]],
        data_collections_out: Mapping[str, Union[Any, List[Any]]],
        logger: LoggingManager = None,
    ) -> None:
        self.logger = logger or LoggingManager(name="Mapper")
        self._data_in = data_collections_in
        self._data_out = data_collections_out
        self._mapping: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None

    def build_mapping(self) -> Dict[str, Dict[str, Dict[str, Any]]]:

        # info start
        self.logger.info_up("Build input-output variables mapping ... ")

        # check mapping
        if self._mapping is not None:
            # info end
            self.logger.info_down("Build input-output variables mapping ... ALREADY AVAILABLE. SKIPPED.")
            return self._mapping

        result: Dict[str, Dict[str, Dict[str, Any]]] = {}

        keys_in = set(self._data_in.keys())
        keys_out = set(self._data_out.keys())

        missing_in = sorted(keys_out - keys_in)
        missing_out = sorted(keys_in - keys_out)
        if missing_out:
            self.logger.warning(f"Keys present only in input: {missing_out}")
        if missing_in:
            self.logger.warning(f"Keys present only in output: {missing_in}")

        # define shared keys
        shared_keys = keys_in & keys_out
        # iterate over shared keys
        for key in shared_keys:

            # info start
            self.logger.info_up(f"Build mapping for key: '{key}' ... ")

            obj_in = self._as_list(self._data_in.get(key))
            obj_out = self._as_list(self._data_out.get(key))

            for side, objs in (("in", obj_in), ("out", obj_out)):
                for idx, partial in enumerate(objs):

                    # sort labels and items
                    labels_sorted, workflow_sorted, items_sorted = self._sorted_labels_and_items(partial, key, side, idx)

                    # check length mismatch
                    if len(labels_sorted) != len(items_sorted):
                        self.logger.warning(
                            f"[{key}] {side.upper()} side mismatch: "
                            f"{len(labels_sorted)} labels vs {len(items_sorted)} template items; "
                            f"extra entries will be ignored."
                        )

                    # iterate over sorted labels and items
                    for label, (tpl_key, tpl_val) in zip(labels_sorted, items_sorted):

                        label = str(label)
                        tpl_key = str(tpl_key)
                        if label not in result:
                            result[label] = {"in": {}, "out": {}}

                        if tpl_key in result[label][side] and result[label][side][tpl_key] != tpl_val:
                            self.logger.warning(
                                f"[{label}] {side.upper()} template key '{tpl_key}' is being overwritten."
                            )
                        result[label][side][tpl_key] = tpl_val

            # info end
            self.logger.info_down(f"Build mapping for key: '{key}' ... DONE")

        # print summary to check if mapping is correct
        for wf_name, wf_struct in result.items():
            self.logger.info(f"'{wf_name}' = {{dict: {len(wf_struct)}}} {wf_struct}")

        # cache mapping
        self._mapping = result

        # info end
        self.logger.info_down("Build input-output variables mapping ... DONE")

        return result

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

    def get_rows_by_priority(
        self,
        priority_vars: Optional[List[str]] = None,
        rows: Optional[List[Dict[str, Any]]] = None,
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

    def _sorted_labels_and_items(
        self,
        partial: Any,
        tag: str,
        side: str,
        index_in_tag: int,
    ) -> Tuple[List[str], List[Tuple[str, Any]]]:

        # get file variables
        file_vars = self._getattr_or_key(partial, "file_variable", None)
        if file_vars is None:
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'file_variable'; skipping.")
            return [], []

        # get file workflows
        file_wf = self._getattr_or_key(partial, "file_workflow", None)
        if file_wf is None:
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'file_wf'; skipping.")
            return [], []

        # manage the order of labels and workflows
        labels = [str(x) for x in file_vars] if isinstance(file_vars, (list, tuple, set)) else [str(file_vars)]
        workflows = [str(x) for x in file_wf] if isinstance(file_wf, (list, tuple, set)) else [str(file_wf)]

        labels_sorted, workflow_sorted = zip(*sorted(zip(labels, workflows), key=lambda x: str(x[0])))
        labels_sorted, workflow_sorted = list(labels_sorted), list(workflow_sorted)

        variable_template = self._getattr_or_key(partial, "variable_template", None)
        if not isinstance(variable_template, (dict, AbcMapping)):
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'variable_template'; skipping.")
            return [], []

        vars_data = variable_template.get("vars_data")
        if not isinstance(vars_data, (dict, AbcMapping)):
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} 'vars_data' is not a mapping; skipping.")
            return [], []

        # sort items based on variable template keys
        items_tmp = sorted(((str(k), v) for k, v in vars_data.items()), key=lambda kv: kv[0])

        # check to ensure according to labels, workflows and items
        items_sorted = []
        for label, wflow in zip(labels_sorted, workflow_sorted):
            # search item matching workflow
            item_pos = next((i for i, t in enumerate(items_tmp) if wflow in t), -1)
            if item_pos == -1:
                self.logger.warning(
                    f"Item is not selected for label '{label}' and workflow '{wflow}'; skipping."
                )
            else:
                items_sorted.append(items_tmp[item_pos])

        return labels_sorted, workflow_sorted, items_sorted
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
def build_pairs_and_process(process_list, file_variable, dataset_namespace, str_separator=':'):
    """
    Build variable-workflow pairs from DatasetNamespace entries and gather diagnostics.

    Parameters
    ----------
    process_list : dict
        {var_name: process_info}
    file_variable : list[str]
        Variable names (order matters if dataset_namespace is a list/tuple)
    dataset_namespace : DatasetNamespace | dict[str, DatasetNamespace] | list[DatasetNamespace] | tuple[...]
        Each DatasetNamespace exposes .variable and .workflow
    str_separator : str, optional
        Separator for compact "key:workflow" strings (default ':')

    Returns
    -------
    pairs_list_str    : list[str]             # ["key:workflow", ...]
    pairs_list_tuple  : list[tuple[str,str]]  # [(key, workflow), ...]
    process_found     : list                  # [process_list[key], ...]
    process_dict      : dict[str, any]        # {"key:workflow": process_info, ...}
    info              : dict                  # diagnostics + {"workflow_tags": {key: workflow or None}}
    """

    def _ns_has_fields(ns):
        return ns is not None and hasattr(ns, "variable") and hasattr(ns, "workflow")

    def _resolve_ns(name, idx):
        if isinstance(dataset_namespace, dict):
            return dataset_namespace.get(name)
        if isinstance(dataset_namespace, (list, tuple)):
            return dataset_namespace[idx] if 0 <= idx < len(dataset_namespace) else None
        return dataset_namespace  # single namespace used for all

    def _dataset_keys():
        if isinstance(dataset_namespace, dict):
            return list(dataset_namespace.keys())
        if isinstance(dataset_namespace, (list, tuple)):
            return [ns.variable for ns in dataset_namespace if _ns_has_fields(ns)]
        return [dataset_namespace.variable] if _ns_has_fields(dataset_namespace) else []

    process_found = []
    pairs_list_tuple = []
    pairs_list_str = []
    process_dict = {}
    workflow_tags = {}

    # check file variable as a list
    if not isinstance(file_variable, list):
        file_variable = [file_variable]

    for i, var_name in enumerate(file_variable):
        if var_name not in process_list:
            continue

        ns = _resolve_ns(var_name, i)
        if _ns_has_fields(ns):
            tag_workflow = ns.workflow
        else:
            tag_workflow = var_name  # fallback if namespace missing

        key = f"{var_name}{str_separator}{tag_workflow}"

        pairs_list_tuple.append((var_name, tag_workflow))
        pairs_list_str.append(key)
        process_found.append(process_list[var_name])
        process_dict[key] = process_list[var_name]
        workflow_tags[var_name] = tag_workflow if _ns_has_fields(ns) else None

    dataset_keys = _dataset_keys()
    info = {
        "missing_in_dataset": [v for v in file_variable if v not in dataset_keys],
        "missing_in_process": [v for v in file_variable if v not in process_list],
        "extras_in_process": [k for k in process_list if k not in file_variable],
        "workflow_tags": workflow_tags,
    }

    return pairs_list_str, pairs_list_tuple, process_found, info
# ----------------------------------------------------------------------------------------------------------------------
