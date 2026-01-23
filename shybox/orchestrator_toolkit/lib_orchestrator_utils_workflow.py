"""
Class Features

Name:          lib_orchestrator_utils_workflow
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations
from collections import defaultdict

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalise deps (args or dataset)
@with_logger(var_name='logger_stream')
def normalize_deps(deps):
    if deps is None:
        return {}
    elif isinstance(deps, dict):
        return deps
    elif isinstance(deps, (list, tuple)):
        return {i + 1: v for i, v in enumerate(deps)}
    else:
        logger_stream.error(f"Unsupported deps type: {type(deps)}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure list
@with_logger(var_name='logger_stream')
def as_list(maybe_seq):
    if isinstance(maybe_seq, (list, tuple)):
        return list(maybe_seq), True
    return [maybe_seq], False
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to remove none values from a list
@with_logger(var_name='logger_stream')
def remove_none(lst):
    return [x for x in lst if x is not None]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to group processes by attribute
@with_logger(var_name='logger_stream')
def group_process(proc_list, proc_tag="reference"):
    proc_group = defaultdict(list)
    for proc in proc_list:
        if proc_tag == "reference":
            if not hasattr(proc, "reference") or proc.reference is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'reference' attribute.")
            proc_group[proc.reference].append(proc)
        elif proc_tag == "workflow":
            if not hasattr(proc, "workflow") or proc.workflow is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'workflow' attribute.")
            proc_group[proc.workflow].append(proc)
        elif proc_tag == "tag":
            if not hasattr(proc, "tag") or proc.tag is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'tag' attribute.")
            proc_group[proc.tag].append(proc)
        else:
            logger_stream.error(f"Invalid proc_tag '{proc_tag}'. Must be 'reference', 'workflow' or 'tag'.")
    return dict(proc_group)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check compatibility between data and fx dicts
@with_logger(var_name='logger_stream')
def ensure_variables(data_collections, fx_collections, mode='strict'):

    # Keys sets
    keys_data = set(data_collections.keys())
    keys_fx = set(fx_collections.keys())

    # Differences
    only_in_data = keys_data - keys_fx
    only_in_fx = keys_fx - keys_data
    common = keys_data & keys_fx

    # Mode checks
    if mode == 'strict':
        logger_stream.info(f"[strict] Keys in BOTH: {sorted(common)}")
        logger_stream.info(f"[strict] Only in DATA: {sorted(only_in_data)}")
        logger_stream.info(f"[strict] Only in FX:  {sorted(only_in_fx)}")

        if keys_data != keys_fx:
            logger_stream.error("Strict mode failed: key mismatch.")
            raise AssertionError("Strict mode failed: key mismatch.")

    elif mode == 'less_from_data':
        logger_stream.info(f"[less_from_out] DATA keys: {sorted(keys_data)}")
        logger_stream.info(f"[less_from_out] FX keys:  {sorted(keys_fx)}")
        logger_stream.info(f"[less_from_out] Missing in FX (problem): {sorted(only_in_data)}")

        if only_in_data:
            logger_stream.error("less_from_out failed: some DATA keys are not in FX.")
            raise AssertionError("less_from_out failed: some DATA keys are not in FX.")

    elif mode == 'less_from_fx':

        logger_stream.info(f"[less_from_fx] FX keys: {sorted(keys_fx)}")
        logger_stream.info(f"[less_from_fx] DATA keys: {sorted(keys_data)}")
        logger_stream.info(f"[less_from_fx] Missing in OUT (problem): {sorted(only_in_fx)}")

        if only_in_fx:
            logger_stream.error("less_from_fx failed: some FX keys are not in DATA.")
            raise AssertionError("less_from_fx failed: some FX keys are not in DATA.")

    elif mode == 'lazy':

        logger_stream.info(f"[lazy] Keys in DATA: {sorted(keys_data)}")
        logger_stream.info(f"[lazy] Keys in FX:  {sorted(keys_fx)}")
        logger_stream.info(f"[lazy] Common keys: {sorted(common)}")

        if not common:
            logger_stream.error("lazy failed: no common keys.")
            raise AssertionError("lazy failed: no common keys.")

    else:
        logger_stream.error(f"Unknown mode '{mode}'")
        raise ValueError(f"Unknown mode '{mode}'")

    return True
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
# method to get links between key and value
def _get_links(var, item, flag="key_to_value"):
    if flag == "key_to_value":
        if item not in var:
            return None, False
        return var[item], True
    elif flag == "value_to_key":
        keys = [k for k, v in var.items() if v == item]
        if len(keys) != 1:
            return None, False
        return keys[0], True
    return None, False

# method to convert to bool
def _as_bool(x):
    return bool(x) if x is not None else False

# method to make report line
def _make_report_line(k, s):
    return (
        f"[{k}] "
        f"file_in={s.get('variable_file_in')} file_out={s.get('variable_file_out')} | "
        f"wf_in={s.get('variable_workflow_in')} wf_out={s.get('variable_workflow_out')} | "
        f"name_in={s.get('variable_name_in')} name_out={s.get('variable_name_out')} | "
        f"process_flag={s.get('process_flag')} check_ok={s.get('check_ok')} "
        f"({s.get('check_code')}) {s.get('check_msg')}"
    )

# method to ensure workflows
@with_logger(var_name='logger_stream')
def ensure_workflows(
    data_package_in: list,
    data_package_out: list,
    data_process: dict,
    *,
    show_links: bool = False,
    show_report: bool = False,
    mode: str = "strict",     # "strict" | "lazy"
    reuse: bool = True        # reuse partial info if OUT or IN missing
):
    # declare common obj
    workflow_obj = {}

    # build IN side
    for _, data_in_obj in enumerate(data_package_in):

        nspace_in = data_in_obj.file_namespace
        variable_in = data_in_obj.variable_template['vars_data']
        if not isinstance(nspace_in, list):
            nspace_in = [nspace_in]

        for nspace_step in nspace_in:
            nspace_var_step, nspace_wf_step = nspace_step.variable, nspace_step.workflow
            # IN: workflow value -> find unique key
            nspace_key_step, status_key_step = _get_links(variable_in, nspace_wf_step, flag="value_to_key")

            if status_key_step:
                if nspace_var_step not in workflow_obj:
                    workflow_obj[nspace_var_step] = {}

                workflow_obj[nspace_var_step].update({
                    'variable_file_in': nspace_var_step,
                    'variable_workflow_in': nspace_wf_step,
                    'variable_name_in': nspace_key_step,
                    'variable_status_in': True,
                })

    # build OUT side
    for _, data_out_obj in enumerate(data_package_out):

        nspace_out = data_out_obj.file_namespace
        variable_out = data_out_obj.variable_template['vars_data']
        if not isinstance(nspace_out, list):
            nspace_out = [nspace_out]

        for nspace_step in nspace_out:
            nspace_var_step, nspace_wf_step = nspace_step.variable, nspace_step.workflow
            # OUT: key -> return value  (your original)
            nspace_val_step, status_val_step = _get_links(variable_out, nspace_wf_step, flag="key_to_value")

            if status_val_step:
                # ensure dict exists even if only OUT is found
                if nspace_var_step not in workflow_obj:
                    workflow_obj[nspace_var_step] = {}

                workflow_obj[nspace_var_step].update({
                    'variable_file_out': nspace_var_step,
                    'variable_workflow_out': nspace_wf_step,
                    'variable_name_out': nspace_val_step,
                    'variable_status_out': True,
                })

    # ensure process in list + set flags
    for workflow_key in list(workflow_obj.keys()):
        _, status_var_step = _get_links(data_process, workflow_key, flag="key_to_value")
        workflow_obj[workflow_key]['process_name'] = workflow_key
        workflow_obj[workflow_key]['process_flag'] = _as_bool(status_var_step)

    # cross-check in/out coherence + add extra variables
    errors = []
    for workflow_key, s in workflow_obj.items():

        # presence checks
        has_in = _as_bool(s.get('variable_status_in'))
        has_out = _as_bool(s.get('variable_status_out'))

        file_in = s.get('variable_file_in')
        file_out = s.get('variable_file_out')
        wf_in = s.get('variable_workflow_in')
        wf_out = s.get('variable_workflow_out')

        # default check state
        s['check_ok'] = True
        s['check_code'] = "OK"
        s['check_msg'] = "consistent"

        # missing side handling
        if not has_in or not has_out:
            if reuse:
                s['check_ok'] = False
                s['check_code'] = "PARTIAL"
                s['check_msg'] = f"partial mapping (has_in={has_in}, has_out={has_out})"
            else:
                # drop incomplete entries in lazy mode; raise in strict
                msg = f"{workflow_key}: missing IN/OUT mapping (has_in={has_in}, has_out={has_out})"
                if mode == "strict":
                    logger_stream.error(msg)
                    raise ValueError(msg)
                errors.append(msg)
                s['check_ok'] = False
                s['check_code'] = "MISSING"
                s['check_msg'] = msg

        # coherence checks only if both present
        if has_in and has_out:
            same_file = (file_in == file_out)
            same_wf = (wf_in == wf_out)

            s['check_file_ok'] = same_file
            s['check_workflow_ok'] = same_wf

            if not same_file and not same_wf:
                s['check_ok'] = False
                s['check_code'] = "FILE+WF_MISMATCH"
                s['check_msg'] = f"file_in!=file_out and wf_in!=wf_out ({file_in}!={file_out}, {wf_in}!={wf_out})"
            elif not same_file:
                s['check_ok'] = False
                s['check_code'] = "FILE_MISMATCH"
                s['check_msg'] = f"file_in!=file_out ({file_in}!={file_out})"
            elif not same_wf:
                s['check_ok'] = False
                s['check_code'] = "WF_MISMATCH"
                s['check_msg'] = f"workflow_in!=workflow_out ({wf_in}!={wf_out})"
            else:
                s['check_file_ok'] = True
                s['check_workflow_ok'] = True

        # strict mode: any mismatch becomes exception
        if mode == "strict" and not s['check_ok']:
            # in strict we fail only for real mismatches, not PARTIAL if you want:
            # decide policy: here strict fails for any non-OK
            logger_stream.error(f"Workflow '{workflow_key}' failed check: {s['check_code']} - {s['check_msg']}")
            raise ValueError(f"Workflow '{workflow_key}' failed check: {s['check_code']} - {s['check_msg']}")

    # optional report
    if show_report:
        # if you have logger_stream from with_logger, prefer that. fallback to print.
        try:
            logger_stream.info("WORKFLOW REPORT:")
            for k, s in workflow_obj.items():
                logger_stream.info(_make_report_line(k, s))
        except Exception as exp:
            logger_stream.info("WORKFLOW REPORT:")
            for k, s in workflow_obj.items():
                logger_stream.warning(_make_report_line(k, s))

    return workflow_obj
# ----------------------------------------------------------------------------------------------------------------------
