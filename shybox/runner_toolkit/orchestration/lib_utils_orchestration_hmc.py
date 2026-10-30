"""
Library Features:

Name:          lib_utils_orchestration_hmc
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261027'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import string

from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.runner_toolkit.namelist.namelist_template_handler import NamelistTemplateManager
from shybox.runner_toolkit.namelist.namelist_structure_handler import NamelistStructureManager

from shybox.runner_toolkit.execution.execution_handler import ExecutionManager, ExecutionAnalyzer
# ----------------------------------------------------------------------------------------------------------------------

def debug_hmc_logger(label, member_log):

    print(f"\n[{label}]")
    print(f"Manager ID : {id(member_log)}")
    print(f"Logger ID  : {id(member_log.logger)}")
    print(f"Logger name: {member_log.logger.name}")
    print(f"Log path   : {member_log.log_path}")

    for handler in member_log.logger.handlers:
        print(
            f"Handler    : {type(handler).__name__}",
            f"File       : {getattr(handler, 'baseFilename', None)}"
        )

# ----------------------------------------------------------------------------------------------------------------------
# method to create hmc logger
def configure_hmc_logger(alg_cfg_obj, members_tags, time_tags, view_table=True, level=logging.DEBUG):

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING INSTANCE MANAGEMENT

    # set logger name
    alg_logger_raw = "shybox_algorithm_runner_hmc_{ensemble_member}"
    # Get placeholders
    placeholders = [
        field_name
        for _, field_name, _, _ in string.Formatter().parse(alg_logger_raw) if field_name is not None
    ]

    # Check placeholders and format
    if placeholders:
        missing = [key for key in placeholders if key not in members_tags]
        if missing:
            raise ValueError(f"Missing tags for '{alg_logger_raw}': {missing}")
        alg_logger_name = alg_logger_raw.format(**members_tags)
    else:
        alg_logger_name = alg_logger_raw

    # get application logging
    alg_app_log = alg_cfg_obj.get_application("log", root_key=None)
    # fill application logging
    alg_app_log = alg_app_log.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        time_keys=("time_start", "time_restart", "time_period", "time_tag"),
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        extra_tags=members_tags,
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # view application logging
    alg_cfg_obj.view(section=alg_app_log, table_name='application [cfg application logging]', table_print=view_table)

    # Create independent member logger
    logging_handle_member = LoggingManager.create_instance(
        name=alg_logger_name,
        logger_folder=alg_app_log["path"],
        logger_file=alg_app_log["file_name"],
        logger_format=(
            "%(asctime)s %(name)-15s %(levelname)-8s "
            "%(message)-80s %(filename)-20s:"
            "[%(lineno)-6s - %(funcName)-20s()]"
        ),
        handlers=alg_app_log.get("handlers", ["file", "stream"]),
        level=level,
        use_arrows=True,
        arrow_dynamic=True,
        arrow_tag="algorithm",
        arrow_base_len=3,
        propagate=False,
        set_as_current=False,
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING VALIDATION MANAGEMENT

    # check logger name
    if logging_handle_member.logger.name != alg_logger_name:
        raise RuntimeError(
            f"Logger name mismatch: "
            f"expected '{alg_logger_name}', "
            f"got '{logging_handle_member.logger.name}'"
        )

    # check logger handlers
    if not logging_handle_member.logger.handlers:
        raise RuntimeError(
            f"No logging handlers configured for '{alg_logger_name}'"
        )

    # log initialization
    logging_handle_member.info(
        f"HMC logging initialized for member "
        f"{members_tags['ensemble_member']}"
    )

    # ------------------------------------------------------------------------------------------------------------------
    # return objects
    return logging_handle_member
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to configure hmc member
def configure_hmc_member(alg_cfg_obj, alg_cfg_log, members_tags, time_tags, view_table=True):

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO MANAGEMENT
    # get application logging
    alg_app_info = alg_cfg_obj.get_application("info", root_key=None, convert_none_to_nan=False, )
    # fill application logging
    alg_app_info = alg_app_info.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        time_keys=("time_start", "time_restart", "time_period", "time_tag"),  # <- keep these in LUT
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        extra_tags=members_tags,
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION MANAGEMENT
    # get application execution
    alg_app_exec = alg_cfg_obj.get_application("application_execution", root_key=None)
    # fill application execution
    alg_app_exec = alg_app_exec.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        extra_tags=members_tags,
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # view application execution section
    alg_cfg_obj.view(section=alg_app_exec, table_name='application [cfg application execution]', table_print=view_table)

    # get application namelist
    alg_app_nml = alg_cfg_obj.get_application("application_namelist", root_key=None)
    # fill application namelist
    alg_app_nml = alg_app_nml.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        time_keys=("time_start", "time_restart", "time_period"),  # <- keep these in LUT
        env_extra=None,  # or {"RUN": "base"} etc
        extra_tags=members_tags,
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )

    # view application namelist section
    alg_cfg_obj.view(section=alg_app_nml, table_name='application [cfg application namelist]', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # return objects
    app_member_cfg = {
        'app_namelist': alg_app_nml, 'app_execution': alg_app_exec,'app_info': alg_app_info, 'app_log': alg_cfg_log}
    return app_member_cfg
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------
# method to execute hmc member
def execute_hmc_member(member_tags, member_config, *, time_tags, dry_run=False, view_table=True):

    # ------------------------------------------------------------------------------------------------------------------
    # APPLICATION MEMBER OBJECT(S)
    app_namelist, app_execution = member_config['app_namelist'], member_config['app_execution']
    app_info, app_log = member_config['app_info'], member_config['app_log']
    # APPLICATION TIME RUN
    app_time_run = time_tags['time_run']
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION NAMELIST
    # 1. create the template manager (with all HMC/S3M templates)
    app_nml_obj = NamelistTemplateManager(logger=app_log)

    # 2. (optional) if you just want to inspect the template fields:
    app_nml_fields = app_nml_obj.get(
        model=app_namelist['description']['type'],
        version=app_namelist['description']['version'],
    )

    # 3. build the namelist text from your fields (flat or sectioned)
    app_nml_struct = NamelistStructureManager.from_dict(
        template_manager=app_nml_obj,
        model=app_namelist['description']['type'],
        version=app_namelist['description']['version'],
        values=app_namelist['fields'],  # e.g. {"by_value": {...}, "by_pattern": {...}}
        as_object=True,
    )
    # view namelist structure
    app_nml_struct.view(table_name='application [file application namelist]', table_print=view_table)
    # write namelist to file
    app_nml_struct.write_to_ascii(
        filename=app_namelist['file']['location'],
        overwrite = True, makedirs = True,)

    ## APPLICATION EXECUTION
    # create execution manager
    app_execution_obj = ExecutionManager(
        execution_obj=app_execution,  # your config dict
        time_obj=app_time_run,  # optional
        settings_obj={
            'MODE': app_execution['description']['execution_mode'],
            'RUN': app_execution['description']['execution_name']},  # or whatever you need for {RUN}
        execution_update=True,  # re-run or reuse .info
        stream_output=True,  # live Fortran logs
        timeout=None,  # or int seconds
        logger=app_log
    )
    # view execution info
    app_execution_obj.view(table_name='execution_info', table_print=view_table)
    # run execution obj
    app_execution_info = app_execution_obj.run(
        dry_run = app_execution.get("description", {}).get("dry_run", dry_run))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION ANALYSIS
    # analyze execution obj
    app_analyzer_obj = app_execution_obj.analyze(app_execution_info)

    # view execution info
    app_analyzer_obj.view(table_name='execution_info', table_print=view_table)
    # dump execution info ['ascii', 'json', 'env']
    app_analyzer_obj.write_to(
        path=app_info['path'], file_name=app_info['file_name'],
        objects=time_tags, fmt=app_info['format'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION CHECKS
    # Do not mark failed subprocesses successful merely because run() returned.
    if not dry_run:
        # Fail closed until the actual ExecutionAnalyzer status contract is wired.
        raise NotImplementedError('Map ExecutionAnalyzer success/returncode to a validated status here')

    return {'run_id': member_tags['run_id'], 'namelist': app_namelist['file']['location'],
            'dry_run': dry_run, 'analysis': str(app_analyzer_obj)}
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------
