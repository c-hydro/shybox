"""
Library Features:

Name:          ts_collections_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261119'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries

import re
import pandas as pd

from shybox.generic_toolkit.lib_utils_ensemble import generate_ensemble_list
from shybox.processing_ts_toolkit.lib_proc_join import join_time_series_by_registry as _join_by_registry

# common decoretors
from shybox import with_logger, as_process

# methods settings
_COLLECTION_DECORATORS = {
    "input_type": "pandas",
    "input_adapter": {
        "data": [
            {"method": "mapping_keys_by_separator", "kwargs": {"key_separator": "|", "other_separator": "::"}},
            {"method": "adapter_variables_to_group"},
        ],
    },
    "output_adapter": {"data": {"method": "fx_to_dict"}},
    "output_type": "pandas",
    "lazy_undefined_args": True,
    "lazy_undefined_value": None,
}

# member and tag template(s)
_MEMBER_TAG = re.compile(r"data\|([0-9]+)(?:@time\|[0-9]+)?")
_TIME_TAG = re.compile(r"time\|[0-9]+")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapt to join series in deterministic mode
@as_process(**_COLLECTION_DECORATORS)
@with_logger(var_name='logger_stream')
def join_time_series_deterministic(data, sections_hmc=None, sections_db=None, **kwargs):

    # info method start
    logger_stream.info_up('Create deterministic time-series ... ')
    # call method to join time-series (deterministic mode)
    ts = _join_by_registry(data, sections_hmc=sections_hmc, sections_db=sections_db, **kwargs)

    # check time-series object
    if ts is None:
        logger_stream.warning('Deterministic time-series are defined by NoneType')
        logger_stream.info_down('Create deterministic time-series ... FAILED')
        return None

    # add attributes
    ts.attrs['members'] = 'deterministic'

    # info method end
    logger_stream.info_down('Create deterministic time-series ... DONE')

    return ts
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapt to join series in probabilistic mode
@as_process(**_COLLECTION_DECORATORS)
@with_logger(var_name='logger_stream')
def join_time_series_probabilistic(
        data, sections_hmc=None, sections_db=None, *,
        ensemble_members=None, ensemble_variables=(), ensemble_format="%03d", **kwargs):

    # info method start
    logger_stream.info_up('Create probabilistic time-series ... ')

    # check members name(s)
    members = None
    if ensemble_members is not None:
        members = [str(member) for member in ensemble_members]
        if not members or "" in members or len(set(members)) != len(members):
            logger_stream.error('Ensemble members IDs are duplicated')
            raise ValueError("Ensemble member IDs must be nonempty and unique.")
    else:
        # info ensembles names
        logger_stream.warning('Ensemble members IDs are not provided. Try to use the data dimensions')

        # the highest member position includes gaps and ignores repeated time steps.
        last_member = max(
            (int(match[1]) for value in data.values() if isinstance(value, dict)
             for tag in value if (match := _MEMBER_TAG.fullmatch(tag))),
            default=0,
        )
        if last_member:
            members = generate_ensemble_list(1, last_member, ensemble_format=ensemble_format)

    # manage series and labels
    series, labels = {}, {}
    for variable, value in data.items():

        # info variable start
        logger_stream.info_up(f'Variable {variable!r} ... ')

        # organize by member
        by_member = {}
        if isinstance(value, dict):
            for tag, frame in value.items():

                # info member start
                logger_stream.info_up(f'Member {tag!r} ... ')

                # match member name or time field
                match = _MEMBER_TAG.fullmatch(tag)
                if match:
                    index = int(match[1])
                    if members is None or not 1 <= index <= len(members):
                        logger_stream.error('Member IDs are not valid.')
                        raise ValueError(f"Data index {index} is outside the configured ensemble.")
                    member = members[index - 1]
                    # info status
                    logger_stream.info_status(f'Member is defined by tag {member!r}')
                elif _TIME_TAG.fullmatch(tag):
                    member = ""
                else:
                    logger_stream.error('Member time-series is not valid')
                    raise ValueError(f"Invalid time-series tag {tag!r} for {variable!r}.")

                # set dataframe in by_member object (probabilistic)
                by_member.setdefault(member, []).append(frame)

                # info member start
                logger_stream.info_down(f'Member {tag!r} ... DONE')
        else:
            # set dataframe in by_member object (deterministic)
            by_member[""] = [value]

        # check variables
        if variable in ensemble_variables and "" in by_member:
            if members is None or len(members) != 1 or len(by_member) != 1:
                raise ValueError(f"Expected member-indexed series for {variable!r}.")
            by_member[members[0]] = by_member.pop("")
        if "" in by_member and len(by_member) > 1:
            raise ValueError(f"Variable {variable!r} mixes shared and member-indexed series.")
        if members is not None and (variable in ensemble_variables or (by_member and "" not in by_member)):
            by_member = {member: by_member.get(member, []) for member in members}

        # create the common series (deterministic and probabilistic types)
        for member, frames in by_member.items():

            # define tag for each variable
            if member == "":
                tag = "deterministic"
            else:
                tag = f"member {member}"

            # info member start
            logger_stream.info_up(f'Add variable {variable} as {tag} ... ')

            # insert to series object
            available = [frame for frame in frames if frame is not None]
            key = str((variable, member))
            series[key] = pd.concat(available) if len(available) > 1 else next(iter(available), None)
            labels[key] = (variable, member)

            # info member start
            logger_stream.info_down(f'Add variable {variable} as {tag} ... DONE')

        # info variable end
        logger_stream.info_down(f'Variable {variable!r} ... DONE')

    # call joine by registry method
    joined = _join_by_registry(series, sections_hmc=sections_hmc, sections_db=sections_db, **kwargs)
    # check time-series object
    if joined is None:
        logger_stream.warning('Probabilistic time-series are defined by NoneType')
        logger_stream.info_down('Create probabilistic time-series ... FAILED')
        return None

    # define dataframe columns
    joined.columns = pd.MultiIndex.from_tuples(
        [(labels[key][0], section, labels[key][1]) for key, section in joined.columns],
        names=["variable", "section", "member"],
    )
    # define dataframe attrs
    joined.attrs["datasets"] = list(dict.fromkeys(joined.columns.get_level_values("variable")))
    joined.attrs['members'] = ';'.join(members or ())

    # info method end
    logger_stream.info_down('Create probabilistic time-series ... DONE')

    return joined
# ----------------------------------------------------------------------------------------------------------------------
