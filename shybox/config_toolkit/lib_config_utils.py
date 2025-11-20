"""
Library Features:

Name:          lib_config_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251120'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import copy
import re

from shybox.logging_toolkit.lib_logging_utils import with_logger

# anchor regex to find {placeholders} in strings
_PLACEHOLDER_RE = re.compile(r"{([^{}]+)}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to clean quotes from string values
def clean_value_quotes(value):
    """
    Remove surrounding single or double quotes from a string if present.
    Example:
        "'marche'" -> "marche"
        "\"marche\"" -> "marche"
        "marche" -> "marche"
    """
    if isinstance(value, str):
        v = value.strip()
        if (v.startswith("'") and v.endswith("'")) or \
           (v.startswith('"') and v.endswith('"')):
            return v[1:-1]
    return value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to sanitize string values
def sanitize_lut_quotes(lut: dict) -> dict:
    """
    Remove outer quotes from all LUT string values.
    """
    for k, v in list(lut.items()):
        lut[k] = clean_value_quotes(v)
    return lut
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to autofill mapping placeholders
def autofill_mapping(
    lut: dict,
    extra_tags: dict | None = None,
    max_iter: int = 3,
    strict: bool = False,
    extend_lut: bool = True,
) -> dict:
    """
    Resolve {key} placeholders inside LUT values using the LUT itself
    plus optional extra tags.

    Example
    -------
    lut = {
        "execution_name": "hmc_dataset",
        "domain_name": "'marche'",
        "file_log": "{execution_name}_{domain_name}.log",
    }

    autofill_lut(lut)  ->
        lut["file_log"] == "hmc_dataset_'marche'.log"

    Parameters
    ----------
    lut : dict
        Mapping to be filled in-place.
    extra_tags : dict | None
        Additional {tag: value} pairs available during formatting.
        If extend_lut=True, they are also written into lut.
    max_iter : int
        Max number of passes (for nested placeholders).
    strict : bool
        If True, raise KeyError if after all iterations some placeholders
        are still unresolved. If False, leave them as-is.
    extend_lut : bool
        If True, extra_tags are merged into lut (updating/adding keys).

    Returns
    -------
    dict
        The same lut object, updated.
    """
    if extra_tags:
        if extend_lut:
            lut.update(extra_tags)
        # tags is what we actually format with
        tags = {**lut, **extra_tags}
    else:
        tags = dict(lut)

    unresolved_keys: set[str] = set()

    for _ in range(max_iter):
        changed = False
        unresolved_keys.clear()

        for key, value in list(lut.items()):
            if not isinstance(value, str):
                continue
            if "{" not in value or "}" not in value:
                continue

            try:
                new_value = value.format(**tags)
            except KeyError as e:
                unresolved_keys.add(str(e).strip("'"))
                continue
            except Exception:
                # any other formatting problem -> skip for safety
                continue

            if new_value != value:
                lut[key] = new_value
                tags[key] = new_value
                changed = True

        if not changed:
            break

    if strict and unresolved_keys:
        raise KeyError(
            "autofill_lut: unresolved placeholder(s): "
            + ", ".join(sorted(unresolved_keys))
        )

    return lut
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers to fill object with mapping
def _normalize_path_like_string(value: str) -> str:
    """
    Normalize path-like strings by collapsing repeated slashes `//` → `/`,
    while preserving URI schemes like `http://`, `https://`, `s3://`, etc.
    """
    if not isinstance(value, str):
        return value

    # Matches "<scheme>://<rest>", e.g. "http://", "s3://", "ftp://"
    m = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*://)(.*)$", value)
    if m:
        scheme, rest = m.groups()
        # collapse slashes only in the path part
        rest = re.sub(r"//+", "/", rest)
        return scheme + rest

    # No scheme → assume filesystem-like path
    return re.sub(r"//+", "/", value)

def _fill_obj_recursive(obj, tags: dict, strict: bool, unresolved: set):
    """
    Internal: recursively walk obj and format strings with tags.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _fill_obj_recursive(v, tags, strict, unresolved)
        return obj

    if isinstance(obj, list):
        return [_fill_obj_recursive(v, tags, strict, unresolved) for v in obj]

    if isinstance(obj, tuple):
        return tuple(_fill_obj_recursive(v, tags, strict, unresolved) for v in obj)

    if isinstance(obj, str):
        s = obj

        if "{" in s and "}" in s:
            def repl(match: re.Match) -> str:
                key = match.group(1)
                if key in tags:
                    return str(tags[key])
                # unknown placeholder
                unresolved.add(key)
                if strict:
                    # In strict mode we still perform the replacement,
                    # but we'll raise later in fill_with_mapping
                    return match.group(0)
                else:
                    # leave {key} unchanged
                    return match.group(0)

            s = _PLACEHOLDER_RE.sub(repl, s)

        # 🔥 normalize path-like strings (remove accidental `//`)
        s = _normalize_path_like_string(s)
        return s

    # int, float, None, etc.
    return obj


# method to fill object with mapping
def fill_with_mapping(
    obj,
    lut: dict,
    extra_tags: dict | None = None,
    strict: bool = False,
    in_place: bool = True,
) -> dict:
    """
    Fill placeholders {tag} in any nested object (e.g. application dict)
    using values from lut plus optional extra_tags.

    Example
    -------
    application["data_source"]["air_t"]["file_name"]
        = "t2m_{time_source}.grib"

    lut = {
        "time_source": "20241017_0600",
        "path_source": "/data/icon",
        ...
    }

    fill_object_with_lut(application, lut) ->
        "t2m_20241017_0600.grib"

    Parameters
    ----------
    obj : any
        Usually a dict (e.g. application), but can contain nested lists/tuples.
    lut : dict
        Base mapping of tags to values.
    extra_tags : dict | None
        Extra tags available during formatting (not forced into lut).
    strict : bool
        If True, raise KeyError if some placeholders cannot be resolved.
        If False, leave unresolved placeholders unchanged.
    in_place : bool
        If True, modify `obj` in-place and return it. If False, work on
        a deep copy and return the copy.

    Returns
    -------
    any
        Filled object (same type as obj; dict if obj is dict).
    """
    # tags used for formatting: lut + extra (extra wins on conflict)
    tags = dict(lut)
    if extra_tags:
        tags.update(extra_tags)

    target = obj if in_place else copy.deepcopy(obj)

    unresolved: set[str] = set()
    result = _fill_obj_recursive(target, tags, strict, unresolved)

    if strict and unresolved:
        raise KeyError(
            "fill_object_with_lut: unresolved placeholder(s): "
            + ", ".join(sorted(unresolved))
        )

    return result
# ----------------------------------------------------------------------------------------------------------------------
