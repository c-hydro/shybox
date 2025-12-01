"""
Class Features

Name:          config_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251120'
Version:       '1.6.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import json
import os
import warnings
import re
import copy
import numpy as np
import pandas as pd

from typing import Iterable
from tabulate import tabulate

from shybox.config_toolkit.lib_config_utils import (
    autofill_mapping, fill_with_mapping, sanitize_lut_quotes, _normalize_path_like_string)
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Config class
class ConfigManager:
    """
    Wrapper around a configuration root (default: 'settings'), with
    full support for:
      - priority / flags / variables / application (application is configurable)
      - optional sections
      - LUT merging (stored only in variables['lut'] or self.lut)
      - LUT/template/format validation (strict or lazy)
      - optional filling of None values from template time formats
      - resolving time templates using datetime or pandas.Timestamp
      - overriding LUT values from system environment variables
      - flatten/unflatten nested configuration (lut/format/template),
        optionally lifting them out of `variables` into top-level
        attributes: self.lut, self.format, self.template
      - application wrappers via ApplicationConfig
      - environment expansion in arbitrary objects
    """

    # --------------------------------------------------------------
    # Constructor
    def __init__(
        self,
        settings: dict,
        auto_merge_lut: bool = True,
        auto_env_override: bool = True,
        env_warn_missing: bool = True,
        auto_validate: bool = True,
        strict_validation: bool = True,
        apply_time_template_for_none: bool = True,
        wrap_time_template_in_braces: bool = False,
        flat_variables: bool = False,
        flat_separator: str = ":",
        flat_key_mode: str = "key:value",
        auto_fill_lut: bool = True,
        convert_none_to_nan: bool = False,
        application_key: str | None = "application",
    ):

        if settings is None:
            raise ValueError("Settings object is None.")

        # Internal (optional) raw config storage (set by from_source)
        self._raw_config: dict | None = None
        self._root_key: str | None = None

        # track keys that come from the reference LUT (e.g. "environment")
        self._env_lut_keys: set[str] = set()

        # Behaviour flags
        self._auto_merge_lut = auto_merge_lut
        self._auto_env_override = auto_env_override
        self._env_warn_missing = env_warn_missing
        self._auto_validate = auto_validate
        self._strict_validation = strict_validation
        self._apply_time_template_for_none = apply_time_template_for_none
        self._wrap_time_template_in_braces = wrap_time_template_in_braces
        self._flat_variables = flat_variables
        self._flat_separator = flat_separator
        self._flat_key_mode = flat_key_mode
        self._auto_fill_lut = auto_fill_lut
        self._convert_none_to_nan = convert_none_to_nan

        # Application key name (can be None if no application is mandatory)
        self._application_key = application_key

        # Mandatory sections
        self._init_mandatory_sections(settings, application_key=application_key)

        # 1) merge LUT (combine user/environment into a single LUT)
        if self._auto_merge_lut:
            self.merge_lut_by_priority()

        # 2) env overrides (LUT values are env var names)
        if self._auto_env_override:
            self.update_lut_from_env(
                keys=None,
                warn_missing=self._env_warn_missing,
                cast_types=True,
            )

        # 3) validate (and apply template-for-None)
        if self._auto_validate:
            self.validate_variables_keys(
                strict=self._strict_validation,
                apply_time_template_for_none=self._apply_time_template_for_none,
                wrap_time_template_in_braces=self._wrap_time_template_in_braces,
            )

        # 4) optional LUT autofill (keeps external use unchanged)
        if self._auto_fill_lut:
            self.autofill_lut()

        # 5) optionally flatten lut/format/template and lift out of variables
        if self._flat_variables:
            self.flatten_variables(
                which=("lut", "format", "template"),
                sep=self._flat_separator,
                key_mode=self._flat_key_mode,
            )

    # --------------------------------------------------------------
    # Mandatory sections initializer
    def _init_mandatory_sections(self, settings: dict, application_key: str | None = "application") -> None:
        """
        Initialize core sections from the 'settings' dict.

        - Always mandatory: priority, flags, variables
        - Application section:
            * if application_key is None -> no mandatory application
            * else                        -> that key must exist in settings
        """
        required_base = ("priority", "flags", "variables")
        missing_base = [k for k in required_base if k not in settings]
        if missing_base:
            raise KeyError(f"Missing mandatory settings section(s): {', '.join(missing_base)}")

        self.priority = settings["priority"]
        self.flags = settings["flags"]
        self.variables = settings["variables"]

        if application_key is None:
            # No mandatory application section; expose empty dict
            self.application = {}
        else:
            if application_key not in settings:
                raise KeyError(
                    f"Missing mandatory application section '{application_key}' in settings."
                )
            # Expose it as generic attribute 'application'
            self.application = settings[application_key]

    # --------------------------------------------------------------
    # CLASS METHOD: load from dict / JSON string / file
    @classmethod
    def from_source(
        cls,
        src,
        root_key: str = "settings",
        auto_merge_lut: bool = True,
        auto_env_override: bool = True,
        env_warn_missing: bool = True,
        auto_validate: bool = True,
        strict_validation: bool = True,
        apply_time_template_for_none: bool = True,
        wrap_time_template_in_braces: bool = False,
        flat_variables: bool = False,
        flat_separator: str = ":",
        flat_key_mode: str = "key:value",
        auto_fill_lut: bool = True,
        convert_none_to_nan: bool = True,
        application_key: str | None = "application",
    ) -> "ConfigManager":
        """
        Create Config from:
            - dict
            - JSON string
            - file path
        """

        # Case 1: Already dict
        if isinstance(src, dict):
            data = src

        # Case 2: JSON string
        elif isinstance(src, str) and src.strip().startswith("{"):
            data = json.loads(src)

        # Case 3: File path
        elif isinstance(src, str):
            if not os.path.isfile(src):
                raise FileNotFoundError(f"JSON file not found: {src}")
            with open(src, "r") as f:
                data = json.load(f)

        else:
            raise TypeError(f"Unsupported source type: {type(src)}")

        if root_key not in data:
            raise KeyError(f"Root key '{root_key}' not found in configuration.")

        settings_section = data[root_key]

        obj = cls(
            settings_section,
            auto_merge_lut=auto_merge_lut,
            auto_env_override=auto_env_override,
            env_warn_missing=env_warn_missing,
            auto_validate=auto_validate,
            strict_validation=strict_validation,
            apply_time_template_for_none=apply_time_template_for_none,
            wrap_time_template_in_braces=wrap_time_template_in_braces,
            flat_variables=flat_variables,
            flat_separator=flat_separator,
            flat_key_mode=flat_key_mode,
            auto_fill_lut=auto_fill_lut,
            convert_none_to_nan=convert_none_to_nan,
            application_key=application_key,
        )
        obj._raw_config = data
        obj._root_key = root_key
        return obj

    # --------------------------------------------------------------
    # Unified accessor for mandatory / optional sections
    def get_section(
        self,
        section: str | None = None,
        root_key: str | None = None,
        raise_if_missing: bool = False,
    ):
        """
        Retrieve a configuration section, checking both mandatory
        sections (priority, flags, variables, application) and
        optional sections.

        Parameters
        ----------
        section : str | None
            Name of the section to retrieve. If None, defaults to
            the configured application key (or "application").
        root_key : str | None
            Root key to use when searching in the raw config for
            optional sections. If None, use self._root_key.
        raise_if_missing : bool
            If True, raise KeyError when the section cannot be found
            in either the mandatory attributes or the optional
            sections. If False, return None.
        """

        default_app_name = getattr(self, "_application_key", None) or "application"
        name = section or default_app_name

        # ------------------------------------------------------------------
        # SPECIAL CASE: treat "lut", "format", "template" as pseudo-sections
        # ------------------------------------------------------------------
        if name in ("lut", "format", "template"):
            src = None

            # Prefer lifted top-level attribute if available (self.lut, self.format, self.template)
            if hasattr(self, name) and isinstance(getattr(self, name), dict):
                src = getattr(self, name)
            else:
                # Fallback to variables[name]
                vars_dict = getattr(self, "variables", None)
                if isinstance(vars_dict, dict) and name in vars_dict:
                    src = vars_dict[name]

            if src is None:
                if raise_if_missing:
                    raise KeyError(f"Section '{name}' not found (no {name} in variables).")
                return None

            # Optionally convert None → np.nan
            if getattr(self, "_convert_none_to_nan", False):
                src = self._convert_none_to_nan_recursive(copy.deepcopy(src))
            else:
                src = copy.deepcopy(src)

            # Expand $HOME (and other uppercase env vars) inside this object
            src = self.expand_env(src, deep_copy=False)

            return src

        # ------------------------------------------------------------------
        # NORMAL SECTIONS: priority, flags, variables, application, etc.
        # ------------------------------------------------------------------

        # 1) mandatory sections first
        mandatory_map = {
            "priority": getattr(self, "priority", None),
            "flags": getattr(self, "flags", None),
            "variables": getattr(self, "variables", None),
            "application": getattr(self, "application", None),
        }

        if name in mandatory_map and mandatory_map[name] is not None:
            section_data = mandatory_map[name]

            # Work on a copy to avoid mutating internal state
            section_copy = copy.deepcopy(section_data)

            # Expand env ($HOME, $RUN, ...) in this view
            section_copy = self.expand_env(section_copy, deep_copy=False)

            if getattr(self, "_convert_none_to_nan", False):
                section_copy = self._convert_none_to_nan_recursive(section_copy)

            return section_copy

        # 2) optional sections via raw config (if available)
        if self._raw_config is not None:
            # first try under the main root (e.g. 'settings' or 'configuration')
            main_root = root_key if root_key is not None else self._root_key

            section_data = None

            if main_root is not None:
                section_data = self.search_optional_section(
                    section_key=name,
                    root_key=main_root,
                    raise_if_missing=False,
                )

            # if not found under root, try as top-level
            if section_data is None:
                section_data = self.search_optional_section(
                    section_key=name,
                    root_key=None,  # top-level
                    raise_if_missing=raise_if_missing,
                )

            if section_data is not None:
                section_copy = copy.deepcopy(section_data)
                section_copy = self.expand_env(section_copy, deep_copy=False)

                if getattr(self, "_convert_none_to_nan", False):
                    section_copy = self._convert_none_to_nan_recursive(section_copy)

                return section_copy

        # 3) nothing found
        if raise_if_missing:
            raise KeyError(f"Section '{name}' not found in mandatory or optional config.")

        return None


    # --------------------------------------------------------------
    # Helper: get an ApplicationConfig wrapper
    def get_application(
        self,
        section_name: str | None = None,
        root_key: str | None = None,
    ) -> "ApplicationConfig":
        """
        Convenience factory for ApplicationConfig.

        Parameters
        ----------
        section_name : str | None
            Name of the application section.
            - If None, use self._application_key (default: 'application').
        root_key : str | None
            - None: section is expected at top-level in the raw config.
            - non-None: section is expected under raw_config[root_key].

        Returns
        -------
        ApplicationConfig

        Raises
        ------
        KeyError
            If the requested section cannot be found.
        """
        # Decide application section name
        if section_name is None:
            default_app_name = getattr(self, "_application_key", None) or "application"
            section_name = default_app_name

        # Auto-detect root_key if not explicitly provided
        if root_key is None:
            if (
                hasattr(self, "_root_key")
                and self._root_key is not None
                and section_name == getattr(self, "_application_key", None)
            ):
                root_key = self._root_key

        # Early check
        _ = self.get_section(section_name, root_key=root_key, raise_if_missing=True)

        return ApplicationConfig(self, section_name, root_key=root_key)

    # --------------------------------------------------------------
    # Priority handling
    def select_variable_priority(self) -> tuple[str, str]:
        if self.priority is None:
            raise ValueError("Priority section is missing.")

        required = ("reference", "other")
        missing = [k for k in required if k not in self.priority]
        if missing:
            raise KeyError(f"Missing priority key(s): {', '.join(missing)}")

        return self.priority["reference"], self.priority["other"]

    # --------------------------------------------------------------
    # Merge LUT by priority (and update variables['lut'])
    def merge_lut_by_priority(self) -> dict:
        """
        Merge LUTs according to priority and update variables['lut'] in place.

        - Takes self.variables['lut'][other] and self.variables['lut'][reference]
        - Builds a single merged LUT where reference values override other
        - Stores the merged LUT ONLY in:
            * self.variables['lut']  (overwriting the old structure)

        Additionally:
        - self._env_lut_keys is set to the set of keys coming from the
          reference LUT (e.g. "environment") so that env overrides only
          apply to those keys.
        """
        ref_key, other_key = self.select_variable_priority()

        lut = self.variables.get("lut", {})
        if ref_key not in lut:
            raise KeyError(f"Reference LUT '{ref_key}' missing.")
        if other_key not in lut:
            raise KeyError(f"Other LUT '{other_key}' missing.")

        other_dict = lut[other_key] or {}
        ref_dict = lut[ref_key] or {}

        # remember which keys come from reference (environment)
        self._env_lut_keys = set(ref_dict.keys())

        merged = dict(other_dict)
        merged.update(ref_dict)

        self.variables["lut"] = merged

        return merged

    # --------------------------------------------------------------
    # Helper: detect if a template string is a "time template"
    @staticmethod
    def _is_time_template(template_value: str) -> bool:
        if not isinstance(template_value, str):
            return False
        return bool(re.search(r"%[a-zA-Z]", template_value))

    # --------------------------------------------------------------
    # Validate LUT structure (strict or lazy) + apply template to None
    def validate_variables_keys(
        self,
        lut: dict | None = None,
        strict: bool = True,
        apply_time_template_for_none: bool | None = None,
        wrap_time_template_in_braces: bool | None = None,
    ) -> dict:

        if lut is None:
            # Prefer lifted version if present
            if hasattr(self, "lut") and isinstance(getattr(self, "lut"), dict):
                lut = self.lut
            else:
                lut = self.variables.get("lut", None)
        if lut is None:
            raise ValueError("LUT not available.")

        # get format / template from variables or lifted
        fmt = None
        template = None
        if hasattr(self, "format") and isinstance(getattr(self, "format"), dict):
            fmt = self.format
        else:
            fmt = self.variables.get("format", {})
        if hasattr(self, "template") and isinstance(getattr(self, "template"), dict):
            template = self.template
        else:
            template = self.variables.get("template", {})

        keys_lut = set(lut.keys())
        keys_fmt = set(fmt.keys())
        keys_tpl = set(template.keys())

        result = {
            "lut_not_in_format": sorted(keys_lut - keys_fmt),
            "lut_not_in_template": sorted(keys_lut - keys_tpl),
            "format_not_in_lut": sorted(keys_fmt - keys_lut),
            "template_not_in_lut": sorted(keys_tpl - keys_lut),
        }

        has_mismatch = any(result.values())

        # STRICT
        if has_mismatch and strict:
            msg = "Variables dict mismatch (strict mode):\n" + json.dumps(result, indent=2)
            raise ValueError(msg)

        # LAZY
        if has_mismatch and not strict:
            missing_in_lut = set(result["format_not_in_lut"]) | set(result["template_not_in_lut"])
            for key in missing_in_lut:
                if key not in lut:
                    lut[key] = None

            # store back to correct place
            if hasattr(self, "lut") and self.lut is lut:
                self.lut = lut
            elif hasattr(self, "variables") and isinstance(self.variables, dict):
                self.variables["lut"] = lut

            warn_msg_parts = []
            if result["lut_not_in_format"]:
                warn_msg_parts.append(
                    f"LUT keys without format definition: {', '.join(result['lut_not_in_format'])}"
                )
            if result["lut_not_in_template"]:
                warn_msg_parts.append(
                    f"LUT keys without template definition: {', '.join(result['lut_not_in_template'])}"
                )
            if result["format_not_in_lut"]:
                warn_msg_parts.append(
                    f"Format keys missing in LUT (set to None): {', '.join(result['format_not_in_lut'])}"
                )
            if result["template_not_in_lut"]:
                warn_msg_parts.append(
                    "Template keys missing in LUT (set to None if not present): "
                    + ", ".join(result["template_not_in_lut"])
                )

            if warn_msg_parts:
                warnings.warn(
                    "Variables dict mismatch (lazy mode): " + " | ".join(warn_msg_parts),
                    UserWarning,
                )

        # Apply time template for None
        use_time_template = (
            self._apply_time_template_for_none
            if apply_time_template_for_none is None
            else apply_time_template_for_none
        )
        wrap_in_braces = (
            self._wrap_time_template_in_braces
            if wrap_time_template_in_braces is None
            else wrap_time_template_in_braces
        )

        if use_time_template:
            for key, value in list(lut.items()):
                if value is None and key in template:
                    tmpl_val = template[key]
                    if self._is_time_template(tmpl_val):
                        new_val = tmpl_val
                        if wrap_in_braces:
                            new_val = "{" + tmpl_val + "}"
                        lut[key] = new_val

            if hasattr(self, "lut") and self.lut is lut:
                self.lut = lut
            elif hasattr(self, "variables") and isinstance(self.variables, dict):
                self.variables["lut"] = lut

        return result

    # --------------------------------------------------------------
    # Resolve time templates using datetime or pandas.Timestamp
    def resolve_time_templates(
        self,
        when=None,
        lut: dict | None = None,
        update_variables: bool = False,
    ) -> dict:

        if lut is None:
            if hasattr(self, "lut") and isinstance(getattr(self, "lut"), dict):
                lut = self.lut
            else:
                lut = self.variables.get("lut", None)
        if lut is None:
            raise ValueError("LUT not available.")

        if when is None:
            return dict(lut)

        is_pandas_ts = hasattr(when, "to_pydatetime") and callable(getattr(when, "to_pydatetime"))
        if is_pandas_ts:
            when_dt = when.to_pydatetime()
        elif hasattr(when, "strftime"):
            when_dt = when
        else:
            raise TypeError(f"'when' must be datetime or pandas.Timestamp, not {type(when)}")

        resolved = {}

        for key, value in lut.items():
            if not isinstance(value, str):
                resolved[key] = value
                continue

            template = value.strip()
            if template.startswith("{") and template.endswith("}"):
                template_inner = template[1:-1]
            else:
                template_inner = template

            if self._is_time_template(template_inner):
                try:
                    resolved_value = when_dt.strftime(template_inner)
                except Exception:
                    resolved_value = value
            else:
                resolved_value = value

            resolved[key] = resolved_value

        if update_variables:
            if hasattr(self, "lut") and self.lut is lut:
                self.lut = resolved
            elif hasattr(self, "variables") and isinstance(self.variables, dict):
                self.variables["lut"] = resolved

        return resolved

    def _collect_time_keys(self) -> set[str]:
        """
        Detect time-like LUT keys using:
          - variables['format'][key] == 'time' (preferred)
          - OR template[key] looks like a time template (contains %X)
          - OR key name matches 'time_*' as a fallback.
        """

        # decide which LUT container we operate on
        if hasattr(self, "lut") and isinstance(self.lut, dict):
            lut = self.lut
        else:
            lut = self.variables.get("lut", {}) if hasattr(self, "variables") else {}

        # get format / template containers
        if hasattr(self, "format") and isinstance(self.format, dict):
            fmt = self.format
        else:
            fmt = self.variables.get("format", {}) if hasattr(self, "variables") else {}

        if hasattr(self, "template") and isinstance(self.template, dict):
            template = self.template
        else:
            template = self.variables.get("template", {}) if hasattr(self, "variables") else {}

        time_keys: set[str] = set()

        # 1) format == "time"
        for k, t in fmt.items():
            if isinstance(t, str) and t.strip().lower() == "time":
                if k in lut:
                    time_keys.add(k)

        # 2) template is a time template (for back-compat)
        for k, tmpl in template.items():
            if self._is_time_template(tmpl):
                if k in lut:
                    time_keys.add(k)

        # 3) fallback on key name (time_*), but don't force if not in LUT
        for k in lut.keys():
            if k.startswith("time_"):
                time_keys.add(k)

        return time_keys

    # --------------------------------------------------------------
    # Override LUT values from system environment variables
    def update_lut_from_env(
        self,
        keys: list[str] | None = None,
        warn_missing: bool = True,
        cast_types: bool = True,
    ) -> dict:
        """
        Update LUT values using system environment variables.

        LUT organisation (by default):
            lut = {
                "path_source": "PATH_SRC",    # ref (environment)
                "time_rounding": "H",         # user-only default
                ...
            }

        Behaviour
        ---------
        - ONLY keys coming from the reference LUT (environment) are
          overridden by environment variables, unless `keys` is explicitly
          given.
        - Keys that exist only in the 'other' LUT (user) are left as-is.
          This keeps user-defined defaults like 'H', 'forward', etc.

        If the environment variable is NOT found for a reference key:
            - warn the user (optional)
            - set lut[cfg_key] = None

        Casting uses variables['format'][cfg_key] when available.
        """

        # choose LUT container
        if hasattr(self, "lut") and isinstance(getattr(self, "lut"), dict):
            lut = self.lut
            lut_is_attr = True
        else:
            lut = self.variables.get("lut", None)
            lut_is_attr = False

        if lut is None:
            raise ValueError("LUT not available.")

        # choose format container
        if hasattr(self, "format") and isinstance(getattr(self, "format"), dict):
            fmt = self.format
        else:
            fmt = self.variables.get("format", {})

        # decide which keys to check
        if keys is not None:
            keys_to_check = list(keys)
        elif getattr(self, "_env_lut_keys", None):
            keys_to_check = list(self._env_lut_keys)
        else:
            keys_to_check = list(lut.keys())

        updated = []
        missing = []
        cast_failed = []

        for cfg_key in keys_to_check:
            if cfg_key not in lut:
                continue

            env_var_name = lut[cfg_key]

            # no valid env var name → set to None for env-driven keys
            if not isinstance(env_var_name, str) or not env_var_name:
                lut[cfg_key] = None
                missing.append(cfg_key)
                continue

            if env_var_name in os.environ:
                raw_val = os.environ[env_var_name]
                new_val = raw_val

                if cast_types:
                    type_decl = fmt.get(cfg_key, "string")
                    if isinstance(type_decl, str):
                        t = type_decl.strip().lower()
                    else:
                        t = "string"

                    try:
                        if t in ("int", "integer"):
                            new_val = int(raw_val)
                        elif t in ("float", "double", "number"):
                            new_val = float(raw_val)
                        elif t in ("time", "datetime"):
                            new_val = raw_val
                        else:
                            new_val = raw_val
                    except Exception:
                        cast_failed.append(cfg_key)
                        new_val = raw_val

                lut[cfg_key] = new_val
                updated.append(cfg_key)
            else:
                lut[cfg_key] = None
                missing.append(cfg_key)

        # write back
        if lut_is_attr:
            self.lut = lut
        else:
            self.variables["lut"] = lut

        if warn_missing and missing:
            warnings.warn(
                "Environment variables missing for LUT keys (set to None): "
                + ", ".join(missing),
                UserWarning,
            )

        if cast_failed:
            warnings.warn(
                "Failed to cast environment values for LUT keys: "
                + ", ".join(cast_failed),
                UserWarning,
            )

        return {"updated": updated, "missing": missing, "cast_failed": cast_failed}

    # --------------------------------------------------------------
    # Optional section search
    def search_optional_section(
        self,
        section_key: str,
        root_key: str | None = None,
        raise_if_missing: bool = False,
    ):
        """
        Search an optional section either:

          - under a root key (e.g. config['settings'][section_key]),
            if root_key is provided or self._root_key is set

          - or at top-level (config[section_key]) if root_key is None
            and no self._root_key is used.
        """
        if self._raw_config is None:
            raise ValueError("Raw config not available (use from_source).")

        config = self._raw_config

        # TOP-LEVEL MODE: no root key → look directly in config
        if root_key is None:
            if section_key in config:
                return config[section_key]
            if raise_if_missing:
                raise KeyError(f"Optional top-level section '{section_key}' not found.")
            return None

        # ROOTED MODE: look under config[root_key]
        if root_key not in config:
            if raise_if_missing:
                raise KeyError(f"Root key '{root_key}' not found.")
            return None

        root_dict = config[root_key]

        if section_key not in root_dict:
            if raise_if_missing:
                raise KeyError(f"Optional section '{section_key}' not found in '{root_key}'.")
            return None

        return root_dict[section_key]

    # --------------------------------------------------------------
    # FLATTEN nested dict (ANY number of levels) with key modes
    def flatten_obj(
        self,
        obj: dict,
        path_prefix: str = "",
        sep: str = ":",
        key_mode: str = "key:value",
        flat: dict | None = None,
    ) -> dict:

        if flat is None:
            flat = {}

        for k, v in obj.items():
            path = f"{path_prefix}{sep}{k}" if path_prefix else k

            if isinstance(v, dict):
                self.flatten_obj(
                    v,
                    path_prefix=path,
                    sep=sep,
                    key_mode=key_mode,
                    flat=flat,
                )
            else:
                segments = path.split(sep)

                if key_mode == "key:value":
                    new_key = path
                elif key_mode == "value":
                    new_key = segments[-1]
                elif key_mode == "key":
                    new_key = segments[0]
                else:
                    raise ValueError(
                        f"Unknown key_mode '{key_mode}'. "
                        "Use 'key:value', 'value', or 'key'."
                    )

                if new_key in flat:
                    raise ValueError(
                        f"Flattening conflict: key '{new_key}' is produced more than once "
                        f"(current path='{path}'). Consider changing key_mode or "
                        f"separator, or fix your nested structure."
                    )

                flat[new_key] = v

        return flat

    # --------------------------------------------------------------
    # UNFLATTEN dict back to nested structure
    def unflatten_obj(self, flat: dict, sep: str = ":") -> dict:
        result = {}
        for composite_key, value in flat.items():
            keys = composite_key.split(sep)
            d = result
            for p in keys[:-1]:
                d = d.setdefault(p, {})
            d[keys[-1]] = value
        return result

    # --------------------------------------------------------------
    # Flatten selected variables dicts and lift them out of variables
    def flatten_variables(
        self,
        which=("lut", "format", "template"),
        sep=":",
        key_mode: str = "key:value",
    ):
        """
        Flatten selected variables dicts, then attach them as top-level
        attributes (self.lut, self.format, self.template). If ALL names
        in `which` are successfully flattened and lifted, remove
        self.variables from the object.
        """
        moved = []

        for name in which:
            # source: prefer variables[name] if variables exists
            src_obj = None
            if hasattr(self, "variables") and isinstance(self.variables, dict) and name in self.variables:
                src_obj = self.variables[name]
            elif hasattr(self, name) and isinstance(getattr(self, name), dict):
                src_obj = getattr(self, name)

            if not isinstance(src_obj, dict):
                continue

            flat_obj = self.flatten_obj(src_obj, sep=sep, key_mode=key_mode)
            setattr(self, name, flat_obj)
            moved.append(name)

        # if all requested names moved and variables exists, drop variables
        if hasattr(self, "variables") and all(n in moved for n in which):
            del self.variables

    # --------------------------------------------------------------
    # Unflatten selected variables dicts and (re)build self.variables
    def unflatten_variables(
        self,
        which=("lut", "format", "template"),
        sep=":",
    ):
        """
        Unflatten selected dicts (self.lut / self.format / self.template
        or variables['lut'] / ...) back to nested dictionaries and
        ensure self.variables exists and is synced.
        """
        # ensure we have a variables dict
        vars_dict = getattr(self, "variables", None)
        if not isinstance(vars_dict, dict):
            vars_dict = {}
            self.variables = vars_dict

        for name in which:
            # source: prefer top-level attribute, then variables[name]
            src_obj = None
            if hasattr(self, name) and isinstance(getattr(self, name), dict):
                src_obj = getattr(self, name)
            elif name in vars_dict and isinstance(vars_dict[name], dict):
                src_obj = vars_dict[name]

            if not isinstance(src_obj, dict):
                continue

            nested = self.unflatten_obj(src_obj, sep=sep)
            vars_dict[name] = nested
            setattr(self, name, nested)

    # --------------------------------------------------------------
    def _convert_none_to_nan_recursive(self, obj):
        """
        Recursively convert None → np.nan inside obj.
        Works on dict, list, tuple, and returns a new object.
        """
        if obj is None:
            return np.nan

        if isinstance(obj, dict):
            return {k: self._convert_none_to_nan_recursive(v) for k, v in obj.items()}

        if isinstance(obj, (list, tuple)):
            return [self._convert_none_to_nan_recursive(v) for v in obj]

        return obj

    # INTERNAL: flatten dict keys for view()
    def __flat_dict_key(
        self,
        data: dict,
        parent_key: str = "",
        separator: str = ":",
        obj_dict: dict | None = None,
    ) -> dict:
        """
        Flatten a nested dict into { "a:b:c": value } style keys.
        """
        if obj_dict is None:
            obj_dict = {}

        for k, v in data.items():
            full_key = parent_key + separator + k if parent_key else k
            if isinstance(v, dict):
                if v:  # non-empty dict
                    self.__flat_dict_key(v, full_key, separator, obj_dict)
                else:
                    obj_dict[full_key] = v
            else:
                obj_dict[full_key] = v

        return obj_dict

    # --------------------------------------------------------------
    # PUBLIC: generic view for LUT or any dict (e.g. application)
    # PUBLIC: generic view for LUT or any dict (e.g. application)
    def view(
            self,
            section: dict | str | None = None,
            table_variable: str = "key",
            table_values: str = "value",
            table_format: str = "psql",
            table_print: bool = True,
            separator: str = ":",
            table_name: str = "table",
    ) -> str:
        """
        View a configuration dictionary as a table.
        """

        # --- decide what to display ---
        if isinstance(section, dict):
            data = section

        elif section is None or section == "lut":
            if hasattr(self, "lut") and isinstance(self.lut, dict):
                data = self.lut
            else:
                vars_dict = getattr(self, "variables", {})
                if not isinstance(vars_dict, dict):
                    raise ValueError("LUT not available (variables is not a dict).")
                data = vars_dict.get("lut", {})

        elif isinstance(section, str):
            data = self.get_section(section, raise_if_missing=True)

        else:
            raise TypeError(
                "section must be None, a section name (str), or a dict, "
                f"not {type(section)}"
            )

        if not isinstance(data, dict):
            raise ValueError("view() expects a dict-like object to display.")

        # --- flatten dict ---
        flat = self.__flat_dict_key(data, separator=separator)

        # --- build DataFrame ---
        df = pd.DataFrame.from_dict(flat, orient="index", columns=[table_values])
        df.index.name = table_variable

        # --- create base table ---
        base = tabulate(
            df,
            headers=[table_variable, table_values],
            tablefmt=table_format,
            showindex=True,
            missingval="N/A",
        )

        lines = base.split("\n")

        # --- find first border line (e.g. "+-----+-----+") ---
        border_idx = None
        for i, line in enumerate(lines):
            if line.startswith("+") and line.endswith("+"):
                border_idx = i
                border_line = line
                break

        if border_idx is None:
            # fallback: no border detected, just prepend title line
            title_line = f"view :: {table_name}"
            final = f"{title_line}\n{base}"
            if table_print:
                print(final)
            return final

        # --- build full-width title row (no truncation inside first column) ---
        table_width = len(border_line)            # total width including '+' at ends
        inner_width = table_width - 2             # between the two border chars

        title_text = f" view :: {table_name}"
        # pad or truncate to inner_width
        title_content = title_text.ljust(inner_width)[:inner_width]
        title_row = "|" + title_content + "|"

        # --- insert title row and a border right after the top border line ---
        insert_pos = border_idx + 1
        lines.insert(insert_pos, title_row)
        lines.insert(insert_pos + 1, border_line)

        final_table = "\n".join(lines)
        final_table = "\n" + final_table + "\n"

        if table_print:
            print(final_table)

        return final_table

    # --------------------------------------------------------------
    def autofill_lut(self, extra_tags=None, max_iter=3, strict=False):
        """
        Autofill nested placeholders in the LUT using autofill_mapping.
        """
        # determine LUT container
        if hasattr(self, "lut") and isinstance(self.lut, dict):
            lut = self.lut
            lut_is_attr = True
        else:
            lut = self.variables["lut"]
            lut_is_attr = False

        # remove surrounding quotes first
        sanitize_lut_quotes(lut)

        # autofill nested placeholders
        autofill_mapping(lut, extra_tags=extra_tags, max_iter=max_iter, strict=strict)

        # write back
        if lut_is_attr:
            self.lut = lut
        else:
            self.variables["lut"] = lut

        return lut

    # --------------------------------------------------------------
    def fill_obj_from_lut(
            self,
            section,
            extra_tags: dict | None = None,
            strict: bool = False,
            in_place: bool = True,
            when=None,
            resolve_time_placeholders: bool = False,
            time_keys: tuple[str, ...] | list[str] | None = None,
            template_keys: tuple[str, ...] | list[str] | None = None,
    ):
        """
        Fill an object from LUT placeholders with advanced time handling.
        """

        # base LUT
        base_lut = self.lut if hasattr(self, "lut") else self.variables["lut"]

        # all time-like keys detected from config
        detected_time_keys = self._collect_time_keys()

        # which time keys are explicitly requested?
        if time_keys is None:
            explicit_time_keys: set[str] = set()
        else:
            explicit_time_keys = set(time_keys)

        # intersect with detected time keys (safety)
        explicit_time_keys &= detected_time_keys

        # start from a shallow copy so we don't mutate base_lut
        effective_lut = dict(base_lut)

        # ---- handle time placeholders (optional) ----
        if not resolve_time_placeholders:
            # remove only those time-like keys that are NOT explicitly requested
            keys_to_drop = detected_time_keys - explicit_time_keys
            for k in keys_to_drop:
                effective_lut.pop(k, None)

        else:
            # resolve_time_placeholders=True
            if when is None:
                raise ValueError(
                    "Parameter 'when' must be provided when "
                    "resolve_time_placeholders=True."
                )

            # decide which time keys to actually resolve
            if explicit_time_keys:
                keys_to_resolve = explicit_time_keys
            else:
                keys_to_resolve = detected_time_keys

            sub_lut = {k: base_lut[k] for k in keys_to_resolve if k in base_lut}

            if sub_lut:
                resolved_sub = self.resolve_time_templates(
                    when=when,
                    lut=sub_lut,
                    update_variables=False,
                )
                effective_lut.update(resolved_sub)

        # ---- handle template_keys: copy template value into LUT ----
        if template_keys is not None:
            if hasattr(self, "template") and isinstance(self.template, dict):
                template_dict = self.template
            else:
                template_dict = (
                    self.variables.get("template", {})
                    if hasattr(self, "variables") and isinstance(self.variables, dict)
                    else {}
                )

            for k in template_keys:
                if k not in template_dict:
                    if strict:
                        raise KeyError(
                            f"Template for key '{k}' not found in template dictionary."
                        )
                    continue

                tmpl_val = template_dict[k]
                effective_lut[k] = tmpl_val

        # update extra_tags with effective LUT values (to adapt the request format)
        if extra_tags is not None and extra_tags:
            for elk, elv in effective_lut.items():
                if elk in list(extra_tags.keys()):
                    extra_tags[elk] = elv

        filled = fill_with_mapping(
            section,
            effective_lut,
            extra_tags=extra_tags,
            strict=strict,
            in_place=in_place,
        )
        return filled

    # --------------------------------------------------------------
    def fill_string_with_times(self, string_raw: str, **time_values) -> str:
        """
        Fill a string by replacing {key} only for keys that:
          - are provided in time_values
          - AND have a time-like template defined in self.template
            (or variables['template'])

        Keys without a template, or not provided, are left as {key}.
        """
        if not isinstance(string_raw, str) or not time_values:
            return string_raw

        # get template dict
        if hasattr(self, "template") and isinstance(self.template, dict):
            template = self.template
        else:
            template = self.variables.get("template", {}) if hasattr(self, "variables") else {}

        formatted_values: dict[str, str] = {}

        for key, val in time_values.items():
            tmpl = template.get(key)
            if tmpl is None or not isinstance(tmpl, str):
                continue

            if not self._is_time_template(tmpl):
                continue

            if hasattr(val, "to_pydatetime") and callable(getattr(val, "to_pydatetime")):
                dt = val.to_pydatetime()
            else:
                dt = val

            if not hasattr(dt, "strftime"):
                continue

            inner = tmpl[1:-1] if tmpl.startswith("{") and tmpl.endswith("}") else tmpl

            try:
                formatted_values[key] = dt.strftime(inner)
            except Exception:
                continue

        if not formatted_values:
            return string_raw

        pattern = re.compile(r"\{([^{}]+)\}")

        def _repl(match: re.Match) -> str:
            k = match.group(1)
            if k in formatted_values:
                return str(formatted_values[k])
            return match.group(0)

        return pattern.sub(_repl, string_raw)

    # --------------------------------------------------------------
    def fill_section_with_times(
            self,
            section,
            time_values: dict,
            root_key: str | None = None,
            deep_copy: bool = True,
    ):
        """
        Recursively fill all strings in a section (or dict) using
        fill_string_with_times, WITHOUT mutating internal config.
        """

        if isinstance(section, str):
            obj = self.get_section(section, root_key=root_key, raise_if_missing=True)
        else:
            obj = section

        if deep_copy:
            obj = copy.deepcopy(obj)

        def _walk(value):
            if isinstance(value, dict):
                return {k: _walk(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_walk(v) for v in value]
            elif isinstance(value, tuple):
                return tuple(_walk(v) for v in value)
            elif isinstance(value, str):
                s = self.fill_string_with_times(value, **time_values)
                s = _normalize_path_like_string(s)
                return s
            else:
                return value

        return _walk(obj)

    # --------------------------------------------------------------
    # Environment expansion helpers
    def _expand_env_in_string(
        self,
        s: str,
        env_map: dict[str, str],
    ) -> str:
        """
        Expand environment-like variables inside a string using env_map.

        Rules:
        - Supports: $VAR and ${VAR}
        - Only expands variables with names matching [A-Z_][A-Z0-9_]*.
          This preserves lowercase HMC-style tokens like $yyyy, $mm, $dd.
        - Also applies os.path.expanduser() so that '~' is expanded.
        """

        if not isinstance(s, str) or not s:
            return s

        # Expand '~', '~user', etc.
        s = os.path.expanduser(s)

        pattern = re.compile(
            r"\$(?:\{(?P<braced>[A-Z_][A-Z0-9_]*)\}|(?P<plain>[A-Z_][A-Z0-9_]*))"
        )

        def _repl(match: re.Match) -> str:
            name = match.group("braced") or match.group("plain")
            if name in env_map:
                return env_map[name]
            return match.group(0)

        return pattern.sub(_repl, s)

    def expand_env(
        self,
        obj,
        extra_env: dict[str, str] | None = None,
        deep_copy: bool = True,
    ):
        """
        Recursively expand environment variables in all strings
        contained in 'obj'.

        This function respects HMC-style tokens like $yyyy/$mm/$dd by
        only expanding uppercase names.

        Parameters
        ----------
        obj : Any
            Arbitrary nested structure (dict/list/tuple/str/...).
        extra_env : dict or None
            Optional mapping that overrides or extends os.environ.
        deep_copy : bool
            If True, work on a deep copy and return it.
            If False, modify the structure in-place.

        Returns
        -------
        Any
            The object with environment variables expanded in all strings.
        """
        if deep_copy:
            obj = copy.deepcopy(obj)

        env_map = dict(os.environ)
        if extra_env:
            env_map.update(extra_env)

        def _walk(value):
            if isinstance(value, dict):
                return {k: _walk(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_walk(v) for v in value]
            elif isinstance(value, tuple):
                return tuple(_walk(v) for v in value)
            elif isinstance(value, str):
                return self._expand_env_in_string(value, env_map)
            else:
                return value

        return _walk(obj)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Application wrapper
class ApplicationConfig:
    """
    Lightweight wrapper around a specific application section
    (e.g., 'application_execution', 'application_namelist').

    This class delegates all LUT and time-template resolution to a
    ConfigManager instance, while providing a clean API:

        - .raw         → raw section from config (no substitutions)
        - .with_times  → apply {time_*} template filling only
        - .with_lut    → apply LUT-based placeholder filling only
        - .resolved    → apply time filling + LUT filling + env + optional validation
    """

    def __init__(self, cfg: ConfigManager, section_name: str, root_key: str | None = None):
        """
        Parameters
        ----------
        cfg : ConfigManager
            The initialized ConfigManager (LUT parsed, env applied, etc.).
        section_name : str
            Name of the target application section in the JSON.
            Example: "application_execution".
        root_key : str | None
            If None: the application section lives at the top-level.
            Otherwise: the section is taken from raw_config[root_key][section_name].
        """
        self._cfg = cfg
        self._section_name = section_name
        self._root_key = root_key

    # --------------------------------------------------------------
    @property
    def raw(self) -> dict:
        """
        Return the application section exactly as it appears in the config
        (no time template substitutions, no LUT filling).
        """
        return self._cfg.get_section(
            self._section_name,
            root_key=self._root_key,
            raise_if_missing=True,
        )

    @property
    def structure(self) -> dict:
        """
        Return the raw application section as a dictionary.
        Alias for .raw for user convenience.
        """
        return self.raw

    # --------------------------------------------------------------
    def with_times(self, time_values: dict) -> dict:
        """
        Apply only time-template substitution to the raw application section.

        Example time_values:
        {
            "time_run": datetime(...),
            "time_restart": datetime(...)
        }

        Returns
        -------
        dict (deep copy)
        """
        return self._cfg.fill_section_with_times(
            section=self.raw,
            time_values=time_values,
            root_key=self._root_key,
            deep_copy=True,
        )

    # --------------------------------------------------------------
    def with_lut(
        self,
        obj: dict | None = None,
        when=None,
        strict: bool = False,
        resolve_time_placeholders: bool = True,
        time_keys: list[str] | tuple[str, ...] | None = None,
        template_keys: list[str] | tuple[str, ...] | None = None,
        extra_tags: dict | None = None,
    ) -> dict:
        """
        Apply only LUT-driven placeholder filling (no time substitution).
        """
        section = self.raw if obj is None else obj

        return self._cfg.fill_obj_from_lut(
            section=section,
            extra_tags=extra_tags,
            strict=strict,
            in_place=False,
            when=when,
            resolve_time_placeholders=resolve_time_placeholders,
            time_keys=time_keys,
            template_keys=template_keys,
        )

    # --------------------------------------------------------------
    def validate(
        self,
        obj: dict | None = None,
        *,
        strict: bool = True,
        allow_placeholders: bool = False,
        allow_none: bool = False,
    ) -> dict:
        """
        Validate an application section for unresolved placeholders and None values.

        Parameters
        ----------
        obj : dict or None
            - None: validate the raw application section.
            - dict: validate this specific object instead.
        strict : bool
            If True, raise ValueError when issues are found and not allowed.
            If False, only return a summary.
        allow_placeholders : bool
            If False, any '{...}' placeholder is reported as an issue.
        allow_none : bool
            If False, any None value is reported as an issue.

        Returns
        -------
        dict
            {
              "unresolved_placeholders": [ "executable.location", ... ],
              "none_values": [ "info.location", ... ]
            }
        """
        if obj is None:
            obj = self.raw

        unresolved_placeholders: list[str] = []
        none_values: list[str] = []

        placeholder_pattern = re.compile(r"\{[^{}]+\}")

        def _walk(value, path: str):
            if isinstance(value, dict):
                for k, v in value.items():
                    new_path = f"{path}.{k}" if path else k
                    _walk(v, new_path)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    new_path = f"{path}[{i}]"
                    _walk(v, new_path)
            elif isinstance(value, tuple):
                for i, v in enumerate(value):
                    new_path = f"{path}[{i}]"
                    _walk(v, new_path)
            else:
                if value is None:
                    none_values.append(path)

                if isinstance(value, str) and placeholder_pattern.search(value):
                    unresolved_placeholders.append(path)

        _walk(obj, path="")

        summary = {
            "unresolved_placeholders": unresolved_placeholders,
            "none_values": none_values,
        }

        if strict:
            errors = []
            if not allow_placeholders and unresolved_placeholders:
                errors.append(
                    f"Unresolved placeholders in {self._section_name}: "
                    + ", ".join(unresolved_placeholders)
                )
            if not allow_none and none_values:
                errors.append(
                    f"None values in {self._section_name}: "
                    + ", ".join(none_values)
                )

            if errors:
                raise ValueError("ApplicationConfig validation failed:\n" + "\n".join(errors))

        return summary

    # --------------------------------------------------------------
    def resolved(
        self,
        time_values: dict | None = None,
        when=None,
        strict: bool = False,
        resolve_time_placeholders: bool = True,
        time_keys: list[str] | tuple[str, ...] | None = None,
        template_keys: list[str] | tuple[str, ...] | None = None,
        extra_tags: dict | None = None,
        expand_env: bool = True,
        env_extra: dict[str, str] | None = None,
        validate_result: bool = False,
        validate_allow_placeholders: bool = False,
        validate_allow_none: bool = False,
    ) -> (None, dict):
        """
        Full pipeline:

            1. Apply time-template substitution (if time_values provided)
            2. Apply LUT placeholder filling
            3. Optionally expand environment variables ($HOME, $RUN, ...)
            4. Optionally validate final result
        """
        obj = self.raw

        # if the section is missing, return None
        if obj is None:
            return None

        # Step 1: time placeholders
        if time_values:
            obj = self._cfg.fill_section_with_times(
                section=obj,
                time_values=time_values,
                root_key=self._root_key,
                deep_copy=True,
            )

        # Step 2: LUT placeholders
        obj = self._cfg.fill_obj_from_lut(
            section=obj,
            extra_tags=extra_tags,
            strict=strict,
            in_place=False,
            when=when,
            resolve_time_placeholders=resolve_time_placeholders,
            time_keys=time_keys,
            template_keys=template_keys,
        )

        # Step 3: environment expansion
        if expand_env:
            obj = self._cfg.expand_env(
                obj,
                extra_env=env_extra,
                deep_copy=False,
            )

        # Step 4: validation (optional)
        if validate_result:
            self.validate(
                obj=obj,
                strict=True,
                allow_placeholders=validate_allow_placeholders,
                allow_none=validate_allow_none,
            )

        return obj

    # --------------------------------------------------------------
    def view(self, table_name: str | None = None, **view_kwargs) -> str:
        """
        Display the raw application section using ConfigManager.view(),
        rendered as a table.
        """
        name = table_name or self._section_name
        return self._cfg.view(
            section=self.raw,
            table_name=name,
            **view_kwargs,
        )
# ----------------------------------------------------------------------------------------------------------------------
