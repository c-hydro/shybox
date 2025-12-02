from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import os

import pandas as pd
from tabulate import tabulate

from shybox.runner_toolkit.namelist.lib_utils_dataclass import Mode, Var
from shybox.runner_toolkit.namelist.lib_utils_namelist import parse_fortran_namelist
from shybox.runner_toolkit.namelist.namelist_template_handler import NamelistTemplateManager


# ======================================================================================
# Result object: contains final dict + Fortran text + helpers
# ======================================================================================

@dataclass
class NamelistCreator:
    """
    Container for a fully built namelist.

    Attributes
    ----------
    model : str
        Model name, e.g. "hmc" or "s3m".
    version : str
        Version string, e.g. "3.3.0".
    values : Dict[str, Dict[str, Any]]
        Final merged values per section (after defaults + overrides).
    text : str
        Fortran NAMELIST text.
    """

    model: str
    version: str
    values: Dict[str, Dict[str, Any]]
    text: str

    # ------------------------------------------------------------------ #
    # Core helpers
    def as_dict(self) -> Dict[str, Dict[str, Any]]:
        """Return the full nested dictionary of namelist values."""
        return self.values

    # ------------------------------------------------------------------ #
    # I/O
    def write_to_ascii(
        self,
        filename: Optional[str] = None,
        *,
        encoding: str = "utf-8",
        overwrite: bool = False,
        makedirs: bool = True,
    ) -> None:
        """
        Write the Fortran namelist text to an ASCII file.

        Parameters
        ----------
        filename : str or None
            If provided, write to this file.
            If None, print a warning and do nothing.
        encoding : str
            File encoding (default: 'utf-8').
        overwrite : bool
            If False and file exists -> warn and do nothing.
            If True  and file exists -> warn and overwrite.
        makedirs : bool
            If True, create parent folders if they do not exist.
        """
        if filename is None:
            print(
                "[WARN] NamelistCreator.write_to_ascii(): no filename provided; "
                "nothing was written."
            )
            return

        folder = os.path.dirname(filename)
        if folder and makedirs and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        if os.path.exists(filename):
            if not overwrite:
                print(
                    f"[WARN] NamelistCreator.write_to_ascii(): file '{filename}' "
                    "already exists and overwrite=False; nothing was written."
                )
                return
            else:
                print(
                    f"[WARN] NamelistCreator.write_to_ascii(): file '{filename}' "
                    "already exists and will be overwritten."
                )

        with open(filename, "w", encoding=encoding) as f:
            f.write(self.text)

    # ------------------------------------------------------------------ #
    # Internal: flatten nested dict into "section:var" -> value
    def __flat_dict_key(
        self,
        d: Dict[str, Any],
        parent_key: str = "",
        separator: str = ":",
    ) -> Dict[str, Any]:
        """
        Flatten a nested dict into a single-level dict with compound keys.

        Example
        -------
        {"HMC_Parameters": {"dUc": 20}}  ->
        {"HMC_Parameters:dUc": 20}  (separator=":")
        """
        items: Dict[str, Any] = {}
        for k, v in d.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else str(k)
            if isinstance(v, dict):
                items.update(self.__flat_dict_key(v, new_key, separator=separator))
            else:
                items[new_key] = v
        return items

    # ------------------------------------------------------------------ #
    # View method (table)
    def view(
        self,
        section: Dict[str, Any] | str | None = None,
        table_variable: str = "key",
        table_values: str = "value",
        table_format: str = "psql",
        table_print: bool = True,
        separator: str = ":",
        table_name: str | None = None,
    ) -> str:
        """
        View namelist content as a table.

        Parameters
        ----------
        section
            - None  -> use self.as_dict() (all sections)
            - dict  -> display that dict
            - str   -> treat as a section name (e.g. "HMC_Namelist")
        table_variable
            Column name for variable names.
        table_values
            Column name for values.
        table_format
            Tabulate format (e.g. 'psql', 'plain', 'github', ...).
        table_print
            If True, print the table.
        separator
            Separator for flattened keys (default ':').
        table_name
            Title to show above the table. If None, use "model-version".
        """

        # --- decide what to display ---
        if isinstance(section, dict):
            data = section
        elif section is None:
            data = self.as_dict()
        elif isinstance(section, str):
            # treat as a section name if present
            if section not in self.values:
                raise ValueError(
                    f"NamelistCreator.view(): section '{section}' not found. "
                    f"Available sections: {list(self.values.keys())}"
                )
            data = self.values[section]
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
        border_line = None
        for i, line in enumerate(lines):
            if line.startswith("+") and line.endswith("+"):
                border_idx = i
                border_line = line
                break

        if table_name is None:
            table_name = f"namelist :: {self.model}-{self.version}"

        if border_idx is None or border_line is None:
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

    # ------------------------------------------------------------------ #
    # Representation
    def __repr__(self) -> str:
        n_sections = len(self.values)
        n_vars = sum(len(sec) for sec in self.values.values())
        return (
            f"Namelist(model={self.model!r}, version={self.version!r}, "
            f"sections={n_sections}, total_vars={n_vars})"
        )


# ======================================================================================
# NamelistStructureManager: builds/validates namelists from templates + user values
# ======================================================================================

class NamelistStructureManager:
    """
    Use a NamelistTemplateManager + user values to:
      - build a complete namelist with defaults + overrides
      - validate mandatory params
      - export to Fortran NAMELIST format (string or NamelistCreator)
    """

    def __init__(self, template_manager: NamelistTemplateManager):
        self.templates = template_manager

    # ---------- CLASSMETHOD entry points ----------

    @classmethod
    def from_dict(
        cls,
        template_manager: NamelistTemplateManager,
        model: str,
        version: str,
        values: Dict[str, Any],
        *,
        check: bool = True,
        as_object: bool = False,
    ) -> str | NamelistCreator:
        """
        Convenience: build a namelist starting from a Python dict.

        values can be:
          - already sectioned: { "HMC_Namelist": {...}, "HMC_Parameters": {...}, ... }
          - flat: { "by_value": {...}, "by_pattern": {...} }
          - flat: { "dUc": 30, "sDomainName": "marche", ... }

        Returns
        -------
        str
            Fortran namelist text if as_object=False.
        NamelistCreator
            Rich object (dict + text + view + write_to_ascii) if as_object=True.
        """
        manager = cls(template_manager)
        return manager.to_fortran(
            model=model,
            version=version,
            user_values=values,
            check=check,
            as_object=as_object,
        )

    @classmethod
    def from_ascii(
        cls,
        template_manager: NamelistTemplateManager,
        model: str,
        version: str,
        ascii_text: str,
        *,
        check: bool = True,
        as_object: bool = False,
    ) -> str | NamelistCreator:
        """
        Parse a Fortran ASCII namelist (string), merge with Var-templates,
        validate and return either:
          - Fortran namelist string (as_object=False)
          - NamelistCreator (as_object=True)
        """
        parsed_values = parse_fortran_namelist(ascii_text)
        manager = cls(template_manager)
        # here parsed_values are treated as user_values (overrides)
        return manager.to_fortran(
            model=model,
            version=version,
            user_values=parsed_values,
            check=check,
            as_object=as_object,
        )

    @classmethod
    def from_file(
        cls,
        template_manager: NamelistTemplateManager,
        model: str,
        version: str,
        filename: str,
        *,
        encoding: str = "utf-8",
        check: bool = True,
        as_object: bool = False,
    ) -> str | NamelistCreator:
        """
        Read a Fortran namelist ASCII file and process it like from_ascii().

        Returns
        -------
        str
            Fortran namelist text if as_object=False.
        NamelistCreator
            Rich object (dict + text + view + write_to_ascii) if as_object=True.
        """
        with open(filename, "r", encoding=encoding) as f:
            text = f.read()
        return cls.from_ascii(
            template_manager=template_manager,
            model=model,
            version=version,
            ascii_text=text,
            check=check,
            as_object=as_object,
        )

    # ------------------------------------------------------------------ #
    # method to auto-cast raw values
    def _auto_cast_value(self, raw_value: Any, var_def: Var | None = None) -> Any:
        """
        If raw_value is a string that looks numeric, cast it to int or float.
        Prefer the type of var_def.value (if provided and not None).
        Otherwise, infer from the string format.
        """
        # Non-string: return as is
        if not isinstance(raw_value, str):
            return raw_value

        s = raw_value.strip()
        if s == "":
            return raw_value  # keep empty string

        # If we know the default type from the template, try to follow it
        if var_def is not None and var_def.value is not None:
            default = var_def.value
            # If default is int/float, try to cast accordingly
            if isinstance(default, int):
                try:
                    return int(s)
                except Exception:
                    # if it fails, keep original string
                    return raw_value
            if isinstance(default, float):
                try:
                    return float(s)
                except Exception:
                    return raw_value

        # Otherwise, infer from the string content
        # Try int first
        try:
            if s.isdigit() or (s.startswith(("+", "-")) and s[1:].isdigit()):
                return int(s)
        except Exception:
            pass

        # Then try float
        try:
            # This will handle things like 3.14, 1e-3, etc.
            float_val = float(s)
            return float_val
        except Exception:
            # Not numeric → keep as string
            return raw_value

    # ------------------------------------------------------------------ #
    # build sectioned user_values from flat "by_value" and "by_pattern"
    def build_updates(
        self,
        model: str,
        version: str,
        by_value: Dict[str, Any] | None = None,
        by_pattern: Dict[str, Dict[str, Any]] | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Convert flat dictionaries into the nested structure expected by to_fortran().

        It searches across all sections of the template.

        Extra logic:
          - if a value is a numeric-like string, cast to int/float
          - track all (section,param) updated via by_value
          - if by_pattern hits a (section,param) already updated by_value,
            skip and warn
        """
        template = self.templates.get(model, version)
        user_values: Dict[str, Dict[str, Any]] = {}

        # Keep track of what was explicitly updated by by_value
        updated_by_value: set[tuple[str, str]] = set()

        # ---------------------------
        # 1) explicit by_value keys
        # ---------------------------
        if by_value:
            for param_name, new_value in by_value.items():
                found = False
                for section_name, params in template.items():
                    if param_name in params:
                        var_def = params[param_name]
                        casted_value = self._auto_cast_value(new_value, var_def)

                        user_values.setdefault(section_name, {})
                        user_values[section_name][param_name] = casted_value
                        updated_by_value.add((section_name, param_name))
                        found = True

                if not found:
                    # you can decide to raise here instead if you want strict behavior
                    print(
                        f"[WARN] Parameter '{param_name}' not found in template "
                        f"{model} {version}; by_value entry was ignored."
                    )

        # ---------------------------
        # 2) by_pattern (bulk updates)
        # ---------------------------
        if by_pattern:
            for pat_name, pat_info in by_pattern.items():
                if not pat_info.get("active", False):
                    continue

                template_str = pat_info.get("template")
                pat_value = pat_info.get("value")

                if not template_str:
                    continue

                for section_name, params in template.items():
                    for param_name, var_def in params.items():
                        # very simple rule: substring match
                        if template_str in param_name:
                            # if the parameter was already updated by_value, skip
                            if (section_name, param_name) in updated_by_value:
                                print(
                                    f"[WARN] pattern '{pat_name}' skipped for "
                                    f"{section_name}.{param_name} because it was "
                                    f"already set by 'by_value'."
                                )
                                continue

                            casted_value = self._auto_cast_value(pat_value, var_def)
                            user_values.setdefault(section_name, {})
                            user_values[section_name][param_name] = casted_value

        return user_values

    # ------------------------------------------------------------------ #
    def build_values(
        self,
        model: str,
        version: str,
        user_values: Dict[str, Any] | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build the final values dict:

          - start from template defaults (Var.mode == DEFAULT)
          - apply user_values (which can be sectioned OR flat)
        """
        template = self.templates.get(model, version)
        values: Dict[str, Dict[str, Any]] = {}

        # ------------------------------------------------------
        # Normalize user_values:
        #   * if already sectioned (keys == section names) -> use as is
        #   * if flat / "fields" with by_value/by_pattern -> build_updates()
        # ------------------------------------------------------
        section_names = set(template.keys())
        sectioned_user_values: Dict[str, Dict[str, Any]] | None = None

        if user_values:
            # Case 1: already sectioned (has at least one known section key)
            if any(k in section_names for k in user_values.keys()):
                sectioned_user_values = user_values  # type: ignore[assignment]

            # Case 2: flat dict (like your "fields" or pure {param: value} dict)
            else:
                if "by_value" in user_values or "by_pattern" in user_values:
                    by_value = user_values.get("by_value", {})
                    by_pattern = user_values.get("by_pattern", {})
                else:
                    # pure flat dict of param_name -> value
                    by_value = user_values
                    by_pattern = None

                sectioned_user_values = self.build_updates(
                    model=model,
                    version=version,
                    by_value=by_value,
                    by_pattern=by_pattern,
                )

        # ------------------------------------------------------
        # 1. defaults from template
        # ------------------------------------------------------
        for section_name, params in template.items():
            values[section_name] = {}
            for param_name, var in params.items():
                if var.mode == Mode.DEFAULT:
                    values[section_name][param_name] = var.value

        # ------------------------------------------------------
        # 2. apply overrides
        # ------------------------------------------------------
        if sectioned_user_values:
            for section_name, section_vals in sectioned_user_values.items():
                values.setdefault(section_name, {})
                values[section_name].update(section_vals)

        return values

    # ------------------------------------------------------------------ #
    def validate(
        self,
        model: str,
        version: str,
        values: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        template = self.templates.get(model, version)
        errors: List[str] = []

        # mandatory
        for section_name, params in template.items():
            for param_name, var in params.items():
                if var.mode == Mode.MANDATORY:
                    if section_name not in values or param_name not in values[section_name]:
                        errors.append(f"Missing mandatory {section_name}.{param_name}")
                    elif values[section_name][param_name] is None:
                        errors.append(f"Mandatory {section_name}.{param_name} is None")

        # unknowns
        for section_name, section_vals in values.items():
            if section_name not in template:
                errors.append(f"Unknown section {section_name}")
                continue
            allowed_params = template[section_name]
            for param_name in section_vals.keys():
                if param_name not in allowed_params:
                    errors.append(f"Unknown parameter {section_name}.{param_name}")

        return errors

    # ------------------------------------------------------------------ #
    def _format_value_for_fortran(self, value: Any) -> str:
        if isinstance(value, bool):
            return ".true." if value else ".false."
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            formatted_items = [self._format_value_for_fortran(v) for v in value]
            return ", ".join(formatted_items)
        return str(value)

    # ------------------------------------------------------------------ #
    def to_fortran(
        self,
        model: str,
        version: str,
        user_values: Dict[str, Any] | None = None,
        *,
        check: bool = True,
        as_object: bool = False,
    ) -> str | NamelistCreator:
        """
        Build, validate and return either:

          - a Fortran NAMELIST string  (as_object=False, default)
          - a NamelistCreator object  (as_object=True) containing both
            the final dict and the text.
        """
        values = self.build_values(model, version, user_values)

        if check:
            errors = self.validate(model, version, values)
            if errors:
                msg = "Namelist validation failed:\n" + "\n".join(f"- {e}" for e in errors)
                raise ValueError(msg)

        template = self.templates.get(model, version)
        lines: List[str] = []

        for section_name in template.keys():
            if section_name not in values:
                continue

            lines.append(f"&{section_name}")
            section_vals = values[section_name]

            for param_name in template[section_name].keys():
                if param_name not in section_vals:
                    continue
                v = section_vals[param_name]
                v_str = self._format_value_for_fortran(v)
                lines.append(f"  {param_name} = {v_str}")
            lines.append("/\n")

        text = "\n".join(lines)

        if as_object:
            return NamelistCreator(
                model=model,
                version=version,
                values=values,
                text=text,
            )

        return text
