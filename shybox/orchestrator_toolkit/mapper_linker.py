"""
Class Features

Name:          mapper_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260904'
Version:       '1.2.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
@dataclass
class Data:

    # workflow information
    workflow_id: int
    workflow_name: str

    # process information
    process_id: int
    process_name: str

    # process method
    fx: str

    # linked data variables
    variables_in: List[str] = field(default_factory=list)
    variables_out: List[str] = field(default_factory=list)

    # linked fx mappings
    fx_in: Dict[str, Any] = field(default_factory=dict)
    fx_out: Dict[str, Any] = field(default_factory=dict)

    # separators
    root_separator: str = "|"
    variable_separator: str = ":"

    # method to create workflow reference
    @property
    def workflow_reference(self) -> str:
        return f"{self.workflow_name}:{self.workflow_id}"
    # method to create process reference
    @property
    def process_reference(self) -> str:
        return f"{self.process_name}:{self.fx}"
    # method to create reference
    @property
    def reference(self) -> str:
        return f"{self.workflow_name}:{self.fx}"

    # method to create links
    def create_links(self, side='in') -> list:

        # get variable name
        workflow_name = self.workflow_name

        # organize input/output links
        # organize input/output variable links
        if side == 'in':
            links = [
                f"{workflow_name}{self.variable_separator}{var_name}"
                for var_name in self.variables_in
            ]
        elif side == 'out':
            links = [
                f"{workflow_name}{self.variable_separator}{var_name}"
                for var_name in self.variables_out
            ]
        else:
            links = []

        return links

    # method to create tag
    def create_tag(self) -> str:

        # organize input/output variables
        in_tag = self.variable_separator.join(self.variables_in)
        out_tag = self.variable_separator.join(self.variables_out)

        # create process tag
        return self.root_separator.join([
            self.workflow_name,
            in_tag,
            out_tag,
        ])

    # method to add input variable
    def add_in(self, variable: str) -> None:
        variable = str(variable)
        if variable not in self.variables_in:
            self.variables_in.append(variable)


    # method to add output variable
    def add_out(self, variable: str) -> None:
        variable = str(variable)
        if variable not in self.variables_out:
            self.variables_out.append(variable)


    # method to add input fx mapping
    def add_fx_in(self, key: str, value: Any) -> None:
        self.fx_in[str(key)] = value

    # method to add output fx mapping
    def add_fx_out(self, key: str, value: Any) -> None:
        self.fx_out[str(key)] = value

    # method to update input fx mapping
    def update_fx_in(self,mapping: Optional[Dict[str, Any]]) -> None:
        if mapping:
            self.fx_in.update(mapping)

    # method to update output fx mapping
    def update_fx_out(self, mapping: Optional[Dict[str, Any]]) -> None:
        if mapping:
            self.fx_out.update(mapping)

    # method to check input variable
    def has_in(self, variable: str) -> bool:
        return str(variable) in self.variables_in

    # method to check output variable
    def has_out(self, variable: str) -> bool:
        return str(variable) in self.variables_out


    # method to check workflow
    def is_workflow(self, workflow: Any) -> bool:
        return workflow == self.workflow_id or str(workflow) == self.workflow_name

    # method to check process
    def is_process(self, process: Any) -> bool:
        return process == self.process_id or str(process) == self.process_name

    # method to check fx
    def is_fx(self, fx: str) -> bool:
        return str(fx) == self.fx

    # method to get data variables
    def get_data(self, side: Optional[str] = None):

        # return input/output
        if side is None:
            return {"in": list(self.variables_in), "out": list(self.variables_out),}

        # normalize side
        side = side.lower()

        # return input
        if side == "in":
            return list(self.variables_in)
        # return output
        if side == "out":
            return list(self.variables_out)

        raise ValueError(f"Data side '{side}' is not supported. Expected 'in', 'out' or None.")


    # method to get fx mappings
    def get_fx(self, side: Optional[str] = None):

        # return input/output
        if side is None:
            return {"in": dict(self.fx_in), "out": dict(self.fx_out),}

        # normalize side
        side = side.lower()

        # return input
        if side == "in":
            return dict(self.fx_in)

        # return output
        if side == "out":
            return dict(self.fx_out)

        raise ValueError(f"FX side '{side}' is not supported. Expected 'in', 'out' or None.")

    # method to get side information
    def get_side(self, side: str) -> Dict[str, Any]:

        # normalize side
        side = side.lower()

        # return input information
        if side == "in":
            return {"data": list(self.variables_in), "fx": dict(self.fx_in),}

        # return output information
        if side == "out":
            return {"data": list(self.variables_out), "fx": dict(self.fx_out),}

        raise ValueError(f"Side '{side}' is not supported. Expected 'in' or 'out'.")

    # method to export object as dictionary
    def to_dict(self) -> Dict[str, Any]:

        data = {
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "process_id": self.process_id,
            "process_name": self.process_name,
            "fx": self.fx,
            "in": {
                "data": list(self.variables_in),
                "fx": dict(self.fx_in),
            },
            "out": {
                "data": list(self.variables_out),
                "fx": dict(self.fx_out),
            },
            "workflow_reference": self.workflow_reference,
            "process_reference": self.process_reference,
            "reference": self.reference,
            "tag": self.create_tag(),
            "links_in": self.create_links(side='in'),
            "links_out": self.create_links(side='out'),
        }

        return data

    # method to create object from dictionary
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):

        # get input/output sections
        section_in = data.get("in", {}) or {}
        section_out = data.get("out", {}) or {}

        # create object
        return cls(
            workflow_id=int(data.get("workflow_id", 0)),
            workflow_name=str(data.get("workflow_name", "")),
            process_id=int(data.get("process_id", 0)),
            process_name=str(data.get("process_name", "")),
            fx=str(data.get("fx", "")),
            variables_in=list(section_in.get("data", []) or []),
            variables_out=list(section_out.get("data", []) or []),
            fx_in=dict(section_in.get("fx", {}) or {}),
            fx_out=dict(section_out.get("fx", {}) or {}),
        )
# ----------------------------------------------------------------------------------------------------------------------
