from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class Mode(str, Enum):
    DEFAULT = "default"
    MANDATORY = "mandatory"

@dataclass(frozen=True)
class Var:
    """
    Compact metadata for a single namelist variable.
    - mode:   Mode.DEFAULT / Mode.MANDATORY
    - value:  default value (None if mandatory)
    - summary: optional short description
    """
    mode: Mode
    value: Any = None
    summary: Optional[str] = None

# Convenience constructors so declarations stay very compact
def D(value: Any, summary: str | None = None) -> Var:
    """Defaulted variable."""
    return Var(mode=Mode.DEFAULT, value=value, summary=summary)

def M(summary: str | None = None) -> Var:
    """Mandatory variable (value=None by design)."""
    return Var(mode=Mode.MANDATORY, value=None, summary=summary)