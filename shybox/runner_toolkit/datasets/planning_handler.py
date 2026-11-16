"""Public dataset planner: existing planning plus optional source-mode helpers."""
from .planning_base_handler import (
    DatasetPlanner as DatasetPlannerBase,
    DEFAULT_TIME_TAGS, DEFAULT_ENSEMBLE_TAGS, VARIABLES_MANDATORY,
    TIME_FIELDS_MANDATORY, SOURCE_FIELDS_MANDATORY, DESTINATION_FIELDS_MANDATORY,
    CREATE_TIME_PERIOD_DEFAULT,
)
from .planning_mode_handler import PlanningModeHandler


class DatasetPlanner(PlanningModeHandler, DatasetPlannerBase):
    """Keep the existing interface and add deterministic/probabilistic views."""
