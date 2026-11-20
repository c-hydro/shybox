"""
Library Features:

Name:          lib_utils_ensemble
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250102'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to generate ensemble list
def generate_ensemble_list(
        ensemble_start: int, ensemble_end: int, ensemble_step: int = 1, ensemble_format: str = "%03d",
        **kwargs) -> list[str]:

    """Return member IDs in an inclusive range using %03d, 03d or {:03d} style."""
    bounds_and_step = (ensemble_start, ensemble_end, ensemble_step)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in bounds_and_step):
        raise ValueError("Ensemble start, end and step must be integers.")
    if ensemble_start < 0 or ensemble_end < ensemble_start:
        raise ValueError("Ensemble bounds must be nonnegative and in ascending order.")
    if ensemble_step < 1:
        raise ValueError("Ensemble step must be positive.")
    if not isinstance(ensemble_format, str) or not ensemble_format:
        raise ValueError("Ensemble format must be a nonempty string.")

    numbers = range(ensemble_start, ensemble_end + 1, ensemble_step)
    try:
        if ensemble_format.startswith("%"):
            members = [ensemble_format % number for number in numbers]
        else:
            format_spec = ensemble_format
            if format_spec.startswith("{:") and format_spec.endswith("}"):
                format_spec = format_spec[2:-1]
            members = [format(number, format_spec) for number in numbers]
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid ensemble format {ensemble_format!r}.") from error

    if len(set(members)) != len(members) or any(not member.isdigit() for member in members):
        raise ValueError("Ensemble format must produce unique numeric IDs.")

    return members
# ----------------------------------------------------------------------------------------------------------------------
