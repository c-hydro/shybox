# ----------------------------------------------------------------------------------------------------------------------
# libraries
from shybox.runner_toolkit.namelist.namelist_template_hmc import *
from shybox.runner_toolkit.namelist.namelist_template_s3m import *
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Central registry for all namelist templates (HMC)
NAMELIST_TEMPLATES_HMC: Dict[tuple[str, str], Dict[str, Dict[str, Var]]] = {
    ("hmc", "3.1.6"): namelist_hmc_316,
    ("hmc", "3.2.0"): namelist_hmc_320,
    ("hmc", "3.3.0"): namelist_hmc_330
}

# Central registry for all namelist templates (S3M)
NAMELIST_TEMPLATES_S3M: Dict[tuple[str, str], Dict[str, Dict[str, Var]]] = {
    ("s3m", "5.3.3"): namelist_s3m_533
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle namelist templates
class NamelistTemplateManager:

    def __init__(self, templates: Dict[tuple[str, str], Dict[str, Dict[str, Var]]] = None):
        if templates is not None:
            self.registry = templates
        else:
            self.registry = {**NAMELIST_TEMPLATES_HMC, **NAMELIST_TEMPLATES_S3M}

    def get(self, model, version):
        key = (model.lower(), version)
        if key not in self.registry:
            raise KeyError(f"No template for {key}")
        return self.registry[key]

    def exists(self, model, version) -> bool:
        return (model.lower(), version) in self.registry

    # method to convert template to plain nested dict structure
    def as_dict(self, model: str, version: str) -> dict:
        """
        Convert the compact Var-based template into a nested dictionary:
        {
            section: {
                param: {
                    "mode": "mandatory" or "default",
                    "value": <default_value or None>,
                    "summary": <optional summary>
                }
            }
        }
        """
        template = self.get(model, version)

        result = {}

        for section_name, params in template.items():
            section_dict = {}
            for param_name, var in params.items():
                section_dict[param_name] = {
                    "mode": var.mode.value,
                    "value": var.value,
                    "summary": var.summary,
                }
            result[section_name] = section_dict

        return result
# ----------------------------------------------------------------------------------------------------------------------
