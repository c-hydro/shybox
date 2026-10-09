import importlib.util
import importlib.metadata
import logging
import subprocess
import sys

logger = logging.getLogger(__name__)


def check_external_toolkit(
    package_name: str,
    auto_install: bool = False,
    raise_error: bool = False
) -> bool:
    """Check and optionally install a SHYBOX external toolkit."""

    # Get optional dependencies from installed SHYBOX metadata
    requirements = importlib.metadata.requires("shybox") or []

    # Find the dependency for the requested extra
    extra_requirements = [
        req for req in requirements
        if f'extra == "{package_name}"' in req
        or f"extra == '{package_name}'" in req
    ]

    if not extra_requirements:
        raise ValueError(
            f'External toolkit "{package_name}" is not configured '
            'in SHYBOX optional dependencies.'
        )

    # Check installation
    try:
        installed = importlib.util.find_spec(package_name) is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        installed = False

    if installed:
        return True

    if auto_install:
        logger.warning(
            f'External toolkit "{package_name}" is missing. Installing ...'
        )

        # Install the configured optional dependency
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                f"shybox[{package_name}]",
            ],
            check=True
        )

        importlib.invalidate_caches()

        return importlib.util.find_spec(package_name) is not None

    message = (
        f'External toolkit "{package_name}" is not installed. '
        f'Install using: pip install "shybox[{package_name}]"'
    )

    if raise_error:
        raise ImportError(message)

    logger.warning(message)
    return False