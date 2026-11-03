import importlib
import importlib.util
import logging
import subprocess
import sys
import tomllib

from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOOLKIT_ROOT = Path(__file__).resolve().parent


def get_external_config(package_name: str) -> dict:
    """Read external toolkit configuration from pyproject.toml."""

    pyproject_file = PROJECT_ROOT / "pyproject.toml"

    with pyproject_file.open("rb") as file:
        config = tomllib.load(file)

    toolkits = (
        config.get("tool", {})
        .get("shybox", {})
        .get("external_toolkits", {})
    )

    if package_name not in toolkits:
        raise ValueError(
            f'External toolkit "{package_name}" is not configured. '
            f'Available: {", ".join(toolkits)}'
        )

    return toolkits[package_name]


def is_module_available(package_name: str) -> bool:
    """Check whether a Python module is importable."""

    try:
        return importlib.util.find_spec(package_name) is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def register_toolkit_path(
    package_name: str,
    folder: Path
) -> bool:
    """Register possible Python source locations."""

    candidates = [
        folder,
        folder / "src",
        folder / "python",
    ]

    for candidate in candidates:
        if not candidate.is_dir():
            continue

        path = str(candidate)

        if path not in sys.path:
            sys.path.insert(0, path)

    importlib.invalidate_caches()

    return is_module_available(package_name)


def check_external_toolkit(
    package_name: str,
    auto_install: bool = False,
    raise_error: bool = False
) -> bool:
    """Check, download and optionally install an external toolkit."""

    # Check already installed/importable package
    if is_module_available(package_name):
        return True

    # Read configuration
    config = get_external_config(package_name)

    repository = config["repository"]
    branch = config.get("branch")
    install_mode = config.get("install_mode", "auto")

    # Destination directory
    toolkit_folder = TOOLKIT_ROOT / package_name

    # Check already downloaded repository
    if toolkit_folder.is_dir():
        if register_toolkit_path(package_name, toolkit_folder):
            return True

    if not auto_install:
        message = (
            f'External toolkit "{package_name}" is not available.'
        )

        if raise_error:
            raise ImportError(message)

        logger.warning(message)
        return False

    # Download repository if missing
    if not toolkit_folder.exists():

        logger.warning(
            f'External toolkit "{package_name}" is missing. '
            'Downloading from GitHub ...'
        )

        command = ["git", "clone"]

        if branch:
            command.extend(["--branch", branch])

        command.extend([
            repository,
            str(toolkit_folder)
        ])

        try:
            subprocess.run(command, check=True)

        except (subprocess.CalledProcessError, OSError) as exc:
            message = (
                f'Failed to download external toolkit '
                f'"{package_name}": {exc}'
            )

            if raise_error:
                raise ImportError(message) from exc

            logger.error(message)
            return False

    # Check whether repository is pip-installable
    is_python_project = (
        (toolkit_folder / "pyproject.toml").is_file()
        or (toolkit_folder / "setup.py").is_file()
    )

    if is_python_project and install_mode in ("auto", "pip"):

        logger.info(
            f'Installing external toolkit "{package_name}" using pip ...'
        )

        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "-e",
                    str(toolkit_folder)
                ],
                check=True
            )

        except (subprocess.CalledProcessError, OSError) as exc:
            message = (
                f'Failed to install external toolkit '
                f'"{package_name}": {exc}'
            )

            if raise_error:
                raise ImportError(message) from exc

            logger.error(message)
            return False

    else:
        logger.warning(
            f'External toolkit "{package_name}" is not a '
            'pip-installable Python project. Using downloaded source.'
        )

    # Register source paths
    installed = register_toolkit_path(
        package_name,
        toolkit_folder
    )

    if installed:
        logger.info(
            f'External toolkit "{package_name}" is available.'
        )
        return True

    message = (
        f'External toolkit "{package_name}" was downloaded to '
        f'"{toolkit_folder}", but no importable Python module '
        f'named "{package_name}" was found. '
        'Check the repository structure and import name.'
    )

    if raise_error:
        raise ImportError(message)

    logger.warning(message)
    return False