"""
Library Features:

Name:          lib_orchestrator_utils_discover
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261112'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import ast
from hashlib import sha256
from importlib import import_module, util
from pathlib import Path
import sys
import types
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to search python file(s) where the methods are stored
def _location_files(location):
    """Yield (source file, import name or None, root directory) candidates."""
    path = Path(location).expanduser()
    if path.exists():
        path = path.resolve()
        if path.is_file():
            if path.suffix != '.py':
                raise ValueError(f'Process source must be a Python file: {path}')
            yield path, None, path.parent
            return
        prefix = None
    else:
        try:
            spec = util.find_spec(str(location))
        except (ImportError, AttributeError, ValueError) as exc:
            raise ValueError(f'Cannot find process location {location!r}') from exc
        if spec is None:
            raise ValueError(f'Cannot find process location {location!r}')
        if spec.submodule_search_locations is None:
            if not spec.origin or not spec.origin.endswith('.py'):
                raise ValueError(f'Process location has no Python source: {location!r}')
            yield Path(spec.origin), str(location), Path(spec.origin).parent
            return
        for root in spec.submodule_search_locations:
            yield from _package_files(Path(root), str(location))
        return
    yield from _package_files(path, prefix)

# helper to search package files
def _package_files(root, prefix):
    for path in sorted(root.rglob('*.py')):
        relative = path.relative_to(root)
        if any(part.startswith('.') or part == '__pycache__' for part in relative.parts):
            continue
        parts = list(relative.with_suffix('').parts)
        if parts[-1] == '__init__':
            parts.pop()
        module = '.'.join([prefix, *parts]) if prefix else None
        yield path, module, root

# method to search process module by fx name and locations
def find_process_module(function_name, search_locations=None):
    """Return the module name or source Path defining a top-level function.

    ``search_locations`` extends the built-in processing toolkit locations;
    it can be one location or a sequence. Dotted package/module names are
    preferred for custom code that uses package-relative imports. Directory
    and file locations are also supported. No candidate module is imported
    while inspecting its functions (resolving dotted locations may import
    their parent packages).

    Missing functions and ambiguous definitions raise ValueError. A matching
    function must still use ``@as_process`` to be loaded into the registry.
    """
    if not isinstance(function_name, str) or not function_name.isidentifier():
        raise ValueError('Process function name must be a Python identifier.')
    package_root = Path(__file__).resolve().parents[1]
    candidates = []
    for root in sorted(package_root.glob('processing*_toolkit')):
        if root.is_dir():
            candidates.extend(_package_files(root, f'shybox.{root.name}'))
    if isinstance(search_locations, (str, Path)):
        search_locations = [search_locations]
    for location in search_locations or ():
        candidates.extend(_location_files(location))

    matches = {}
    for path, module, _ in candidates:
        path = path.resolve()
        if path in matches:
            continue
        tree = ast.parse(util.decode_source(path.read_bytes()), filename=str(path))
        if any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
               and node.name == function_name for node in tree.body):
            matches[path] = module or path
    if not matches:
        raise ValueError(f'Process {function_name!r} was not found in the processing toolkits '
                         'or additional search locations.')
    if len(matches) > 1:
        raise ValueError(f'Ambiguous process {function_name!r}; definitions found in: '
                         + ', '.join(str(path) for path in matches))
    return next(iter(matches.values()))

# method to load process(es)
def load_process(function_name, search_locations=None):
    """Find, import and return a registered process; keep existing registrations.

    Example::

        process = load_process('mask_data_by_ref')
        process = load_process('custom_method', ['my_project.processes', '/opt/methods'])

    Imports execute Python code, so extra locations should contain trusted code.
    This helper configures registration only; it never executes the process.
    """
    from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES

    if not isinstance(function_name, str) or not function_name.isidentifier():
        raise ValueError('Process function name must be a Python identifier.')
    if function_name in PROCESSES:
        return PROCESSES[function_name]
    location = find_process_module(function_name, search_locations)
    if isinstance(location, str):
        module = import_module(location)
    else:
        # A namespace keeps sibling/relative imports local without changing sys.path.
        namespace = '_shybox_processes_' + sha256(str(location.parent).encode()).hexdigest()[:16]
        if namespace not in sys.modules:
            package = types.ModuleType(namespace)
            package.__path__ = [str(location.parent)]
            package.__spec__ = util.spec_from_loader(namespace, loader=None, is_package=True)
            sys.modules[namespace] = package
        module = import_module(f'{namespace}.{location.stem}')
    if function_name not in PROCESSES:
        raise ValueError(f'Process {function_name!r} in {location} did not register itself; '
                         'decorate it with @as_process.')
    return PROCESSES[function_name]
