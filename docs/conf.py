"""Build public documentation without importing SHYBOX or its GIS stack."""
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[1]
with (ROOT / 'pyproject.toml').open('rb') as handle:
    metadata = tomllib.load(handle)['project']

project = 'SHYBOX'
author = 'CIMA Research Foundation'
copyright = '2026, CIMA Research Foundation'
# Documentation editions are independent of package releases.
release = '0.1.0'
version = release
rst_epilog = f'\n.. |package_version| replace:: {metadata["version"]}\n'
extensions = []
root_doc = 'index'
source_suffix = {'.rst': 'restructuredtext'}
exclude_patterns = ['_build', 'IFS/**', 'MeteoData/**']
language = 'en'
nitpicky = True
html_theme = 'sphinx_rtd_theme'
html_title = f'{project} documentation {release}'
html_theme_options = {'navigation_depth': 3, 'collapse_navigation': False}
html_static_path = ['_static']
html_css_files = ['shybox.css']
html_logo = '_static/images/app_logo_fcima.png'
html_context = {
    'display_github': True,
    'github_user': 'c-hydro',
    'github_repo': 'shybox',
    'github_version': 'main',
    'conf_py_path': '/docs/',
}
html_show_sourcelink = False
