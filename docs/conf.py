# Sphinx configuration for Aerospike Python SDK docs

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(".."))

project = "Aerospike Python SDK"
copyright = "2025-2026, Aerospike, Inc."
author = "Aerospike, Inc."
# Single source of truth: the top-level VERSION file (also consumed by
# `bin/get-version` and `[tool.setuptools.dynamic]` in pyproject.toml).
release = (Path(__file__).resolve().parent.parent / "VERSION").read_text().strip()
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_design",
    "sphinx_autodoc_typehints",
]

# -- MyST-Parser --------------------------------------------------------
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# -- Autodoc -------------------------------------------------------------
# If aerospike_async (PAC) is not installed, uncomment the next line:
# autodoc_mock_imports = ["aerospike_async"]
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "member-order": "bysource",
    "undoc-members": False,
}
autodoc_typehints = "signature"
set_type_checking_flag = True
autodoc_class_signature = "separated"
autoclass_content = "class"

# -- Napoleon (Google-style docstrings) ----------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_examples = True
napoleon_preprocess_types = True

# -- Intersphinx ---------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# -- Theme ---------------------------------------------------------------
html_theme = "furo"
# When RTD is building a tag, point furo's "Edit on GitHub" links at the tag
# itself (READTHEDOCS_GIT_IDENTIFIER is the ref RTD checked out). For local
# dev builds and branch-driven RTD builds, fall back to the active integration
# branch.
_source_branch = os.environ.get("READTHEDOCS_GIT_IDENTIFIER", "dev")
html_theme_options = {
    "source_repository": "https://github.com/aerospike/aerospike-client-python-sdk",
    "source_branch": _source_branch,
    "source_directory": "docs/",
}
html_title = "Aerospike Python SDK"

# -- Misc ----------------------------------------------------------------
exclude_patterns = ["_build"]
pygments_style = "sphinx"
pygments_dark_style = "monokai"

suppress_warnings = [
    "autodoc.duplicate_object",
]
autodoc_preserve_defaults = True


def setup(app):
    """Filter out 'duplicate object description' warnings for dataclass fields."""
    import logging

    class _DuplicateFilter(logging.Filter):
        def filter(self, record):
            return "duplicate object description" not in record.getMessage()

    for name in list(logging.Logger.manager.loggerDict) + ["sphinx", "sphinx.domains"]:
        logging.getLogger(name).addFilter(_DuplicateFilter())
    logging.getLogger().addFilter(_DuplicateFilter())
