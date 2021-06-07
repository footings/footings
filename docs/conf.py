# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
import os
import sys

from footings import __version__ as version

sys.path.insert(0, os.path.abspath("./.."))


# -- Project information -----------------------------------------------------

project = "Footings"
copyright = "2020, Dustin Tindall"
author = "Dustin Tindall"

# The full version, including alpha/beta/rc tags
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "myst_nb",
]
# autodoc settings
autodoc_default_options = {"exclude-members": "__init__, __call__, __signature__"}

# autosummary settings
autosummary_generate = True
autosummary_generate_overwrite = True
add_module_names = False

# myst_nb settings
# jupyter_execute_notebooks = "cache"
# execution_in_temp = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**.ipynb_checkpoints"]

# The master toctree document.
master_doc = "index"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# html_sidebars = {"**": []}

html_theme_options = {
    "github_url": "https://github.com/footings/footings",
    "search_bar_position": "navbar",
    "search_bar_text": "Search this site...",
}

source_suffix = {
    ".rst": "restructuredtext",
    ".ipynb": "myst-nb",
    ".myst": "myst-nb",
}


def skip(app, what, name, obj, would_skip, options):
    if name == "__call__":
        return False
    elif name[:1] == "_":
        return True
    elif name[:2] == "__":
        return True
    return False


def setup(app):
    app.connect("autodoc-skip-member", skip)
