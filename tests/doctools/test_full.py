from distutils.version import LooseVersion
import os.path as op
import shutil
import re

import pytest
import sphinx
from sphinx.application import Sphinx
from sphinx.util.docutils import docutils_namespace


# Test framework adapted from sphinx-gallery (BSD 3-clause)
@pytest.fixture(scope="module")
def sphinx_app(tmpdir_factory):
    temp_dir = (tmpdir_factory.getbasetemp() / "root").strpath
    src_dir = op.join(op.dirname(__file__), "tinybuild")

    def ignore(src, names):
        return ("_build", "generated")

    shutil.copytree(src_dir, temp_dir, ignore=ignore)
    # For testing iteration, you can get similar behavior just doing `make`
    # inside the tinybuild directory
    src_dir = temp_dir
    conf_dir = temp_dir
    out_dir = op.join(temp_dir, "_build", "html")
    toctrees_dir = op.join(temp_dir, "_build", "toctrees")
    # Set behavior across different Sphinx versions
    kwargs = dict()
    if LooseVersion(sphinx.__version__) >= LooseVersion("1.8"):
        kwargs.update(warningiserror=True, keep_going=True)
    # Avoid warnings about re-registration, see:
    # https://github.com/sphinx-doc/sphinx/issues/5038
    with docutils_namespace():
        app = Sphinx(
            src_dir, conf_dir, out_dir, toctrees_dir, buildername="html", **kwargs
        )
        # need to build within the context manager
        # for automodule and backrefs to work
        app.build(False, [])
    return app


def test_rst(sphinx_app):
    src_dir, _ = sphinx_app.srcdir, sphinx_app.outdir
    generated_file = op.join(src_dir, "generated", "footings_test_module.DocModel.rst")
    with open(generated_file, "r") as fid:
        generated = fid.read()
    expected_file = op.join(
        "tests", "doctools", "output", "footings_test_module.DocModel.rst"
    )
    with open(expected_file, "r") as fid:
        expected = fid.read()
    assert generated == expected


def test_html(sphinx_app):
    _, out_dir = sphinx_app.srcdir, sphinx_app.outdir
    generated_file = op.join(out_dir, "generated", "footings_test_module.DocModel.html")
    with open(generated_file, "r") as fid:
        generated = fid.read()
    expected_file = op.join(
        "tests", "doctools", "output", "footings_test_module.DocModel.html"
    )
    with open(expected_file, "r") as fid:
        expected = fid.read()
    skip_lines = ["http://sphinx-doc.org", "https://github.com/bitprophet/alabaster"]
    diff = []
    for idx, lines in enumerate(zip(generated.split("\n"), expected.split("\n"))):
        gen = re.sub("in Python v3.\\d", "", lines[0].strip())
        exp = re.sub("in Python v3.\\d", "", lines[1].strip())
        if gen != exp:
            if not any([x in gen for x in skip_lines]):
                diff.append((idx, gen, exp))
    print("Differnce in html:")
    print(diff)
    assert len(diff) == 0
