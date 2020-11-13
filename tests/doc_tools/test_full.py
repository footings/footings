from distutils.version import LooseVersion
from html.parser import HTMLParser
import os.path as op
import shutil

import pytest
import sphinx
from sphinx.application import Sphinx
from sphinx.util.docutils import docutils_namespace


# copied from https://github.com/numpy/numpydoc/blob/master/numpydoc/tests/test_full.py
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


class ParseSphinxHTML(HTMLParser):
    sections = ["Parameters", "Modifiers", "Meta", "Assets", "Steps", "Methods"]

    def __init__(self):
        HTMLParser.__init__(self)
        self.tag = False
        self.collect = []

    def handle_starttag(self, tag, attrs):
        if tag == "dt" or tag == "p":
            self.tag = True

    def handle_endtag(self, tag):
        if tag == "dt":
            self.tag = False

    def handle_data(self, data):
        if self.tag and "\n" not in data:
            self.collect.append(data)


def scrape_html(file):
    with open(file, "r") as f:
        collect = False
        main = []
        sub = []
        for line in f.readlines():
            if "py method" in line:
                collect = False
            if any(x in line for x in ["field-list"]):
                if sub != []:
                    main.append("".join(sub))
                collect = True
                sub = []
            if collect:
                sub.append(line)

    ret = []
    for item in main:
        parser = ParseSphinxHTML()
        parser.feed(item)
        parser.close()
        ret.extend(parser.collect)
    return ret


def test_rst(sphinx_app):
    def strip_str(x: str):
        return x.replace("\n", "").replace(" ", "")

    src_dir = sphinx_app.srcdir
    generated_file = op.join(src_dir, "generated", "footings_test_module.DocModel.rst")
    with open(generated_file, "r") as fid:
        generated = fid.read()
    expected_file = op.join(
        "tests", "doc_tools", "output", "footings_test_module.DocModel.rst"
    )
    with open(expected_file, "r") as fid:
        expected = fid.read()
    assert strip_str(generated) == strip_str(expected)


def test_html(sphinx_app):
    """Test that class documentation is reasonable."""
    out_dir = sphinx_app.outdir
    src_html = op.join(
        "tests", "doc_tools", "output", "footings_test_module.DocModel.html"
    )
    gen_html = op.join(out_dir, "generated", "footings_test_module.DocModel.html")

    assert scrape_html(src_html) == scrape_html(gen_html)
