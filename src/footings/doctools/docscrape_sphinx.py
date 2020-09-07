import inspect
import pydoc
from collections.abc import Callable
import os

from jinja2 import FileSystemLoader
from jinja2.sandbox import SandboxedEnvironment
from sphinx.jinja2glue import BuiltinTemplateLoader

from numpydoc.docscrape_sphinx import SphinxDocString
from numpydoc.docscrape import ClassDoc, FunctionDoc


class SphinxFootingsDocString(SphinxDocString):
    def __str__(self, indent=0, func_role="obj"):
        ns = {
            "signature": self._str_signature(),
            "index": self._str_index(),
            "summary": self._str_summary(),
            "extended_summary": self._str_extended_summary(),
            "parameters": self._str_param_list("Parameters"),
            "modifiers": self._str_param_list("Modifiers"),
            "meta": self._str_param_list("Meta"),
            "assets": self._str_param_list("Assets"),
            "uses": self._str_param_list("Uses"),
            "impacts": self._str_param_list("Impacts"),
            "returns": self._str_returns("Returns"),
            "yields": self._str_returns("Yields"),
            "receives": self._str_returns("Receives"),
            "other_parameters": self._str_param_list("Other Parameters"),
            "raises": self._str_returns("Raises"),
            "warns": self._str_returns("Warns"),
            "warnings": self._str_warnings(),
            "see_also": self._str_see_also(func_role),
            "notes": self._str_section("Notes"),
            "references": self._str_references(),
            "examples": self._str_examples(),
            "attributes": self._str_param_list("Attributes", fake_autosummary=True)
            if self.attributes_as_param_list
            else self._str_member_list("Attributes"),
            "methods": self._str_member_list("Methods"),
        }
        ns = dict((k, "\n".join(v)) for k, v in ns.items())

        rendered = self.template.render(**ns)
        return "\n".join(self._str_indent(rendered.split("\n"), indent))


class SphinxFunctionDoc(SphinxFootingsDocString, FunctionDoc):
    def __init__(self, obj, doc=None, config={}):
        self.load_config(config)
        FunctionDoc.__init__(self, obj, doc=doc, config=config)


class SphinxClassDoc(SphinxFootingsDocString, ClassDoc):
    def __init__(self, obj, doc=None, func_doc=None, config={}):
        self.load_config(config)
        ClassDoc.__init__(self, obj, doc=doc, func_doc=None, config=config)


class SphinxObjDoc(SphinxFootingsDocString):
    def __init__(self, obj, doc=None, config={}):
        self._f = obj
        self.load_config(config)
        SphinxDocString.__init__(self, doc, config=config)


def get_doc_object(obj, what=None, doc=None, config={}, builder=None):
    if what is None:
        if inspect.isclass(obj):
            what = "class"
        elif inspect.ismodule(obj):
            what = "module"
        elif isinstance(obj, Callable):
            what = "function"
        else:
            what = "object"

    template_dirs = [os.path.join(os.path.dirname(__file__), "templates")]
    print("\n")
    print(template_dirs)
    print(os.listdir(template_dirs[0]))
    print("\n")
    if builder is not None:
        template_loader = BuiltinTemplateLoader()
        template_loader.init(builder, dirs=template_dirs)
    else:
        template_loader = FileSystemLoader(template_dirs)
    template_env = SandboxedEnvironment(loader=template_loader)
    print("\n")
    print(template_loader)
    print("\n")
    config["template"] = template_env.get_template("footings_docstring.rst")

    if what == "class":
        return SphinxClassDoc(obj, func_doc=SphinxFunctionDoc, doc=doc, config=config)
    elif what in ("function", "method"):
        return SphinxFunctionDoc(obj, doc=doc, config=config)
    else:
        if doc is None:
            doc = pydoc.getdoc(obj)
        return SphinxObjDoc(obj, doc, config=config)
