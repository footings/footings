import inspect
import pydoc
from collections.abc import Callable
import os

from jinja2 import FileSystemLoader
from jinja2.sandbox import SandboxedEnvironment
from sphinx.jinja2glue import BuiltinTemplateLoader
from numpydoc.docscrape_sphinx import (
    SphinxDocString,
    SphinxClassDoc,
    SphinxFunctionDoc,
    SphinxObjDoc,
)
from .docscrape import FootingsDoc


class SphinxFootingsDoc(SphinxDocString, FootingsDoc):
    def __init__(self, obj, doc=None, func_doc=None, config={}):
        self.load_config(config)
        FootingsDoc.__init__(self, obj, doc=doc, func_doc=None, config=config)

    def load_config(self, config):
        self.use_plots = config.get("use_plots", False)
        self.use_blockquotes = config.get("use_blockquotes", False)
        self.class_members_toctree = config.get("class_members_toctree", True)
        self.attributes_as_param_list = config.get("attributes_as_param_list", True)
        self.xref_param_type = config.get("xref_param_type", False)
        self.xref_aliases = config.get("xref_aliases", dict())
        self.xref_ignore = config.get("xref_ignore", set())
        self.template = config.get("template", None)
        if self.template is None:
            template_dirs = [os.path.join(os.path.dirname(__file__), "templates")]
            template_loader = FileSystemLoader(template_dirs)
            template_env = SandboxedEnvironment(loader=template_loader)
            self.template = template_env.get_template("footings_docstring.rst")

    def _str_steps(self):
        """Generate RST for steps"""
        out = []
        if self["Steps"]:
            out += self._str_field_list("Steps")
            out += [""]
            for step in self["Steps"]:
                out += self._str_indent([step])
            out += [""]
        return out

    def __str__(self, indent=0, func_role="obj"):
        ns = {
            "signature": self._str_signature(),
            "index": self._str_index(),
            "summary": self._str_summary(),
            "extended_summary": self._str_extended_summary(),
            "parameters": self._str_param_list("Parameters"),
            "modifiers": self._str_param_list("Modifiers"),
            "meta": self._str_param_list("Meta"),
            "placeholders": self._str_param_list("Placeholders"),
            "assets": self._str_param_list("Assets"),
            "steps": self._str_steps(),
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
            "methods": self._str_member_list("Methods"),
        }

        ns = dict((k, "\n".join(v)) for k, v in ns.items())
        rendered = self.template.render(**ns)
        return "\n".join(self._str_indent(rendered.split("\n"), indent))


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

    if what == "class" and hasattr(obj, "run") and hasattr(obj, "audit"):
        what = "footing"
    template_dirs = [os.path.join(os.path.dirname(__file__), "templates")]
    if builder is not None:
        template_loader = BuiltinTemplateLoader()
        template_loader.init(builder, dirs=template_dirs)
    else:
        template_loader = FileSystemLoader(template_dirs)
    template_env = SandboxedEnvironment(loader=template_loader)
    config["template"] = template_env.get_template("footings_docstring.rst")
    if what == "footing":
        return SphinxFootingsDoc(obj, func_doc=SphinxFunctionDoc, doc=doc, config=config)
    elif what == "class":
        return SphinxClassDoc(obj, func_doc=SphinxFunctionDoc, doc=doc, config=config)
    elif what in ("function", "method"):
        return SphinxFunctionDoc(obj, doc=doc, config=config)
    else:
        if doc is None:
            doc = pydoc.getdoc(obj)
        return SphinxObjDoc(obj, doc, config=config)
