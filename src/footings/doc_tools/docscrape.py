from numpydoc.docscrape import ClassDoc, Reader


class FootingsDoc(ClassDoc):
    sections = {
        "Signature": "",
        "Summary": [""],
        "Extended Summary": [],
        "Parameters": [],
        "Modifiers": [],
        "Meta": [],
        "Placeholders": [],
        "Assets": [],
        "Steps": [],
        "Returns": [],
        "Yields": [],
        "Receives": [],
        "Raises": [],
        "Warns": [],
        "Other Parameters": [],
        "Attributes": [],
        "Methods": [],
        "See Also": [],
        "Notes": [],
        "Warnings": [],
        "References": "",
        "Examples": "",
        "index": {},
    }

    def _parse(self):
        self._doc.reset()
        self._parse_summary()

        sections = list(self._read_sections())
        section_names = set([section for section, content in sections])

        has_returns = "Returns" in section_names
        has_yields = "Yields" in section_names
        # We could do more tests, but we are not. Arbitrarily.
        if has_returns and has_yields:
            msg = "Docstring contains both a Returns and Yields section."
            raise ValueError(msg)
        if not has_yields and "Receives" in section_names:
            msg = "Docstring contains a Receives section but not Yields."
            raise ValueError(msg)

        for (section, content) in sections:
            if not section.startswith(".."):
                section = (s.capitalize() for s in section.split(" "))
                section = " ".join(section)
                if self.get(section):
                    self._error_location(
                        "The section %s appears twice in  %s"
                        % (section, "\n".join(self._doc._str))
                    )

            if section in (
                "Parameters",
                "Modifiers",
                "Meta",
                "Placeholders",
                "Assets",
                "Other Parameters",
                "Attributes",
                "Methods",
            ):
                self[section] = self._parse_param_list(content)
            elif section == "Steps":
                self[section] = self._parse_steps(content)
            elif section in ("Returns", "Yields", "Raises", "Warns", "Receives"):
                self[section] = self._parse_param_list(
                    content, single_element_is_type=True
                )
            elif section.startswith(".. index::"):
                self["index"] = self._parse_index(section, content)
            elif section == "See Also":
                self["See Also"] = self._parse_see_also(content)
            else:
                self[section] = content

    def _parse_steps(self, content):
        r = Reader(content)
        list_ = []
        while not r.eof():
            line = r.read().strip()
            list_.append(line)
        return list_

    def _str_steps(self, name):
        out = []
        if self[name]:
            out += self._str_header(name)
            out += self[name]
            out += [""]
        return out

    def __str__(self, func_role=""):
        out = []
        out += self._str_signature()
        out += self._str_summary()
        out += self._str_extended_summary()
        for param_list in (
            "Parameters",
            "Modifiers",
            "Meta",
            "Placeholders",
            "Assets",
        ):
            out += self._str_param_list(param_list)
        out += self._str_steps("Steps")
        for param_list in (
            "Returns",
            "Yields",
            "Receives",
            "Other Parameters",
            "Raises",
            "Warns",
        ):
            out += self._str_param_list(param_list)
        out += self._str_section("Warnings")
        out += self._str_see_also(func_role)
        for s in ("Notes", "References", "Examples"):
            out += self._str_section(s)
        for param_list in ("Methods",):
            out += self._str_param_list(param_list)
        out += self._str_index()
        return "\n".join(out)
