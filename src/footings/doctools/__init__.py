"""A Sphinx extension that uses and builds upon the Sphinx extension numpydoc_ to build custom documentation for models built using the Footings framework.

Documentation for a Footings based model includes sections for Parameters, Modifiers, Meta, Placeholders, Assets, and Steps as well as the standard sections of the numpy docstring format.

To use this extension, include ``footings.doctools`` as an extension with your conf.py file.

.. code-block:: python

   extensions = [
       "footings.doctools",
       ...
    ]

.. _numpydoc: https://numpydoc.readthedocs.io/en/latest/
"""


def setup(app, *args, **kwargs):
    from numpydoc.numpydoc import setup
    from .docscrape_sphinx import get_doc_object

    return setup(app, *args, get_doc_object_=get_doc_object, **kwargs)
