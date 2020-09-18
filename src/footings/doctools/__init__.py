def setup(app, *args, **kwargs):
    from numpydoc.numpydoc import setup
    from .docscrape_sphinx import get_doc_object

    return setup(app, *args, get_doc_object_=get_doc_object, **kwargs)
