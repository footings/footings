from footings.doc_tools.docscrape_sphinx import SphinxFootingsDoc, get_doc_object

from .shared import DocModel


def test_sphinx_footings_doc():
    doc_1 = SphinxFootingsDoc(DocModel)
    doc_2 = get_doc_object(DocModel)
    assert doc_1 == doc_2

    expected = """
This is a model to test documentation.


:Parameters:

    **param_1** : int
        This is parameter 1.

    **param_2** : int
        This is parameter 2.

:Sensitivities:

    **sensit_1** : int
        This is sensitivity 1.

    **sensit_2** : int
        This is sensitivity 2.

:Meta:

    **meta_1**
        This is meta 1.

    **meta_2**
        This is meta 2.


:Returns:

    **return_1** : int
        This is return 1.

    **return_2** : int
        This is return 2.




:Steps:

    1) _step_1 - Step 1 summary
    2) _step_2 - Step 2 summary








.. rubric:: Methods

.. autosummary::
   :toctree:

   audit
   run
   visualize

    """
    for idx, tup in enumerate(zip(str(doc_1).split("\n"), expected.split("\n"))):
        if tup[0] != tup[1]:
            print(idx)
            print(tup[0])
            print(tup[1])
    for x in str(doc_1).split("\n"):
        print(x)
    assert all([a == e for a, e in zip(str(doc_1).split("\n"), expected.split("\n"))])
