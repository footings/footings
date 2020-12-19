from numpydoc.docscrape import Parameter

from footings.doc_tools.docscrape import FootingsDoc

from .shared import DocModel


def test_footings_doc():
    doc = FootingsDoc(DocModel)

    assert doc["Parameters"] == [
        Parameter("param_1", int.__qualname__, ["This is parameter 1."]),
        Parameter("param_2", int.__qualname__, ["This is parameter 2."]),
    ]

    assert doc["Sensitivities"] == [
        Parameter("sensit_1", int.__qualname__, ["This is sensitivity 1."]),
        Parameter("sensit_2", int.__qualname__, ["This is sensitivity 2."]),
    ]

    assert doc["Meta"] == [
        Parameter("meta_1", "", ["This is meta 1."]),
        Parameter("meta_2", "", ["This is meta 2."]),
    ]

    assert doc["Returns"] == [
        Parameter("return_1", int.__qualname__, ["This is return 1."]),
        Parameter("return_2", int.__qualname__, ["This is return 2."]),
    ]

    assert doc["Steps"] == [
        "1) _step_1 - Step 1 summary",
        "2) _step_2 - Step 2 summary",
    ]
