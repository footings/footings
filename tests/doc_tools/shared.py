from footings import (
    Footing,
    model,
    step,
    define_parameter,
    define_meta,
    define_sensitivity,
    define_return,
)


@model(steps=["_step_1", "_step_2"])
class DocModel(Footing):
    """This is a model to test documentation."""

    param_1 = define_parameter(dtype=int, description="This is parameter 1.")
    param_2 = define_parameter(dtype=int, description="This is parameter 2.")
    sensit_1 = define_sensitivity(
        dtype=int, default=1, description="This is sensitivity 1."
    )
    sensit_2 = define_sensitivity(
        dtype=int, default=2, description="This is sensitivity 2."
    )
    meta_1 = define_meta(meta="meta_1", description="This is meta 1.")
    meta_2 = define_meta(meta="meta_2", description="This is meta 2.")
    return_1 = define_return(dtype=int, description="This is return 1.")
    return_2 = define_return(dtype=int, description="This is return 2.")

    @step(uses=["param_1", "sensit_1"], impacts=["return_1"])
    def _step_1(self):
        """Step 1 summary"""
        pass

    @step(uses=["param_2", "sensit_2"], impacts=["return_2"])
    def _step_2(self):
        """Step 2 summary"""
        pass
