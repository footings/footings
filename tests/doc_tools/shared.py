from footings import (
    Footing,
    model,
    step,
    define_parameter,
    define_meta,
    define_modifier,
    define_asset,
)


@model(steps=["_step_1", "_step_2"])
class DocModel(Footing):
    """This is a model to test documentation."""

    param_1 = define_parameter(dtype=int, description="This is parameter 1.")
    param_2 = define_parameter(dtype=int, description="This is parameter 2.")
    modif_1 = define_modifier(dtype=int, default=1, description="This is modifier 1.")
    modif_2 = define_modifier(dtype=int, default=2, description="This is modifier 2.")
    meta_1 = define_meta(meta="meta_1", description="This is meta 1.")
    meta_2 = define_meta(meta="meta_2", description="This is meta 2.")
    asset_1 = define_asset(dtype=int, description="This is asset 1.")
    asset_2 = define_asset(dtype=int, description="This is asset 2.")

    @step(uses=["param_1", "modif_1"], impacts=["asset_1"])
    def _step_1(self):
        """Step 1 summary"""
        pass

    @step(uses=["param_2", "modif_2"], impacts=["asset_2"])
    def _step_2(self):
        """Step 2 summary"""
        pass
