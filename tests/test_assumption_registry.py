from inspect import signature
from types import MethodType

from footings.assumption_registry import (
    Assumption,
    AssumptionRegistry,
    AssumptionSet,
    AssumptionSetRegistry,
    assumption_registry,
    def_assumption_set,
    make_assumption_set,
)


def func(x, y):
    """This is func."""
    return x + y


class TestAssumption:

    asn_1 = Assumption.create(name="Assumption1", assumption=func)
    asn_2 = Assumption.create(
        name="Assumption2", description="This is assumption 2.", assumption=func
    )
    asn_3 = Assumption.create(
        name="Assumption3", description="This is assumption 3.", uses=(), assumption=func,
    )
    asn_4 = Assumption.create(
        name="Assumption4",
        description="This is assumption 4.",
        assumption=func,
        metadata={"x": "This is x.", "y": "This is y."},
    )
    asn_5 = Assumption.create(
        name="Assumption5", assumption=func, doc_generator=lambda self: ""
    )
    asn_6 = Assumption.create(
        name="Assumption6", assumption=lambda self, x, y: x + y, bounded=True
    )

    def test_init(self):
        assert isinstance(self.asn_1, Assumption)
        assert isinstance(self.asn_2, Assumption)
        assert isinstance(self.asn_3, Assumption)
        assert isinstance(self.asn_4, Assumption)
        assert isinstance(self.asn_5, Assumption)
        # assert isinstance(self.asn_6, Assumption)

    def test_signature(self):
        assert signature(self.asn_1) == signature(func)
        assert signature(self.asn_2) == signature(func)
        assert signature(self.asn_3) == signature(func)
        assert signature(self.asn_4) == signature(func)
        assert signature(self.asn_5) == signature(func)
        # assert signature(self.asn_6) == signature(func)

    def test_doc(self):
        assert (
            self.asn_1.__doc__
            == "**Assumption1 [get('func', *args, **kwargs)] :**\nThis is func.\n\n"
        )
        assert (
            self.asn_2.__doc__
            == "**Assumption2 [get('func', *args, **kwargs)] :**\nThis is assumption 2.\n\n"
        )
        assert (
            self.asn_3.__doc__
            == "**Assumption3 [get('func', *args, **kwargs)] :**\nThis is assumption 3.\n\n"
        )
        assert (
            self.asn_4.__doc__
            == "**Assumption4 [get('func', *args, **kwargs)] :**\nThis is assumption 4.\n\n"
        )
        assert self.asn_5.__doc__ == ""

    def test_call(self):
        assert self.asn_1(x=1, y=2) == 3
        assert self.asn_2(x=1, y=2) == 3
        assert self.asn_3(x=1, y=2) == 3
        assert self.asn_4(x=1, y=2) == 3
        assert self.asn_5(x=1, y=2) == 3


class TestAssumptionSetRegistry:

    assumption_set = AssumptionSetRegistry(description="This is a test.")

    def test_init(self):
        assert isinstance(self.assumption_set, AssumptionSetRegistry)
        assert self.assumption_set.description == "This is a test."
        assert self.assumption_set.registry == {}

    def test_register(self):
        def func_1(x: int):
            return x + 1

        self.assumption_set.register(func_1)

        @self.assumption_set.register
        def func_2(x: int):
            return x + 2

        assert list(self.assumption_set.registry.keys()) == ["func_1", "func_2"]

    def test_attributes(self):
        pass

    def test_doc(self):
        pass


class TestAssumptionSet:

    registry = {
        "asn_1": Assumption.create(name="Assumption1", assumption=lambda x, y: x + y),
        "asn_2": Assumption.create(
            name="Assumption2", assumption=lambda self, x, y: x + y, bounded=True
        ),
    }
    asn_set_1 = make_assumption_set(
        name="AssumptionSet1", description="This is AssumptionSet1.", registry=registry
    )

    def test_init(self):
        assert isinstance(self.asn_set_1, AssumptionSet)
        assert isinstance(self.asn_set_1.asn_1, Assumption)
        assert isinstance(self.asn_set_1.asn_2, MethodType)
        assert isinstance(self.asn_set_1.asn_2.__func__, Assumption)

    def test_get(self):
        assert self.asn_set_1.get("asn_1")(x=1, y=2) == 3
        assert self.asn_set_1.asn_1(x=1, y=2) == 3
        assert self.asn_set_1.get("asn_2")(x=1, y=2) == 3
        assert self.asn_set_1.asn_2(x=1, y=2) == 3

    # def test_doc(self):
    #     print(self.asn_set_1.__doc__)
    #     assert 0


class TestAssumptionRegistry:
    @assumption_registry
    class Registry:
        """Test Registry."""

        One = def_assumption_set(description="Assumption Set One.")
        Two = def_assumption_set(description="Assumption Set Two.")

        @One.register
        def func_1(x):
            """This is func1."""
            return x + 1

        @Two.register(name="Function 2")
        def func_2(x):
            """This is func2."""
            return x + 2

        @One.register(name="Function 3")
        @Two.register(name="Function 3")
        def func_3(x):
            """This is func3."""
            return x + 3

    def test_init(self):
        assert isinstance(self.Registry, AssumptionRegistry)
        assert isinstance(self.Registry.One, AssumptionSet)
        assert isinstance(self.Registry.Two, AssumptionSet)

    def test_register(self):
        assert isinstance(self.Registry.One.func_1, Assumption)
        assert isinstance(self.Registry.One.func_3, Assumption)
        assert isinstance(self.Registry.Two.func_2, Assumption)
        assert isinstance(self.Registry.Two.func_3, Assumption)

    def test_get(self):
        assert self.Registry.get("One", "func_1")(1) == 2
        assert self.Registry.One.get("func_1")(1) == 2
        assert self.Registry.get("One", "func_3")(1) == 4
        assert self.Registry.One.get("func_3")(1) == 4
        assert self.Registry.get("Two", "func_2")(1) == 3
        assert self.Registry.Two.get("func_2")(1) == 3
        assert self.Registry.get("Two", "func_3")(1) == 4
        assert self.Registry.Two.get("func_3")(1) == 4

    def test_attributes(self):
        pass

    # def test_doc(self):
    #     print(self.Registry.__doc__)
    #     assert 0
    #     doc = [
    #         "Test Registry.\n",
    #         "\n",
    #         "Assumption Sets\n",
    #         "---------------\n",
    #         "One : Assumption Set One.\n",
    #         "\tfunc_1(x)\n",
    #         "\t\tThis is func1.\n",
    #         "\tfunc_3(x)\n",
    #         "\t\tThis is func3.\n",
    #         "Two : Assumption Set Two.\n",
    #         "\tfunc_2(x)\n",
    #         "\t\tThis is func2.\n",
    #         "\tfunc_3(x)\n",
    #         "\t\tThis is func3.\n",
    #     ]
    #     assert self.Registry.__doc__ == "".join(doc)
