import pytest

from footings.assumption_registry import (
    AssumptionRegistry,
    AssumptionSet,
    assumption_registry,
    def_assumption_set,
)


class TestAssumptionSet:

    assumption_set = AssumptionSet(name="Test", description="This is a test.")

    def test_init(self):
        assert isinstance(self.assumption_set, AssumptionSet)
        assert self.assumption_set.name == "Test"
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

    def test_get(self):
        assert self.assumption_set.get("func_1")(1) == 2
        assert self.assumption_set.get("func_2")(1) == 3

    def test_attributes(self):
        pass

    def test_doc(self):
        pass


class TestAssumptionRegistry:
    @assumption_registry
    class Registry:
        One = def_assumption_set(description="Assumption Set One.")
        Two = def_assumption_set(description="Assumption Set Two.")

    def test_init(self):
        assert isinstance(self.Registry, AssumptionRegistry)
        assert isinstance(self.Registry.One, AssumptionSet)
        assert isinstance(self.Registry.Two, AssumptionSet)

    def test_register(self):
        @self.Registry.One.register
        def func_1(x):
            return x + 1

        @self.Registry.register("Two")
        def func_2(x):
            return x + 2

        @self.Registry.register(("One", "Two"))
        def func_3(x):
            return x + 3

        assert list(self.Registry.One.registry.keys()) == ["func_1", "func_3"]
        assert list(self.Registry.Two.registry.keys()) == ["func_2", "func_3"]

        with pytest.raises(ValueError):

            @self.Registry.register(["One", "Two"])
            def func_4(x):
                return x + 4

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

    def test_doc(self):
        pass
