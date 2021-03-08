import pytest

from footings.scenario_registry import (
    ScenarioRegistry,
    def_attribute,
    scenario_registry,
)


class TestScenarioRegistry:
    @scenario_registry
    class Scenarios:
        sens_1 = def_attribute(default=1, dtype=int, description="Sensitivity 1")
        sens_2 = def_attribute(default=2, dtype=int, description="Sensitivity 2")

    def test_init(self):
        assert isinstance(self.Scenarios, ScenarioRegistry)
        assert self.Scenarios.sens_1 == 1
        assert self.Scenarios.sens_2 == 2
        assert list(self.Scenarios.registry.keys()) == ["base"]

    def test_register(self):
        @self.Scenarios.register
        class update_1:
            sens_1 = 2

        @self.Scenarios.register
        class update_2:
            sens_2 = 3

        @self.Scenarios.register
        class update_3:
            sens_1 = 2
            sens_2 = 3

        assert list(self.Scenarios.registry.keys()) == [
            "base",
            "update_1",
            "update_2",
            "update_3",
        ]

        with pytest.raises(TypeError):

            @self.Scenarios.register
            class update_4:
                sens_1 = 2
                sens_2 = 3
                sens_3 = 4

        with pytest.raises(TypeError):

            @self.Scenarios.register
            class update_5:
                sens_1 = "hello"

    def test_get(self):
        assert self.Scenarios.get("base") == {"sens_1": 1, "sens_2": 2}
        assert self.Scenarios.get("update_1") == {"sens_1": 2, "sens_2": 2}
        assert self.Scenarios.get("update_2") == {"sens_1": 1, "sens_2": 3}
        assert self.Scenarios.get("update_3") == {"sens_1": 2, "sens_2": 3}

    def test_scenario_keys(self):
        assert list(self.Scenarios.scenario_keys()) == [
            "base",
            "update_1",
            "update_2",
            "update_3",
        ]

    def test_scenario_items(self):
        assert self.Scenarios.scenario_items() == {
            "base": {"sens_1": 1, "sens_2": 2},
            "update_1": {"sens_1": 2, "sens_2": 2},
            "update_2": {"sens_1": 1, "sens_2": 3},
            "update_3": {"sens_1": 2, "sens_2": 3},
        }

    def test_doc(self):
        pass
