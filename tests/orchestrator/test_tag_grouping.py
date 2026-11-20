import unittest

from shybox.orchestrator_toolkit.lib_orchestrator_process import _build_tag, _group_tag


class TestTagGrouping(unittest.TestCase):
    def test_groups_ensemble_members_and_keeps_observations_as_values(self):
        data = {_build_tag("data|q_sim", data_idx=member, time_idx=0): member for member in range(1, 5)}
        data["data|q_sim::data|1@time|1"] = 10
        data["data|q_obs@time|0"] = 20
        data["data|air_t_obs@time|0"] = 30

        self.assertEqual(_group_tag(data), {
            "data|q_sim": {
                "data|1@time|0": 1,
                "data|2@time|0": 2,
                "data|3@time|0": 3,
                "data|4@time|0": 4,
                "data|1@time|1": 10,
            },
            "data|q_obs": 20,
            "data|air_t_obs": 30,
        })

    def test_single_member_respects_flatten_single(self):
        data = {"data|q_sim::data|1@time|0": 5}
        self.assertEqual(_group_tag(data), {"data|q_sim": {"data|1@time|0": 5}})
        self.assertEqual(_group_tag(data, flatten_single=True), {"data|q_sim": 5})

    def test_preserves_placeholder_based_groups(self):
        data = {"data|{id}::data|1@time|0": 5, "code|{t}@time|0": 6}
        self.assertEqual(_group_tag(data), {"data": {"data|1@time|0": 5}, "code": {"time|0": 6}})
