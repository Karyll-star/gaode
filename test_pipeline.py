import unittest

from pipeline import collect_poi, score_candidate


class StubClient:
    def __init__(self):
        self._poi = {
            "id": "B0FFHA400C",
            "name": "柘林幼儿园",
            "typecode": "141204",
            "location": "121.485087,30.838964",
            "distance": "388",
        }

    def place_around(self, lng, lat, radius, typecodes, page_size=25):
        if typecodes == ["141200"] or typecodes == ["1412", "1413"]:
            return [self._poi]
        return []


class PipelineRegressionTests(unittest.TestCase):
    def test_collect_poi_should_keep_governance_entries_under_community_service(self):
        class GovernanceStubClient:
            def place_around(self, lng, lat, radius, typecodes, page_size=25):
                if typecodes == ["141200"]:
                    return [
                        {
                            "id": "GOV001",
                            "name": "柘林居委会党群服务中心",
                            "typecode": "141200",
                            "location": "121.400001,30.800001",
                            "distance": "210",
                        },
                        {
                            "id": "SCH001",
                            "name": "柘林幼儿园",
                            "typecode": "141204",
                            "location": "121.400002,30.800002",
                            "distance": "220",
                        },
                    ]
                if typecodes == ["1412", "1413"]:
                    return [
                        {
                            "id": "SCH001",
                            "name": "柘林幼儿园",
                            "typecode": "141204",
                            "location": "121.400002,30.800002",
                            "distance": "220",
                        }
                    ]
                return []

        hits, present = collect_poi(GovernanceStubClient(), 121.4, 30.8, (200, 300, 450))

        gov_hits = [h for h in hits if h["group"] == "community_service" and h["id"] == "GOV001"]
        school_in_community = [h for h in hits if h["group"] == "community_service" and h["id"] == "SCH001"]

        self.assertEqual(1, len(gov_hits))
        self.assertEqual(0, len(school_in_community))
        self.assertIn("community_service", present["A"])

    def test_collect_poi_should_not_misclassify_school_as_community_service(self):
        hits, present = collect_poi(StubClient(), 121.4, 30.8, (200, 300, 450))

        school_hits = [h for h in hits if h["group"] == "school" and h["id"] == "B0FFHA400C"]
        community_hits = [h for h in hits if h["group"] == "community_service" and h["id"] == "B0FFHA400C"]

        self.assertEqual(1, len(school_hits))
        self.assertEqual(0, len(community_hits))
        self.assertIn("school", present["B"])
        self.assertNotIn("community_service", present["A"])

    def test_score_boundary_should_count_school_even_with_same_poi_id_in_other_group(self):
        hits = [
            {
                "category": "A",
                "group": "community_service",
                "radius": 200,
                "id": "B0FFHA400C",
                "name": "柘林幼儿园",
                "typecode": "141204",
                "lng": 121.485087,
                "lat": 30.838964,
                "distance": 188,
            },
            {
                "category": "B",
                "group": "school",
                "radius": 200,
                "id": "B0FFHA400C",
                "name": "柘林幼儿园",
                "typecode": "141204",
                "lng": 121.485087,
                "lat": 30.838964,
                "distance": 188,
            },
        ]
        present = {"A": {"community_service"}, "B": {"school"}, "C": set()}

        _, dims, _, _ = score_candidate(present, hits, (200, 300, 450))

        # 学校属于核心半径切割线索，边界分应从 5 降到 4
        self.assertEqual(4, dims["边界清晰"])

    def test_score_pilot_should_dedupe_same_c_poi_across_groups(self):
        hits = [
            {
                "category": "C",
                "group": "gate",
                "radius": 200,
                "id": "B0C_DUP_1",
                "name": "地面停车场",
                "typecode": "150900",
                "lng": 121.1,
                "lat": 30.1,
                "distance": 120,
            },
            {
                "category": "C",
                "group": "parking",
                "radius": 200,
                "id": "B0C_DUP_1",
                "name": "地面停车场",
                "typecode": "150900",
                "lng": 121.1,
                "lat": 30.1,
                "distance": 120,
            },
        ]
        present = {"A": {"food"}, "B": set(), "C": {"gate", "parking"}}

        _, dims, _, _ = score_candidate(present, hits, (200, 300, 450))

        # 同一 C 类 POI 不应因跨子类重复计分：唯一命中 1 个 => 3 分
        self.assertEqual(3, dims["试点可行"])

    def test_score_boundary_should_not_double_deduct_same_split_poi_across_groups(self):
        hits = [
            {
                "category": "A",
                "group": "sanitation",
                "radius": 200,
                "id": "B0ROAD1",
                "name": "海湾路",
                "typecode": "190301",
                "lng": 121.2,
                "lat": 30.2,
                "distance": 100,
            },
            {
                "category": "C",
                "group": "trash_point",
                "radius": 200,
                "id": "B0ROAD1",
                "name": "海湾路",
                "typecode": "190301",
                "lng": 121.2,
                "lat": 30.2,
                "distance": 100,
            },
        ]
        present = {"A": {"sanitation"}, "B": set(), "C": {"trash_point"}}

        _, dims, _, _ = score_candidate(present, hits, (200, 300, 450))

        # 同一切割 POI 跨 group 命中只扣一次：5 -> 4
        self.assertEqual(4, dims["边界清晰"])


if __name__ == "__main__":
    unittest.main()
