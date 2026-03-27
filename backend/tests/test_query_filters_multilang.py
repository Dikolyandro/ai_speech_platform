import unittest

from app.services.query_filters import build_dynamic_filters_and_order


class TestQueryFiltersMultilang(unittest.TestCase):
    def setUp(self) -> None:
        self.cols = [
            ("Gender", "TEXT"),
            ("School_Type", "TEXT"),
            ("Attendance", "DOUBLE"),
            ("Exam_Score", "DOUBLE"),
        ]

    def test_english_column_with_space_value(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols, "show me school type public"
        )
        self.assertTrue(any("`School_Type`" in f for f in frags), (frags, params, meta))
        self.assertTrue(any(m.get("column") == "School_Type" and m.get("value") == "public" for m in meta))

    def test_english_numeric_more(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols, "show me attendance more 80"
        )
        self.assertTrue(any("`Attendance` >" in f for f in frags), (frags, params, meta))
        self.assertTrue(any(m.get("column") == "Attendance" and m.get("op") == ">" for m in meta))

    def test_multi_conditions_ru_en(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols, "show me gender male and school type public"
        )
        self.assertTrue(any(m.get("column") == "Gender" and m.get("value") == "male" for m in meta))
        self.assertTrue(any(m.get("column") == "School_Type" and m.get("value") == "public" for m in meta))


if __name__ == "__main__":
    unittest.main()

