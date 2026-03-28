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

    def test_english_exam_grade_more_than_70(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "female students college distance moderate exam grade more than 70",
        )
        self.assertTrue(
            any(m.get("column") == "Exam_Score" and m.get("op") == ">" and m.get("value") == 70 for m in meta),
            meta,
        )

    def test_english_exam_grade_range_but(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "exam grade more than 70 but less than 75",
        )
        gt = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == ">"]
        lt = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == "<"]
        self.assertTrue(any(m.get("value") == 70 for m in gt), meta)
        self.assertTrue(any(m.get("value") == 75 for m in lt), meta)

    def test_ru_score_range_no_no_column_name(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "студенты оценка экзамена больше 70 но меньше 75",
        )
        gt = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == ">"]
        lt = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == "<"]
        self.assertTrue(any(m.get("value") == 70 for m in gt), meta)
        self.assertTrue(any(m.get("value") == 75 for m in lt), meta)

    def test_english_exam_score_more_than_70(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "show me high school students that are male and the exam score more than 70",
        )
        self.assertTrue(
            any("`Exam_Score` >" in f or "CAST(`Exam_Score` AS REAL) >" in f for f in frags),
            (frags, params, meta),
        )
        self.assertTrue(
            any(m.get("column") == "Exam_Score" and m.get("op") == ">" and m.get("value") == 70 for m in meta),
            meta,
        )

    def test_english_exam_score_range_more_and_less(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "female students from college with exam score more than 75 and less than 77",
        )
        self.assertTrue(
            any("`Exam_Score` >" in f or "CAST(`Exam_Score` AS REAL) >" in f for f in frags),
            frags,
        )
        self.assertTrue(
            any("`Exam_Score` <" in f or "CAST(`Exam_Score` AS REAL) <" in f for f in frags),
            frags,
        )
        lows = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == ">"]
        highs = [m for m in meta if m.get("column") == "Exam_Score" and m.get("op") == "<"]
        self.assertTrue(any(m.get("value") == 75 for m in lows), meta)
        self.assertTrue(any(m.get("value") == 77 for m in highs), meta)

    def test_multi_conditions_ru_en(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols, "show me gender male and school type public"
        )
        self.assertTrue(any(m.get("column") == "Gender" and m.get("value") == "male" for m in meta))
        self.assertTrue(any(m.get("column") == "School_Type" and m.get("value") == "public" for m in meta))

    def test_ru_exam_score_phrase_with_noun_between(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "покажи студентов-мужчин, колледж, оценка экзамена больше 70.",
        )
        self.assertTrue(
            any(m.get("column") == "Exam_Score" and m.get("op") == ">" and m.get("value") == 70 for m in meta),
            meta,
        )

    def test_ru_gender_and_score(self):
        frags, params, meta, _ = build_dynamic_filters_and_order(
            self.cols,
            "покажи парней с оценкой больше 70",
        )
        self.assertTrue(any(m.get("column") == "Gender" and m.get("value") == "male" for m in meta), meta)
        self.assertTrue(
            any(m.get("column") == "Exam_Score" and m.get("op") == ">" and m.get("value") == 70 for m in meta),
            meta,
        )

    def test_ru_parental_high_school_distance_or(self):
        cols = [
            ("Gender", "TEXT"),
            ("Parental_Education_Level", "TEXT"),
            ("Distance_from_Home", "TEXT"),
            ("Exam_Score", "DOUBLE"),
        ]
        frags, params, meta, _ = build_dynamic_filters_and_order(
            cols,
            "студенты старшей школы расстояние близкое или среднее",
        )
        self.assertTrue(
            any(m.get("column") == "Parental_Education_Level" and m.get("value") == "High School" for m in meta),
            meta,
        )
        or_meta = [m for m in meta if m.get("reason") == "pattern_ru_distance"]
        self.assertTrue(or_meta and or_meta[0].get("value") == ["Near", "Moderate"], meta)


if __name__ == "__main__":
    unittest.main()

