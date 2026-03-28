import unittest

from app.api.v1.routes_answer import (
    _find_current_and_previous_score_columns,
    _resolve_top_n_or_extreme_row_ranking,
    _wants_largest_score_delta_from_previous,
)


class TestRowRankingResolve(unittest.TestCase):
    def setUp(self) -> None:
        self.cols = [
            ("Attendance", "DOUBLE"),
            ("Exam_Score", "DOUBLE"),
            ("Gender", "TEXT"),
        ]
        self.col_set = {c[0] for c in self.cols}

    def test_lowest_attendance_uses_limit_cap(self):
        r = _resolve_top_n_or_extreme_row_ranking(
            "show me the lowest attendance for students",
            "select",
            None,
            None,
            self.cols,
            self.col_set,
            20,
        )
        self.assertEqual(r, ("single", "Attendance", "ASC", 20), r)

    def test_highest_exam_score(self):
        r = _resolve_top_n_or_extreme_row_ranking(
            "highest exam score",
            "select",
            None,
            None,
            self.cols,
            self.col_set,
            15,
        )
        self.assertEqual(r, ("single", "Exam_Score", "DESC", 15), r)

    def test_dual_lowest_and_highest(self):
        r = _resolve_top_n_or_extreme_row_ranking(
            "lowest attendance and highest attendance",
            "select",
            None,
            None,
            self.cols,
            self.col_set,
            20,
        )
        self.assertIsNotNone(r)
        assert r is not None
        self.assertEqual(r[0], "dual")
        self.assertEqual(r[1], "Attendance")
        self.assertEqual(r[2], 10)

    def test_biggest_exam_gain_from_previous_phrase(self):
        q = "show me the biggest development in exam score from previous score"
        self.assertTrue(_wants_largest_score_delta_from_previous(q))
        cols = ["Hours_Studied", "Exam_Score", "Previous_Scores", "Gender"]
        num = {"Hours_Studied", "Exam_Score", "Previous_Scores"}
        p = _find_current_and_previous_score_columns(cols, num)
        self.assertEqual(p, ("Exam_Score", "Previous_Scores"))


if __name__ == "__main__":
    unittest.main()
