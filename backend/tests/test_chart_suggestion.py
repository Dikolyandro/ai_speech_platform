"""Unit tests for ``_build_chart_suggestion`` (query/answer chart hints)."""

import unittest

from app.api.v1.routes_answer import _build_chart_suggestion


class TestBuildChartSuggestion(unittest.TestCase):
    def test_empty_rows_disabled(self) -> None:
        r = _build_chart_suggestion(
            "top",
            "listed_in",
            None,
            [],
            {"domain_pattern": {"matched": True, "pattern": "top_genres"}},
            20,
        )
        self.assertFalse(r["enabled"])
        self.assertIsNone(r["chart_type"])

    def test_release_trend_line(self) -> None:
        rows = [{"group_key": 2015, "count": 10}, {"group_key": 2016, "count": 20}]
        sm = {"domain_pattern": {"matched": True, "pattern": "release_trend"}}
        r = _build_chart_suggestion("count", "release_year", None, rows, sm, 50)
        self.assertTrue(r["enabled"])
        self.assertEqual(r["chart_type"], "line")
        self.assertEqual(r["x"], "group_key")
        self.assertEqual(r["y"], "count")
        self.assertEqual(r["reason"], "domain_release_trend")

    def test_compare_type_pie(self) -> None:
        rows = [{"group_key": "Movie", "count": 100}, {"group_key": "TV Show", "count": 200}]
        sm = {"domain_pattern": {"matched": True, "pattern": "compare_type"}}
        r = _build_chart_suggestion("count", "type", None, rows, sm, 10)
        self.assertEqual(r["chart_type"], "pie")
        self.assertEqual(r["reason"], "domain_compare_type")

    def test_top_genres_bar_prefers_listed_in(self) -> None:
        rows = [{"listed_in": "Dramas", "count": 5, "group_key": "Dramas"}]
        sm = {"domain_pattern": {"matched": True, "pattern": "top_genres"}}
        r = _build_chart_suggestion("top", "listed_in", None, rows, sm, 25)
        self.assertEqual(r["chart_type"], "bar")
        self.assertEqual(r["x"], "listed_in")
        self.assertEqual(r["y"], "count")

    def test_top_countries_bar(self) -> None:
        rows = [{"group_key": "US", "count": 10}]
        sm = {"domain_pattern": {"matched": True, "pattern": "top_countries"}}
        r = _build_chart_suggestion("top", "country", None, rows, sm, 20)
        self.assertEqual(r["chart_type"], "bar")
        self.assertEqual(r["reason"], "domain_top_countries")

    def test_longest_movies_bar(self) -> None:
        rows = [{"group_key": "Inception", "sum": 148}]
        sm = {"domain_pattern": {"matched": True, "pattern": "longest_movies"}}
        r = _build_chart_suggestion("top", "title", "duration", rows, sm, 15)
        self.assertEqual(r["chart_type"], "bar")
        self.assertEqual(r["y"], "sum")

    def test_no_group_column_table(self) -> None:
        rows = [{"a": 1, "b": 2}]
        r = _build_chart_suggestion("select", None, None, rows, {}, 10)
        self.assertEqual(r["chart_type"], "table")
        self.assertTrue(r["enabled"])

    def test_generic_grouped_bar(self) -> None:
        rows = [{"group_key": "East", "sum": 12.5}]
        r = _build_chart_suggestion("sum", "region", "amount", rows, {"domain_pattern": {}}, 10)
        self.assertEqual(r["chart_type"], "bar")
        self.assertEqual(r["reason"], "generic_grouped_aggregate")

    def test_malformed_row_fallback_table(self) -> None:
        r = _build_chart_suggestion("top", "category", None, ["not-a-dict"], {}, 10)  # type: ignore[arg-type]
        self.assertEqual(r["chart_type"], "table")
        self.assertTrue(r["enabled"])


if __name__ == "__main__":
    unittest.main()
