"""
Netflix-style domain patterns for ``/api/v1/query/answer``.

Manual checks (run backend + import CSV with Netflix columns), example queries:

- top genres on netflix
- most common genres
- which genres dominate netflix
- compare movies and tv shows
- trend of releases over time
- which countries produce most content
- longest movies on netflix
"""

import unittest
from unittest.mock import patch

from app.api.v1.routes_answer import (
    _detect_netflix_query_pattern,
    _netflix_duration_agg_expr_sql,
    _netflix_top_genres_listed_in_split_sql,
)
from app.services.analytics_semantic_layer import analyze_query_semantics


NETFLIX_COLS: list[tuple[str, str]] = [
    ("show_id", "TEXT"),
    ("type", "TEXT"),
    ("title", "TEXT"),
    ("director", "TEXT"),
    ("cast", "TEXT"),
    ("country", "TEXT"),
    ("date_added", "DATE"),
    ("release_year", "INTEGER"),
    ("rating", "TEXT"),
    ("duration", "TEXT"),
    ("listed_in", "TEXT"),
    ("description", "TEXT"),
]


class TestDetectNetflixQueryPattern(unittest.TestCase):
    def _col_set(self) -> set[str]:
        return {c[0] for c in NETFLIX_COLS}

    def test_top_genres_requires_semantic_listed_in(self) -> None:
        col_set = self._col_set()
        sl = analyze_query_semantics("top genres on netflix", NETFLIX_COLS, user_lang="en")
        r = _detect_netflix_query_pattern("top genres on netflix", col_set, sl)
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "top_genres")
        self.assertEqual(r["operation"], "top")
        self.assertIsNone(r["metric_col"])
        self.assertEqual(r["group_col"], "listed_in")

    def test_compare_movies_tv(self) -> None:
        col_set = self._col_set()
        r = _detect_netflix_query_pattern("compare movies and tv shows", col_set, None)
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "compare_type")
        self.assertEqual(r["operation"], "count")
        self.assertEqual(r["group_col"], "type")

    def test_release_trend(self) -> None:
        col_set = self._col_set()
        r = _detect_netflix_query_pattern("trend of releases over time", col_set, None)
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "release_trend")
        self.assertEqual(r["group_col"], "release_year")
        self.assertEqual(r["order_by"], "release_year_asc")

    def test_top_countries(self) -> None:
        col_set = self._col_set()
        r = _detect_netflix_query_pattern(
            "which countries produce most content",
            col_set,
            None,
        )
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "top_countries")
        self.assertEqual(r["group_col"], "country")

    def test_longest_movies(self) -> None:
        col_set = self._col_set()
        r = _detect_netflix_query_pattern("longest movies on netflix", col_set, None)
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "longest_movies")
        self.assertEqual(r["metric_col"], "duration")
        self.assertEqual(r["group_col"], "title")

    def test_no_match_without_columns(self) -> None:
        col_set = {"amount", "category"}
        sl = analyze_query_semantics("top genres on netflix", NETFLIX_COLS, user_lang="en")
        r = _detect_netflix_query_pattern("top genres on netflix", col_set, sl)
        self.assertFalse(r["matched"])

    def test_duration_sql_expr_non_empty(self) -> None:
        s = _netflix_duration_agg_expr_sql("duration")
        self.assertIn("duration", s)
        self.assertIn("MAX", s)


    def test_which_genres_dominate(self) -> None:
        col_set = self._col_set()
        sl = analyze_query_semantics("which genres dominate netflix", NETFLIX_COLS, user_lang="en")
        r = _detect_netflix_query_pattern("which genres dominate netflix", col_set, sl)
        self.assertTrue(r["matched"])
        self.assertEqual(r["pattern"], "top_genres")

    def test_most_common_genres(self) -> None:
        col_set = self._col_set()
        sl = analyze_query_semantics("most common genres", NETFLIX_COLS, user_lang="en")
        r = _detect_netflix_query_pattern("most common genres", col_set, sl)
        self.assertTrue(r["matched"])

    def test_genre_split_sqlite_contains_recursive_cte(self) -> None:
        col_set = self._col_set()
        sql, strat, gs = _netflix_top_genres_listed_in_split_sql("ds_1_data", "", 25, col_set)
        self.assertIn("WITH RECURSIVE", sql)
        self.assertIn("split_genres", sql)
        self.assertIn("listed_in", sql)
        self.assertEqual(strat, "recursive_cte")
        self.assertTrue(gs)

    def test_genre_split_mysql_old_fallback(self) -> None:
        from app.db.session import engine

        col_set = self._col_set()
        with patch.object(engine.dialect, "name", "mysql"), patch.object(
            engine.dialect, "server_version_info", (5, 7, 30)
        ):
            sql, strat, gs = _netflix_top_genres_listed_in_split_sql("ds_1_data", "", 10, col_set)
        self.assertEqual(strat, "fallback_grouped_string")
        self.assertFalse(gs)
        self.assertIn("GROUP BY `listed_in`", sql)
        self.assertNotIn("WITH RECURSIVE", sql)


if __name__ == "__main__":
    unittest.main()
