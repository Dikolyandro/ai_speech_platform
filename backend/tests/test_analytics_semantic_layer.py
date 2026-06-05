"""Unit tests for ``app.services.analytics_semantic_layer``."""

import unittest

from app.services.analytics_semantic_layer import (
    AnalyticsSemanticResult,
    analyze_query_semantics,
    build_column_semantic_info,
    extract_query_keywords,
    infer_column_business_type,
    normalize_text,
    score_column_match,
)


class TestAnalyticsSemanticLayer(unittest.TestCase):
    def test_show_top_revenue_by_category(self) -> None:
        columns = [
            ("payment_value", "DOUBLE"),
            ("category", "TEXT"),
        ]
        r = analyze_query_semantics(
            "show top revenue by category",
            columns,
            user_lang="en",
        )
        self.assertIsInstance(r, AnalyticsSemanticResult)
        self.assertIn("revenue", r.normalized_query)
        self.assertIsNotNone(r.detected_metric, r.debug)
        self.assertEqual(r.detected_metric.column, "payment_value")
        self.assertGreater(r.detected_metric.score, 0.4)
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "category")

    def test_russian_sales_by_categories(self) -> None:
        columns = [
            ("amount", "DECIMAL"),
            ("category", "TEXT"),
        ]
        r = analyze_query_semantics(
            "покажи продажи по категориям",
            columns,
            user_lang="ru",
        )
        self.assertIsNotNone(r.detected_metric)
        self.assertEqual(r.detected_metric.column, "amount")
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "category")

    def test_orders_by_month_detects_date_column(self) -> None:
        columns = [
            ("order_id", "INTEGER"),
            ("order_date", "DATE"),
            ("total", "DOUBLE"),
        ]
        r = analyze_query_semantics(
            "show orders by month",
            columns,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_date_column, r.debug)
        self.assertEqual(r.detected_date_column.column, "order_date")
        self.assertGreater(r.detected_date_column.score, 0.35)

    def test_unknown_query_low_confidence_no_crash(self) -> None:
        columns = [
            ("x", "TEXT"),
            ("y", "INTEGER"),
        ]
        r = analyze_query_semantics(
            "asdf qwerty zxcvbnm plokij",
            columns,
            user_lang="en",
        )
        self.assertIsNotNone(r.normalized_query)
        self.assertLessEqual(r.confidence, 0.5)

    def test_normalize_and_keywords(self) -> None:
        self.assertEqual(normalize_text("  Foo   BAR!!!  "), "foo bar")
        kw = extract_query_keywords("show me revenue totals", "en")
        self.assertIn("revenue", kw)
        self.assertIn("totals", kw)

    def test_infer_metric_and_dimension(self) -> None:
        self.assertEqual(infer_column_business_type("payment_value", "DOUBLE"), "metric")
        self.assertEqual(infer_column_business_type("category", "TEXT"), "dimension")
        self.assertEqual(infer_column_business_type("order_date", "DATE"), "date")

    def test_score_column_match(self) -> None:
        col = build_column_semantic_info("payment_value", "DOUBLE")
        s = score_column_match("top revenue by region", col)
        self.assertGreater(s, 0.4)

    def test_infer_netflix_style_columns(self) -> None:
        self.assertEqual(infer_column_business_type("listed_in", "TEXT"), "dimension")
        self.assertEqual(infer_column_business_type("director", "TEXT"), "dimension")
        self.assertEqual(infer_column_business_type("cast", "TEXT"), "dimension")
        self.assertEqual(infer_column_business_type("duration", "TEXT"), "metric")
        self.assertEqual(infer_column_business_type("release_year", "INTEGER"), "date")
        self.assertEqual(infer_column_business_type("title", "TEXT"), "text")
        self.assertEqual(infer_column_business_type("type", "TEXT"), "dimension")


class TestEntertainmentNetflixSemantics(unittest.TestCase):
    """Netflix Movies & TV Shows–style schema (column names from common Kaggle exports)."""

    NETFLIX_COLUMNS: list[tuple[str, str]] = [
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

    def test_top_genres_on_netflix(self) -> None:
        r = analyze_query_semantics(
            "top genres on netflix",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "listed_in")
        self.assertGreater(r.detected_dimension.score, 0.45)

    def test_compare_movies_and_tv_shows(self) -> None:
        r = analyze_query_semantics(
            "compare movies and tv shows",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "type")
        self.assertGreater(r.detected_dimension.score, 0.4)

    def test_trend_releases_over_time(self) -> None:
        r = analyze_query_semantics(
            "trend of releases over time",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_date_column)
        self.assertEqual(r.detected_date_column.column, "release_year")
        self.assertGreater(r.detected_date_column.score, 0.35)

    def test_countries_most_content(self) -> None:
        r = analyze_query_semantics(
            "which countries produce most content",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "country")
        self.assertGreater(r.detected_dimension.score, 0.4)

    def test_longest_movies(self) -> None:
        r = analyze_query_semantics(
            "longest movies on netflix",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_metric)
        self.assertEqual(r.detected_metric.column, "duration")
        self.assertGreater(r.detected_metric.score, 0.45)

    def test_most_common_directors(self) -> None:
        r = analyze_query_semantics(
            "most common directors",
            self.NETFLIX_COLUMNS,
            user_lang="en",
        )
        self.assertIsNotNone(r.detected_dimension)
        self.assertEqual(r.detected_dimension.column, "director")
        self.assertGreater(r.detected_dimension.score, 0.45)

    def test_russian_genre_and_duration(self) -> None:
        r1 = analyze_query_semantics(
            "топ жанров на netflix",
            self.NETFLIX_COLUMNS,
            user_lang="ru",
        )
        self.assertEqual(r1.detected_dimension.column, "listed_in")

        r2 = analyze_query_semantics(
            "самые длинные фильмы",
            self.NETFLIX_COLUMNS,
            user_lang="ru",
        )
        self.assertEqual(r2.detected_metric.column, "duration")

    def test_kazakh_country_and_release(self) -> None:
        r1 = analyze_query_semantics(
            "қай ел ең көп контент шығарады",
            self.NETFLIX_COLUMNS,
            user_lang="kk",
        )
        self.assertEqual(r1.detected_dimension.column, "country")

        r2 = analyze_query_semantics(
            "шыққан жылы бойынша тренд",
            self.NETFLIX_COLUMNS,
            user_lang="kk",
        )
        self.assertIsNotNone(r2.detected_date_column)
        self.assertEqual(r2.detected_date_column.column, "release_year")


if __name__ == "__main__":
    unittest.main()
