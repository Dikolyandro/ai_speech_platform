import unittest

from app.services.i18n_service import validate_query_language
from app.api.v1.routes_answer import (
    _append_nonempty_group_where_clause,
    _canonicalize_localized_suggestion_query,
    _detect_control_intent,
    _is_canonical_suggestion_query,
    _localized_control_response,
    _query_relevant_to_dataset,
    _resolve_canonical_suggestion_query,
    _strip_dataset_columns_for_language_validation,
)
from app.services.dataset_suggestion_service import build_dataset_suggestions


class TestI18nLanguagePolicy(unittest.TestCase):
    def test_en_accepts_english(self):
        ok, reason = validate_query_language("show me top 5 by attendance", "en")
        self.assertTrue(ok, reason)

    def test_en_rejects_cyrillic(self):
        ok, reason = validate_query_language("покажи топ 5", "en")
        self.assertFalse(ok)
        self.assertEqual(reason, "need_en")

    def test_ru_accepts_russian(self):
        ok, reason = validate_query_language("покажи максимум по посещаемости", "ru")
        self.assertTrue(ok, reason)

    def test_ru_rejects_english(self):
        ok, reason = validate_query_language("show me max attendance", "ru")
        self.assertFalse(ok)
        self.assertEqual(reason, "need_ru")

    def test_ru_accepts_russian_with_latin_column_name(self):
        ok, reason = validate_query_language("Показать среднее значение по release_year", "ru")
        self.assertTrue(ok, reason)

    def test_kk_accepts_kazakh_markers(self):
        ok, reason = validate_query_language("қатысу бойынша ең жоғары мәнді көрсет", "kk")
        self.assertTrue(ok, reason)

    def test_kk_rejects_russian_text(self):
        ok, reason = validate_query_language("покажи максимальное значение", "kk")
        self.assertFalse(ok)
        self.assertEqual(reason, "need_kk")

    def test_kk_accepts_kazakh_with_latin_column_name(self):
        ok, reason = validate_query_language("release_year бойынша орташа мәнді көрсету", "kk")
        self.assertTrue(ok, reason)

    def test_general_knowledge_is_controlled(self):
        self.assertEqual(_detect_control_intent("Who is the president of France?"), "general")
        self.assertIn("dataset-driven analytics", _localized_control_response("general", "en"))

    def test_unrelated_low_grounding_is_not_dataset_relevant(self):
        cols = [("amount", "INTEGER"), ("category", "TEXT")]
        semantic_meta = {
            "column_bindings": {
                "metric": {"column": "amount", "method": "first_numeric", "score": 0.55}
            }
        }
        self.assertFalse(_query_relevant_to_dataset("who wrote war and peace", cols, semantic_meta, None))

    def test_column_mention_is_dataset_relevant(self):
        cols = [("amount", "INTEGER"), ("category", "TEXT")]
        semantic_meta = {"column_bindings": {}}
        self.assertTrue(_query_relevant_to_dataset("average amount by category", cols, semantic_meta, None))

    def test_dataset_suggestions_are_structured_not_display_text(self):
        columns = [
            {"name": "release_year", "type": "INTEGER", "profile": {"distinct": 20}},
            {"name": "director", "type": "TEXT", "profile": {"distinct": 50}},
        ]
        suggestions = build_dataset_suggestions(dataset_id=1, columns=columns, language="ru", max_suggestions=4)

        self.assertTrue(any(item["intent"] == "average" and item["column"] == "release_year" for item in suggestions))
        count_by_director = next(
            item for item in suggestions if item["intent"] == "count_by" and item["column"] == "director"
        )
        self.assertEqual(count_by_director["displayText"]["kk"], "director бойынша жазбалар санын көрсету")
        self.assertEqual(count_by_director["canonicalQuery"], "Count records by director")
        self.assertTrue(all("title" not in item for item in suggestions))
        self.assertTrue(all("query" not in item for item in suggestions))
        self.assertTrue(all("explanation" not in item for item in suggestions))

    def test_canonical_suggestion_queries_are_internal_templates(self):
        self.assertTrue(_is_canonical_suggestion_query("count records by type"))
        self.assertTrue(_is_canonical_suggestion_query("show average release_year"))
        self.assertFalse(_is_canonical_suggestion_query("who is the president of france"))

    def test_grouped_queries_exclude_blank_group_keys(self):
        where_sql = _append_nonempty_group_where_clause("", "Airport_fee")
        self.assertIn("`Airport_fee` IS NOT NULL", where_sql)
        self.assertIn("TRIM(CAST(`Airport_fee`", where_sql)

        existing = _append_nonempty_group_where_clause(" WHERE `VendorID` = :vendor ", "Airport_fee")
        self.assertIn("WHERE `VendorID` = :vendor", existing)
        self.assertIn("AND `Airport_fee` IS NOT NULL", existing)

    def test_canonical_suggestion_queries_resolve_intent_and_column(self):
        cols = [("release_year", "INTEGER"), ("type", "TEXT"), ("description", "TEXT")]
        cases = {
            "Show average release_year": ("average", "release_year"),
            "Count records by type": ("count_by", "type"),
            "Show trend by release_year": ("trend_by", "release_year"),
            "Show sample values from description": ("sample", "description"),
            "Show top values by type": ("top_values", "type"),
            "Show minimum release_year": ("min", "release_year"),
            "Show maximum release_year": ("max", "release_year"),
            "Show sum of release_year": ("sum", "release_year"),
        }
        for query, expected in cases.items():
            with self.subTest(query=query):
                resolved = _resolve_canonical_suggestion_query(query, cols)
                self.assertIsNotNone(resolved)
                self.assertEqual((resolved["intent"], resolved["column"]), expected)

        self.assertEqual(
            _resolve_canonical_suggestion_query("Show first rows", cols),
            {"intent": "first_rows", "column": None},
        )

    def test_language_validation_ignores_latin_dataset_columns(self):
        cols = [("release_year", "INTEGER"), ("type", "TEXT")]
        stripped = _strip_dataset_columns_for_language_validation("release_year бойынша орташа мәнді көрсету", cols)
        ok, reason = validate_query_language(stripped, "kk")
        self.assertTrue(ok, reason)

    def test_localized_suggestion_queries_canonicalize_for_parser(self):
        cols = [("release_year", "INTEGER"), ("type", "TEXT")]
        self.assertEqual(
            _canonicalize_localized_suggestion_query("release_year бойынша орташа мәнді көрсету", cols),
            "show average release_year",
        )
        self.assertEqual(
            _canonicalize_localized_suggestion_query("Показать среднее значение по release_year", cols),
            "show average release_year",
        )
        self.assertEqual(
            _canonicalize_localized_suggestion_query("type бойынша жазбалар санын көрсету", cols),
            "count records by type",
        )

    def test_localized_suggestion_template_can_cross_account_language(self):
        cols = [("release_year", "INTEGER")]
        localized = "Показать среднее значение по release_year"
        self.assertFalse(validate_query_language(localized, "kk")[0])
        canonicalized = _canonicalize_localized_suggestion_query(localized, cols)
        self.assertNotEqual(canonicalized, localized.lower())
        self.assertEqual(canonicalized, "show average release_year")


if __name__ == "__main__":
    unittest.main()

