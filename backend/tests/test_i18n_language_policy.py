import unittest

from app.services.i18n_service import validate_query_language


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

    def test_kk_accepts_kazakh_markers(self):
        ok, reason = validate_query_language("қатысу бойынша ең жоғары мәнді көрсет", "kk")
        self.assertTrue(ok, reason)

    def test_kk_rejects_russian_text(self):
        ok, reason = validate_query_language("покажи максимальное значение", "kk")
        self.assertFalse(ok)
        self.assertEqual(reason, "need_kk")


if __name__ == "__main__":
    unittest.main()

