import unittest

from app.services.profile_semantic_resolver import (
    resolve_profile_value_filters,
    compile_profile_filters_to_sql,
)


class TestProfileSemanticResolver(unittest.TestCase):
    def test_dataset_a_gender_income(self):
        columns_json = [
            {
                "name": "Gender",
                "type": "TEXT",
                "semantic": {
                    "categorical_values_norm": ["male", "female"],
                },
            },
            {
                "name": "Family_Income",
                "type": "TEXT",
                "semantic": {
                    "categorical_values_norm": ["low", "medium", "high"],
                },
            },
            {"name": "Exam_Score", "type": "DOUBLE", "semantic": {}},
        ]
        q = "show male with low family income"
        filters, dbg = resolve_profile_value_filters(query_text=q, columns_json=columns_json)
        self.assertTrue(any(f.column == "Gender" and f.value == "male" for f in filters), dbg)
        self.assertTrue(any(f.column == "Family_Income" and f.value == "low" for f in filters), dbg)

        frags, params, meta, used = compile_profile_filters_to_sql(filters)
        self.assertTrue(any("`Gender`" in s for s in frags))
        self.assertTrue(any("`Family_Income`" in s for s in frags))
        self.assertTrue("Gender" in used and "Family_Income" in used)
        self.assertTrue(all(k.startswith("pf") for k in params.keys()))

    def test_dataset_b_barcelona(self):
        columns_json = [
            {
                "name": "club",
                "type": "TEXT",
                "semantic": {"categorical_values_norm": ["barcelona", "realmadrid", "arsenal"]},
            },
            {"name": "player_name", "type": "TEXT", "semantic": {}},
            {"name": "market_value_eur", "type": "DOUBLE", "semantic": {}},
        ]
        q = "show players from Barcelona"
        filters, dbg = resolve_profile_value_filters(query_text=q, columns_json=columns_json)
        self.assertTrue(any(f.column == "club" and f.value == "barcelona" for f in filters), dbg)

    def test_dataset_c_city_amount(self):
        columns_json = [
            {
                "name": "customer_city",
                "type": "TEXT",
                "semantic": {"categorical_values_norm": ["almaty", "astana", "shymkent"]},
            },
            {"name": "total_amount", "type": "DOUBLE", "semantic": {}},
            {"name": "order_date", "type": "TEXT", "semantic": {}},
        ]
        q = "show orders from Almaty above 10000"
        filters, dbg = resolve_profile_value_filters(query_text=q, columns_json=columns_json)
        self.assertTrue(any(f.column == "customer_city" and f.value == "almaty" for f in filters), dbg)

    def test_dataset_d_category_electronics(self):
        columns_json = [
            {
                "name": "category_name",
                "type": "TEXT",
                "semantic": {"categorical_values_norm": ["electronics", "books", "toys"]},
            },
            {"name": "rating", "type": "DOUBLE", "semantic": {}},
            {"name": "product_title", "type": "TEXT", "semantic": {}},
        ]
        q = "show electronics with rating above 4"
        filters, dbg = resolve_profile_value_filters(query_text=q, columns_json=columns_json)
        self.assertTrue(any(f.column == "category_name" and f.value == "electronics" for f in filters), dbg)

    def test_ambiguity_caps_confidence(self):
        columns_json = [
            {"name": "home_city", "type": "TEXT", "semantic": {"categorical_values_norm": ["almaty"]}},
            {"name": "work_city", "type": "TEXT", "semantic": {"categorical_values_norm": ["almaty"]}},
        ]
        q = "almaty"
        filters, dbg = resolve_profile_value_filters(query_text=q, columns_json=columns_json)
        self.assertEqual(len(filters), 1, dbg)
        self.assertLess(filters[0].score, 0.9, dbg)


if __name__ == "__main__":
    unittest.main()

