from olist.tables import TABLES


def test_every_table_has_csv_and_primary_key():
    for name, meta in TABLES.items():
        assert meta["csv"].endswith(".csv"), name
        assert meta["pk"], name
