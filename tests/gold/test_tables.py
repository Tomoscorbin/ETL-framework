from src.gold.dim_product import product_dimension
from src.gold.fact_order_item import order_item_fact


def test_dim_product_columns() -> None:
    expected = ["product_id", "product_name", "aisle", "department"]
    assert [col.name for col in product_dimension.columns] == expected


def test_fact_order_item_columns() -> None:
    expected = [
        "order_id",
        "product_id",
        "user_id",
        "order_number",
        "order_day_of_week",
        "order_hour",
        "days_since_prior_order",
        "add_to_cart_order",
        "reordered",
    ]
    assert [col.name for col in order_item_fact.columns] == expected
