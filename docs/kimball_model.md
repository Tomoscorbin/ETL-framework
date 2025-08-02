# Kimball Data Model

The gold layer follows a simple star schema optimised for dashboard consumption.

## Dimensions

### `dim_product`
Attributes describing each product, including the aisle and department names.

## Facts

### `fact_order_item`
Grain is one row per product in an order. Contains ordering context like user, order timing and reorder indicator.
