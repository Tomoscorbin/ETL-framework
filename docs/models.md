# Models

The project defines Python models that describe key objects within the ETL workflow.

## DeltaTable

`DeltaTable` encapsulates the definition and lifecycle operations for a Delta Lake table. It is a frozen dataclass with the following primary attributes:

- `table_name`, `schema_name`, `catalog_name` – identify the table.
- `columns` – list of `DeltaColumn` instances that make up the schema.
- `comment` – human readable description.
- `delta_properties` – additional Delta table properties to set.
- `rules` – collection of data quality `DQRule` objects.

### Derived properties

- `full_name` – convenience property returning `catalog.schema.table`.
- `column_names` – list of column names.
- `schema` – PySpark `StructType` generated from the columns.
- `expected_delta_properties` – default properties merged with any custom properties.
- `primary_key_column_names` – names of columns marked as primary keys.

### Operations

- `ensure(spark)` – create or alter the table so it exists with the expected columns and properties.
- `check_exists(spark)` – return `True` if the table already exists.
- `read(spark)` – load the table as a DataFrame.
- `overwrite(dataframe)` – replace the table's data with the supplied DataFrame.
- `merge(dataframe)` – upsert the DataFrame into the table using primary keys.

