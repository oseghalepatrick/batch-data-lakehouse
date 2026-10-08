import pandas as pd

from pyiceberg.catalog import load_catalog
from pyiceberg.types import DateType


# ============================================================
# 1. Load Nessie Iceberg catalog
# ============================================================

catalog = load_catalog(
    "nessie",
    **{
        "type": "rest",
        "uri": "http://nessie:19120/iceberg/main/",
        "warehouse": "warehouse",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password123",
        "s3.region": "us-east-1",
    },
)


# ============================================================
# 2. Load existing Iceberg table
# ============================================================

table_identifier = "silver.products"

table = catalog.load_table(table_identifier)

print("=" * 80)
print("CURRENT SCHEMA")
print("=" * 80)

print(table.schema())


# ============================================================
# 3. Check existing effective_end column
# ============================================================

schema = table.schema()

effective_end_field = schema.find_field("effective_end")

print("\nCurrent effective_end:")
print(f"Field ID   : {effective_end_field.field_id}")
print(f"Field Type : {effective_end_field.field_type}")
print(f"Required   : {effective_end_field.required}")


# ============================================================
# 4. Add a new DATE column
# ============================================================

new_column = "effective_end_date"

try:
    schema.find_field(new_column)

    print(
        f"\nColumn '{new_column}' already exists. "
        "Skipping schema update."
    )

except ValueError:

    print(
        f"\nAdding '{new_column}' as optional date..."
    )

    with table.update_schema() as update:
        update.add_column(
            new_column,
            DateType(),
            required=False,
        )

    print(
        f"Column '{new_column}' successfully added."
    )


# ============================================================
# 5. Reload table after schema change
# ============================================================

table = catalog.load_table(table_identifier)


# ============================================================
# 6. Display updated schema
# ============================================================

print("\n" + "=" * 80)
print("UPDATED SCHEMA")
print("=" * 80)

print(table.schema())


# ============================================================
# 7. Read existing data
# ============================================================

df = table.scan().to_pandas()

print("\n" + "=" * 80)
print("SOURCE DATA")
print("=" * 80)

print(df.head())

print("\nData types:")
print(df.dtypes)


# ============================================================
# 8. Convert timestamp → Python date
# ============================================================

df[new_column] = (
    pd.to_datetime(
        df["effective_end"],
        errors="coerce",
    )
    .dt.date
)


# ============================================================
# 9. Inspect converted data
# ============================================================

print("\n" + "=" * 80)
print("CONVERTED DATA")
print("=" * 80)

print(
    df[
        [
            "effective_end",
            new_column,
        ]
    ].head(20)
)


# ============================================================
# 10. Inspect Python data types
# ============================================================

print("\nPython types:")

for value in df[new_column].dropna().head(5):
    print(
        value,
        type(value),
    )


# ============================================================
# 11. IMPORTANT
# ============================================================
#
# The code above creates the correct date values in the
# DataFrame.
#
# Existing Iceberg rows are NOT modified automatically by
# update_schema(). Schema evolution only changes the schema.
#
# To physically populate existing records, you need to write
# the converted data back to the table.
#
# ============================================================


print("\n" + "=" * 80)
print("FINAL SCHEMA")
print("=" * 80)

print(table.schema())