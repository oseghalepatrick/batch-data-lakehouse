# from pyiceberg.catalog import load_catalog
# from pyiceberg.types import DateType

# catalog = load_catalog(
#     "nessie",
#     **{
#         "type": "rest",
#         "uri": "http://nessie:19120/iceberg/main/",
#         "warehouse": "warehouse",
#         "s3.endpoint": "http://minio:9000",
#         "s3.access-key-id": "admin",
#         "s3.secret-access-key": "password123",
#         "s3.region": "us-east-1",
#     },
# )


# print(catalog.list_namespaces())

# try:
#     catalog.create_namespace("demo")
# except Exception as ex:
#     print(f"{type(ex).__name__}")

# from pyiceberg.schema import Schema
# from pyiceberg.types import IntegerType, NestedField, StringType

# # Define the schema for the table
# schema = Schema(
#     NestedField(1, "id", IntegerType(), required=True),
#     NestedField(2, "name", StringType(), required=False),
# )

# catalog.drop_table("silver.data_quality_results")
# catalog.drop_table("silver.customers")
# catalog.drop_table("silver.stores")
# catalog.drop_table("silver.sales")
# catalog.drop_table("demo.sample_table")
# # Create the table in the `demo` namespace
# try:
#     catalog.create_table("demo.sample_table", schema)
# except Exception as ex:
#     print(f"Error: with {type(ex).__name__}")

# import pyarrow as pa

# # Create an Arrow table with sample data
# data = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

# data = data.cast(
#     pa.schema(
#         [
#             pa.field("id", pa.int32(), nullable=False),
#             pa.field("name", pa.string(), nullable=True),
#         ]
#     )
# )
# print(data.schema)
# # Append the data to the Iceberg table
# table = catalog.load_table("silver.products")
# # table.append(data)

# # result = table.scan().to_arrow()
# # print(result)

# # # Get the table schema
# print(f"Table Schema Before: \n{table.schema()}")

# with table.update_schema() as update:
#     update.delete_column("effective_end")
#     update.rename_column("effective_end_date", "effective_end")


# table = catalog.load_table("silver.products")

# print(f"Table Schema After: \n{table.schema()}")
# # # Get the table properties
# # properties = table.properties
# # print("Table Properties:", properties)

# # Inspect table partitions
# partitions = table.spec()
# print("Table Partitions:", partitions)


import random

import pycountry
from faker import Faker


import random

import pycountry
from faker import Faker


class LocationGenerator:

    def __init__(self):
        self.fake = Faker()

        self.regions = {
            "Africa": [
                "NG",
                "GH",
                "KE",
                "ZA",
                "TZ",
                "EG",
            ],
            "Europe": [
                "GB",
                "DE",
                "FR",
                "IT",
                "ES",
                "NL",
            ],
            "North America": [
                "US",
                "MX",
            ],
            "Canada": [
                "CA",
            ],
            "Asia": [
                "IN",
                "JP",
                "SG",
                "MY",
                "TH",
            ],
            "Australia": [
                "AU",
                "NZ",
            ],
        }

    def generate(self):

        region = random.choice(
            list(self.regions.keys())
        )

        country_code = random.choice(
            self.regions[region]
        )

        location = self.fake.local_latlng(
            country_code=country_code
        )

        # Some countries may not have a location
        # in Faker's land_coords dataset.
        if location is None:
            return self.generate()

        latitude, longitude, city, returned_country_code, timezone = (
            location
        )

        country = pycountry.countries.get(
            alpha_2=returned_country_code
        )

        return {
            "region": region,
            "city": city,
            "country_code": returned_country_code,
            "country": (
                country.name
                if country
                else returned_country_code
            ),
            "latitude": float(latitude),
            "longitude": float(longitude),
            "timezone": timezone,
        }


if __name__ == "__main__":

    generator = LocationGenerator()

    for _ in range(10):
        print(generator.generate())