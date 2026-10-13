"""Synthetic sales data generator."""

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pycountry
from faker import Faker
from faker_commerce import Provider

from src.utils.checkpoint import CheckpointManager
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.storage import MinIOStorage

logger = get_logger(__name__)


class SalesDataGenerator:
    """Generate synthetic retail sales and dimension data."""

    def __init__(self, **context):
        """Initialise the generator, configuration, state, and storage."""
        self.config = get_config()
        cfg = self.config["data_generation"]
        self.airflow_context = {}
        if context:
            self.airflow_context = context
        if self.airflow_context:
            self.tm = self.airflow_context.get("logical_date").replace(tzinfo=None)
            seed = int(datetime.now(UTC).timestamp())
            logger.info(f"Airflow Running Timestamp: {self.tm}")
        else:
            self.tm = datetime.now(UTC).replace(tzinfo=None)
            seed = int(datetime.now(UTC).timestamp())
            logger.info(f"System Running Timestamp: {self.tm}")

        Faker.seed(seed)
        random.seed(seed)
        self.fake = Faker()
        self.fake.add_provider(Provider)

        # Initial values only Current values are tracked in checkpoint
        self.initial_customers = cfg["initial_dimensions"]["customers"]
        self.initial_products = cfg["initial_dimensions"]["products"]
        self.initial_stores = cfg["initial_dimensions"]["stores"]
        self.daily_sales_records = cfg["sales"]["daily_average_records"]
        self.initial_average_records = cfg["sales"]["initial_average_records"]
        self.output_path = Path(cfg["paths"]["output"])
        self.checkpoint = CheckpointManager(cfg["checkpoint"]["file"])
        self.regions = {
            "Africa": ["NG", "GH", "KE", "ZA", "TZ", "EG"],
            "Europe": ["GB", "DE", "FR", "IT", "ES", "NL"],
            "North America": ["US", "MX", "CA"],
            "Asia": ["IN", "JP", "SG", "MY", "TH"],
            "Australia": ["AU", "NZ"],
        }
        # Avoid repeatedly reading checkpoint
        self.current_counts = {}
        self.storage = MinIOStorage(cfg["minio"])

    def run(self):
        """Run either the initial or incremental data generation process."""
        # Use the checkpoint to decide between the first load and a daily increment.
        if not self.checkpoint.exists():
            self.initial_load()
        else:
            self.incremental_load()

    def initial_load(self):
        """Generate and persist the initial dimensions and sales data."""
        logger.info("Running initial data generation")
        customers = self.generate_customers(self.initial_customers)
        products = self.generate_products(self.initial_products, initial_load=True)
        stores = self.generate_stores(self.initial_stores)
        # Historical initial dimension files
        self.write_dimension_file(customers, "customers")
        self.write_dimension_file(products, "products")
        self.write_dimension_file(stores, "stores")
        # Current state
        self.write_current(customers, "customers")
        self.write_current(products, "products")
        self.write_current(stores, "stores")
        # Initialise checkpoint BEFORE sales
        # Persist dimension counts and ID boundaries for future incremental runs.
        state = {
            "dimensions": {
                "customers": {
                    "initial_count": len(customers),
                    "current_count": len(customers),
                    "last_id": len(customers),
                },
                "products": {
                    "initial_count": len(products),
                    "current_count": len(products),
                    "last_id": len(products),
                },
                "stores": {
                    "initial_count": len(stores),
                    "current_count": len(stores),
                    "last_id": len(stores),
                },
            },
            "last_transaction_id": 0,
        }
        self.checkpoint.save(state)
        self.current_counts = {
            "customers": len(customers),
            "products": len(products),
            "stores": len(stores),
        }
        sales = self.generate_sales(self.initial_average_records, initial_load=True)
        self.write_sales(sales)
        state["last_transaction_id"] = len(sales)
        self.checkpoint.save(state)

    def incremental_load(self):
        """Generate daily dimension changes and incremental sales data."""
        logger.info("Running daily incremental generation")
        # Reload the latest state so generated IDs continue from the correct point.
        state = self.checkpoint.load()
        self.current_counts = {
            "customers": state["dimensions"]["customers"]["current_count"],
            "products": state["dimensions"]["products"]["current_count"],
            "stores": state["dimensions"]["stores"]["current_count"],
        }
        state = self.process_dimension_changes(state)
        sales_count = self.random_sales_volume()
        sales = self.generate_sales(sales_count, state["last_transaction_id"])
        self.write_sales(sales)
        state["last_transaction_id"] += len(sales)
        self.checkpoint.save(state)

    def generate_location(self):
        """Generate a random region, city, and country."""
        region = random.choice(list(self.regions.keys()))
        country_code = random.choice(self.regions[region])
        location = self.fake.local_latlng(country_code=country_code)
        # Some countries may not have a location in Faker's land_coords dataset.
        if location is None:
            return self.generate_location()
        _, _, city, returned_country_code, _ = location
        country = pycountry.countries.get(alpha_2=returned_country_code)
        return {
            "region": region,
            "city": city,
            "country": (country.name if country else returned_country_code),
        }

    def generate_customer_email(self, customer_name, existing_emails):
        """Generate a unique email address for a customer."""
        domains = ["gmail.com", "yahoo.com", "ymail.com", "outlook.com", "hotmail.com"]
        base_name = customer_name.lower().replace(" ", ".")
        while True:
            email = (
                f"{base_name}" f"{random.randint(1,9999)}" f"@{random.choice(domains)}"
            )
            if email not in existing_emails:
                return email

    def generate_customer_phone(self, existing_phones):
        """Generate a unique customer phone number."""
        while True:
            phone = self.fake.phone_number()
            if phone not in existing_phones:
                return phone

    def generate_customers(self, count, start_id=1):
        """Generate synthetic customer records."""
        customers = []
        existing_emails = set()
        existing_phones = set()
        for i in range(start_id, start_id + count):
            customer_name = self.fake.name()
            email = self.generate_customer_email(customer_name, existing_emails)
            phone = self.generate_customer_phone(existing_phones)
            existing_emails.add(email)
            existing_phones.add(phone)
            customer_location = self.generate_location()
            start = self.tm.date() - timedelta(days=90)
            end = self.tm - timedelta(days=30)
            signup_time = self.fake.date_time_between(start_date=start, end_date=end)
            effective_date = signup_time + timedelta(minutes=random.randint(0, 2880))
            customers.append(
                {
                    "customer_id": i,
                    "customer_name": customer_name,
                    "email": email,
                    "phone": phone,
                    "city": customer_location.get("city"),
                    "country": customer_location.get("country"),
                    "region": customer_location.get("region"),
                    "signup_date": signup_time.date(),
                    "segment": random.choice(["Premium", "Regular", "New"]),
                    "effective_start": effective_date.date(),
                }
            )
        return customers

    def generate_products(
        self, count, start_id=1, existing_names=None, initial_load=False
    ):
        """Generate synthetic product records."""
        if existing_names is None:
            existing_names = set()
        products = []
        while len(products) < count:
            product_name = self.fake.ecommerce_name()
            if product_name in existing_names:
                continue
            existing_names.add(product_name)
            if initial_load:
                start = self.tm.date() - timedelta(days=90)
                end = self.tm - timedelta(days=50)
                signup_time = self.fake.date_time_between(
                    start_date=start, end_date=end
                )
                effective_date = signup_time - timedelta(
                    minutes=random.randint(0, 1440)
                )  # Last 2 month
            else:
                effective_date = self.tm - timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59),
                )  # Last 1 day
            products.append(
                {
                    "product_id": start_id + len(products),
                    "product_name": product_name,
                    "category": self.fake.ecommerce_category(),
                    "price": round(random.uniform(10.0, 500.0), 2),
                    "effective_start": effective_date.date(),
                }
            )
        return products

    def generate_stores(self, count, start_id=1):
        """Generate synthetic store records."""
        stores = []
        for i in range(start_id, start_id + count):
            store_location = self.generate_location()
            stores.append(
                {
                    "store_id": i,
                    "store_name": self.fake.company(),
                    "city": store_location.get("city"),
                    "country": store_location.get("country"),
                    "region": store_location.get("region"),
                }
            )
        return stores

    def process_dimension_changes(self, state):
        """Apply configured inserts or updates to a dimension."""
        cfg = self.config["data_generation"]
        # Decide if dimension activity happens
        # Dimension changes only occur when the configured probability is met.
        if random.random() > cfg["dimension_change"]["probability"]:
            logger.info("No dimension changes")
            return state
        dimension = random.choice(["customers", "products", "stores"])
        dimension_cfg = cfg["dimensions"][dimension]
        # Decide operation
        # Randomly choose whether this dimension event is an update or an insert.
        if random.random() < dimension_cfg["update_probability"]:
            action = "UPDATE"
        else:
            action = "INSERT"
        logger.info(f"{action} {dimension}")
        # Only read current state when required
        current = self.read_current(dimension)
        if action == "UPDATE":
            changes = self.update_dimension(current, dimension)
            logger.info(f"{len(changes)} records changed!!!")
        else:
            changes = self.insert_dimension(current, dimension, state)
            logger.info(f"{len(changes)} records inserted!!!")
        if changes:
            # Write changed/new records only
            self.write_dimension_file(changes, dimension)
            # Update complete current state
            self.write_current(current, dimension)
        return state

    def update_dimension(self, df, dimension):
        """Update a sample of records in the selected dimension."""
        dimension_cfg = self.config["data_generation"]["dimensions"][dimension]
        update_percentage = dimension_cfg["update_percentage"]
        update_count = random.randint(1, int(len(df) * update_percentage))
        # Update only a sample of rows to simulate realistic dimension changes.
        selected = df.sample(update_count)
        changes = []
        for index, row in selected.iterrows():
            record = row.to_dict()
            if dimension == "customers":
                customer_id = record["customer_id"]
                # Exclude current customer when checking uniqueness
                # Exclude the current customer before checking email and phone uniqueness.
                other_customers = df[df["customer_id"] != customer_id]
                existing_emails = set(other_customers["email"])
                existing_phones = set(other_customers["phone"])
                update_attr = random.choice(["city", "email", "phone"])
                effective_date = self.tm - timedelta(
                    hours=random.randint(0, 72), minutes=random.randint(0, 59)
                )
                if update_attr == "city":
                    location = self.generate_location()
                    record["city"] = location.get("city")
                    record["country"] = location.get("country")
                    record["region"] = location.get("region")
                elif update_attr == "email":
                    customer_name = record["customer_name"]
                    record["email"] = self.generate_customer_email(
                        customer_name, existing_emails
                    )
                elif update_attr == "phone":
                    record["phone"] = self.generate_customer_phone(existing_phones)
                record["effective_start"] = effective_date.date()
            elif dimension == "products":
                effective_date = self.tm - timedelta(minutes=random.randint(0, 1399))
                record["price"] = round(random.uniform(10.0, 500.0), 2)
                record["effective_start"] = effective_date.date()
            else:
                logger.info("Store update is disabled")
                return []
            changes.append(record)
            # Update current dataframe
            for column, value in record.items():
                df.loc[index, column] = value
        return changes

    def insert_dimension(self, df, dimension, state):
        """Insert new records into the selected dimension."""
        dimension_cfg = self.config["data_generation"]["dimensions"][dimension]
        insert_percentage = dimension_cfg["insert_percentage"]
        insert_count = random.randint(1, int(len(df) * insert_percentage))
        metadata = state["dimensions"][dimension]
        next_id = metadata["last_id"] + 1
        if dimension == "customers":
            existing_emails = set(df["email"])
            existing_phones = set(df["phone"])
            records = []
            for i in range(insert_count):
                customer_name = self.fake.name()
                customer_location = self.generate_location()
                start = self.tm.date() - timedelta(days=30)
                signup_time = self.fake.date_time_between(
                    start_date=start, end_date=self.tm
                )
                effective_date = signup_time + timedelta(
                    hours=random.randint(0, 168),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59),
                )
                records.append(
                    {
                        "customer_id": next_id + i,
                        "customer_name": customer_name,
                        "email": self.generate_customer_email(
                            customer_name, existing_emails
                        ),
                        "phone": self.generate_customer_phone(existing_phones),
                        "city": customer_location.get("city"),
                        "country": customer_location.get("country"),
                        "region": customer_location.get("region"),
                        "signup_date": signup_time.date(),
                        "segment": random.choice(["Premium", "Regular", "New"]),
                        "effective_start": effective_date.date(),
                    }
                )
        elif dimension == "products":
            existing_names = set(df["product_name"])
            records = self.generate_products(insert_count, next_id, existing_names)
        elif dimension == "stores":
            records = self.generate_stores(insert_count, next_id)
        else:
            return []
        # Add new records to current state
        # Merge new records into the in-memory current-state dataframe.
        updated = pd.concat([df, pd.DataFrame(records)], ignore_index=True)
        df.drop(df.index, inplace=True)
        for column in updated.columns:
            df[column] = updated[column]
        # Update checkpoint metadata
        metadata["last_id"] = next_id + insert_count - 1
        metadata["current_count"] = len(df)
        self.current_counts[dimension] = len(df)
        return records

    def generate_sales(self, count, start_id=0, initial_load=False):
        """Generate synthetic sales transactions with optional quality issues."""
        sales = []
        for i in range(count):
            quantity = random.randint(1, 10)
            payment_method = random.choice(
                ["credit_card", "debit_card", "cash_transfer", "digital_wallet"]
            )
            transaction_status = random.choice(["completed", "pending", "cancelled"])
            # Initial loads backfill history; incremental runs generate recent transactions.
            if initial_load:
                transaction_time = self.tm - timedelta(
                    days=random.randint(0, 60),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59),
                )  # Last 2 month
            else:
                transaction_time = self.tm - timedelta(
                    minutes=random.randint(0, 1399), seconds=random.randint(0, 59)
                )  # Last 1 day
            sale = {
                "transaction_id": start_id + i + 1,
                "customer_id": random.randint(1, self.current_counts["customers"]),
                "product_id": random.randint(1, self.current_counts["products"]),
                "store_id": random.randint(1, self.current_counts["stores"]),
                "quantity": quantity,
                "transaction_timestamp": transaction_time,
                "payment_method": payment_method,
                "status": transaction_status,
            }
            # Introduce realistic data quality issues (10% of records)
            # Inject occasional data quality issues so Soda checks can detect them.
            if random.random() < 0.10:
                issue_type = random.choice(
                    [
                        "null_customer",
                        "invalid_product",
                        "negative_amount",
                        "future_date",
                        "invalid_status",
                        "invalid_quantity",
                        "invalid_payment",
                        "null_timestamp",
                    ]
                )
                if issue_type == "null_customer":
                    sale["customer_id"] = None
                elif issue_type == "invalid_product":
                    # Product ID outside valid range
                    sale["product_id"] = random.randint(10000, 19999)
                elif issue_type == "future_date":
                    sale["transaction_timestamp"] = self.tm + timedelta(
                        days=random.randint(1, 30)
                    )
                elif issue_type == "invalid_status":
                    sale["status"] = random.choice(["UNKNOWN", "ERROR", "", None])
                elif issue_type == "invalid_quantity":
                    sale["quantity"] = random.choice([0, -abs(sale["quantity"])])
                elif issue_type == "invalid_payment":
                    if sale["status"] != "completed":
                        sale["payment_method"] = random.choice(
                            ["", None, "INVALID", "bitcoin"]
                        )
                elif issue_type == "null_timestamp":
                    sale["transaction_timestamp"] = None
            sales.append(sale)
        logger.info(f"Generated {len(sales)} sales records")
        return sales

    def random_sales_volume(self):
        """Return a random daily sales volume within the configured range."""
        # Vary the daily transaction volume around the configured average.
        variation = (
            self.daily_sales_records
            * self.config["data_generation"]["sales"]["variation_percentage"]
        )
        return random.randint(
            int(self.daily_sales_records - variation),
            int(self.daily_sales_records + variation),
        )

    def read_current(self, dimension):
        """Read the current state of a dimension from storage."""
        # Current-state files support incremental dimension updates and inserts.
        return self.storage.read_parquet(f"current/{dimension}/{dimension}.parquet")

    def write_current(self, data, dimension):
        """Write the current state of a dimension to storage."""
        df = pd.DataFrame(data)
        self.storage.write_parquet(df, f"current/{dimension}/{dimension}.parquet")

    def write_dimension_file(self, data, dimension):
        """Write dimension records to the Bronze dimension path."""
        filename = f"dimensions/{dimension}/{dimension}.parquet"
        df = pd.DataFrame(data)
        self.storage.write_parquet(df, filename)

    def write_sales(self, data):
        """Write generated sales records to the Bronze sales path."""
        filename = "sales/sales.parquet"
        df = pd.DataFrame(data)
        self.storage.write_parquet(df, filename)


if __name__ == "__main__":
    generator = SalesDataGenerator()
    generator.run()
