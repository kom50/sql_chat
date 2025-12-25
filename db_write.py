from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random
from datetime import datetime, timedelta
import time

# ============ Database Setup ============
Base = declarative_base()


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    category = Column(String)
    price = Column(Float)
    stock = Column(Integer)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    city = Column(String)


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total = Column(Float)


# ============ Configuration ============
NUM_PRODUCTS = 10_000  # 10K products
NUM_CUSTOMERS = 100_000  # 100K customers
NUM_ORDERS = 1_000_000  # 1M orders

BATCH_SIZE = 10_000  # Insert in batches of 10K

# ============ Initialize ============
fake = Faker()
engine = create_engine("sqlite:///db/store.db", echo=False)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def clear_database():
    """Clear all existing data"""
    print("🗑️  Clearing existing data...")
    session.execute(text("DELETE FROM orders"))
    session.execute(text("DELETE FROM products"))
    session.execute(text("DELETE FROM customers"))
    session.commit()
    print("✅ Database cleared!\n")


def generate_products(num_products):
    """Generate realistic product data"""
    print(f"📦 Generating {num_products:,} products...")

    categories = [
        "Electronics",
        "Furniture",
        "Clothing",
        "Books",
        "Sports",
        "Home & Garden",
        "Toys",
        "Beauty",
        "Automotive",
        "Food & Beverages",
    ]

    product_prefixes = {
        "Electronics": [
            "Laptop",
            "Phone",
            "Tablet",
            "Headphones",
            "Camera",
            "Speaker",
            "Monitor",
            "Keyboard",
        ],
        "Furniture": [
            "Chair",
            "Desk",
            "Table",
            "Sofa",
            "Bed",
            "Cabinet",
            "Shelf",
            "Lamp",
        ],
        "Clothing": [
            "T-Shirt",
            "Jeans",
            "Jacket",
            "Dress",
            "Shoes",
            "Hat",
            "Sweater",
            "Coat",
        ],
        "Books": [
            "Novel",
            "Textbook",
            "Magazine",
            "Comic",
            "Biography",
            "Cookbook",
            "Guide",
        ],
        "Sports": [
            "Ball",
            "Bat",
            "Racket",
            "Bike",
            "Treadmill",
            "Weights",
            "Mat",
            "Helmet",
        ],
        "Home & Garden": ["Plant", "Tool", "Pot", "Hose", "Mower", "Grill", "Decor"],
        "Toys": [
            "Action Figure",
            "Puzzle",
            "Board Game",
            "Doll",
            "Car",
            "Building Set",
        ],
        "Beauty": ["Lipstick", "Perfume", "Cream", "Shampoo", "Makeup Kit", "Lotion"],
        "Automotive": ["Tire", "Battery", "Oil", "Filter", "Wiper", "Cover", "Polish"],
        "Food & Beverages": [
            "Coffee",
            "Tea",
            "Snacks",
            "Juice",
            "Cookies",
            "Chocolate",
        ],
    }

    products = []
    start_time = time.time()

    for i in range(num_products):
        category = random.choice(categories)
        prefix = random.choice(product_prefixes[category])
        brand = fake.company().split()[0]

        product = {
            "name": f"{brand} {prefix} {fake.color_name()}",
            "category": category,
            "price": round(random.uniform(9.99, 2999.99), 2),
            "stock": random.randint(0, 1000),
        }
        products.append(product)

        # Progress indicator
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            eta = (num_products - i - 1) / rate
            print(
                f"  Progress: {i+1:,}/{num_products:,} ({(i+1)/num_products*100:.1f}%) - ETA: {eta:.1f}s",
                end="\r",
            )

    print(
        f"\n  Generated {len(products):,} products in {time.time() - start_time:.2f}s"
    )
    return products


def generate_customers(num_customers):
    """Generate realistic customer data"""
    print(f"\n👥 Generating {num_customers:,} customers...")

    customers = []
    start_time = time.time()

    for i in range(num_customers):
        customer = {"name": fake.name(), "email": fake.email(), "city": fake.city()}
        customers.append(customer)

        # Progress indicator
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            eta = (num_customers - i - 1) / rate
            print(
                f"  Progress: {i+1:,}/{num_customers:,} ({(i+1)/num_customers*100:.1f}%) - ETA: {eta:.1f}s",
                end="\r",
            )

    print(
        f"\n  Generated {len(customers):,} customers in {time.time() - start_time:.2f}s"
    )
    return customers


def generate_orders(num_orders, num_customers, num_products):
    """Generate realistic order data"""
    print(f"\n📋 Generating {num_orders:,} orders...")

    orders = []
    start_time = time.time()

    for i in range(num_orders):
        customer_id = random.randint(1, num_customers)
        product_id = random.randint(1, num_products)
        quantity = random.randint(1, 10)

        # Price will be calculated based on product, but we'll use random for dummy data
        unit_price = round(random.uniform(9.99, 2999.99), 2)
        total = round(unit_price * quantity, 2)

        order = {
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "total": total,
        }
        orders.append(order)

        # Progress indicator
        if (i + 1) % 10000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            eta = (num_orders - i - 1) / rate
            print(
                f"  Progress: {i+1:,}/{num_orders:,} ({(i+1)/num_orders*100:.1f}%) - ETA: {eta:.1f}s",
                end="\r",
            )

    print(f"\n  Generated {len(orders):,} orders in {time.time() - start_time:.2f}s")
    return orders


def bulk_insert(table_class, data, batch_size=10000):
    """Insert data in batches for better performance"""
    total = len(data)
    print(f"\n💾 Inserting {total:,} records into {table_class.__tablename__}...")

    start_time = time.time()
    inserted = 0

    for i in range(0, total, batch_size):
        batch = data[i : i + batch_size]
        session.bulk_insert_mappings(table_class, batch)
        session.commit()

        inserted += len(batch)
        elapsed = time.time() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        eta = (total - inserted) / rate if rate > 0 else 0

        print(
            f"  Inserted: {inserted:,}/{total:,} ({inserted/total*100:.1f}%) - "
            f"Rate: {rate:.0f} rec/s - ETA: {eta:.1f}s",
            end="\r",
        )

    elapsed = time.time() - start_time
    print(
        f"\n  ✅ Inserted {total:,} records in {elapsed:.2f}s ({total/elapsed:.0f} rec/s)"
    )


def main():
    total_start = time.time()

    print("=" * 60)
    print("🚀 MASS DATA GENERATOR")
    print("=" * 60)
    print(f"Products:  {NUM_PRODUCTS:,}")
    print(f"Customers: {NUM_CUSTOMERS:,}")
    print(f"Orders:    {NUM_ORDERS:,}")
    print(f"Total:     {NUM_PRODUCTS + NUM_CUSTOMERS + NUM_ORDERS:,} records")
    print("=" * 60)

    # Step 1: Clear existing data
    clear_database()

    # Step 2: Generate data
    products_data = generate_products(NUM_PRODUCTS)
    customers_data = generate_customers(NUM_CUSTOMERS)
    orders_data = generate_orders(NUM_ORDERS, NUM_CUSTOMERS, NUM_PRODUCTS)

    # Step 3: Insert data
    bulk_insert(Product, products_data, BATCH_SIZE)
    bulk_insert(Customer, customers_data, BATCH_SIZE)
    bulk_insert(Order, orders_data, BATCH_SIZE)

    # Final summary
    total_time = time.time() - total_start
    total_records = NUM_PRODUCTS + NUM_CUSTOMERS + NUM_ORDERS

    print("\n" + "=" * 60)
    print("✅ DATABASE POPULATION COMPLETE!")
    print("=" * 60)
    print(f"Total Records: {total_records:,}")
    print(f"Total Time:    {total_time:.2f}s ({total_time/60:.2f} minutes)")
    print(f"Average Rate:  {total_records/total_time:.0f} records/second")
    print("=" * 60)

    # Verify counts
    print("\n📊 Verification:")
    product_count = session.query(Product).count()
    customer_count = session.query(Customer).count()
    order_count = session.query(Order).count()

    print(f"  Products:  {product_count:,}")
    print(f"  Customers: {customer_count:,}")
    print(f"  Orders:    {order_count:,}")
    print(f"  Total:     {product_count + customer_count + order_count:,}")

    session.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
