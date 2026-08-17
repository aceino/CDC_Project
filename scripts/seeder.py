from connection import get_connection 
from faker import Faker 

fake = Faker() 

def seed_inital_data(cur) : 
    conn = get_connection()
    cur = conn.cursor() 
    cur.execute("SELECT COUNT(*) from customers")
    customer_count = cur.fetchone()[0]

    if customer_count > 0: 
        print("Database already populated. Skipping seed.")
        return 

    for i in range(20):
        name = fake.name()
        email = fake.email()
        address = fake.address()
        cur.execute("""
            insert INTO customers (name, email, address)
            values (%s, %s, %s)
        """, (name,email,address)
        )
    print("Seeded 20 customers")

    cur.execute("SELECT COUNT(*) FROM products;")
    product_count = cur.fetchone()[0]
    
    if product_count == 0:
        print("Seeding initial products...")
        sample_products = [
            ("Laptop Pro", "Electronics", 1200.00, 50),
            ("Wireless Mouse", "Electronics", 29.99, 150),
            ("Mechanical Keyboard", "Electronics", 89.99, 80),
            ("USB-C Cable", "Accessories", 12.50, 300),
            ("Coffee Mug", "Home", 15.00, 100)
        ]
        for name, category, price, stock in sample_products:
            cur.execute(
                """
                INSERT INTO products (name, category, price, stock_quantity)
                VALUES (%s, %s, %s, %s);
                """,
                (name, category, price, stock)
            )
        print("5 Products seeded.")

if __name__ == "__main__": 
    conn = get_connection() 
    cur = conn.cursor() 
    seed_inital_data(cur) 
    cur.close() 
    conn.close() 