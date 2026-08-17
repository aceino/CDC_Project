import time 
import random 
import psycopg2 
from faker import Faker

from connection import get_connection 

def create_random_order() : 
    conn = get_connection() 
    cur = conn.cursor() 

    cur.execute("select id from customers order by random() limit 1")
    customer_id = cur.fetchone()[0]

    cur.execute("select id, price from products order by random() limit 1")
    product_id, price = cur.fetchone() 

    quantity = random.randint(1, 3)
    total_amount = float(price) * quantity 

    cur.execute (
        """
        insert into orders (customer_id, total_amount, status)
        values(%s, %s, 'PENDING')
        returning id
        """, 
        (customer_id, total_amount)
    )

    order_id = cur.fetchone()[0]

    cur.execute(
        """
        insert into order_items(order_id, product_id, quantity, price, total_amount) 
        values(%s, %s, %s, %s, %s)
        """,
        (order_id, product_id, quantity, price, total_amount)
    )

    print(f"[CREATE] Order #{order_id} created for Customer #{customer_id} (Total: ${total_amount:.2f})")

def update_order_status(): 
    conn = get_connection() 
    cur = conn.cursor() 

    cur.execute("select id, status, total_amount from orders limit 1")

    res = cur.fetchone()

    if res : 
        order_id, status, total_amount = res 
        new_status = random.choice(['SHIPPED', 'DELIVERED', 'CANCELLED', 'PAID'])
        cur.execute("update orders set status =%s, updated_at = now() where id=%s", (new_status, order_id))
        print(f"[UPDATE] order #{order_id} -> {new_status}: (Total: ${total_amount})")

def delete_random_order(): 
    conn = get_connection() 
    cur = conn.cursor() 

    cur.execute("select id from orders order by random() limit 1")
    res = cur.fetchone()

    if res : 
        order_id = res[0]
        cur.execute("delete from orders where id=%s", (order_id,))
        print(f"[DELETE] physical delete on order #{order_id} (CDC SOFT DELETE TEST)")

    cur.close() 
    conn.close() 

def main() : 
    print("🚀 Starting E-Commerce Live Transaction Feeder...")
    while True:
        # Weighted random choice: 60% Create, 30% Update, 10% Delete
        action = random.choices(["create", "update", "delete"], weights=[60, 30, 10])[0]
        
        if action == "create":
            create_random_order()
        elif action == "update":
            update_order_status()
        elif action == "delete":
            delete_random_order()
            
        time.sleep(random.randint(2, 4))

if __name__ == "__main__" : 
    main() 
     