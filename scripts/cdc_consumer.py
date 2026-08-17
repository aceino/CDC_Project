import json 
import os 
import time
from confluent_kafka import Consumer, KafkaError, Producer
from connection import get_connection

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'group.id': 'cdc_orders_group',
    'auto.offset.reset': 'earliest'
}

PRODUCER_CONFIG = { 
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", 'localhost:9092')
}

dlq_producer = Producer(PRODUCER_CONFIG)
DLQ_TOPIC = 'cdc_orders_dlq'

def send_to_dlq(raw_message_str, error_msg):
    dlq_payload = { 
        "failed_message": raw_message_str, 
        "error_reason": str(error_msg),
        "failed_at": time.time()
    }

    dlq_producer.produce(
        DLQ_TOPIC,
        value=json.dumps(dlq_payload).encode('utf-8')
    )

    dlq_producer.flush() 
    print(f"🚨 [DLQ ALERT] Event routed to Dead Letter Queue '{DLQ_TOPIC}' | Reason: {error_msg}")

def upsert_target_orders(cur, data):
    cur.execute(
        """
        insert into target_orders (id, customer_id, total_amount, status, created_at, updated_at, is_deleted, synced_at)
        VALUES (%s, %s, %s, %s, TO_TIMESTAMP(%s / 1000000.0), TO_TIMESTAMP(%s / 1000000.0), FALSE, NOW())
        on conflict(id) do update set 
            customer_id = EXCLUDED.customer_id,
            total_amount = EXCLUDED.total_amount,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at,
            is_deleted = FALSE,
            synced_at = NOW();        
        """, 
        (
            data["id"],
            data["customer_id"],
            data["total_amount"],
            data["status"],
            data["created_at"],
            data["updated_at"]
        )
    )
    print(f"✅ [SYNC UPSERT] Target order #{data['id']} synced! (Status: {data['status']})")

def upsert_target_order_items(cur, data):
    cur.execute(
        """
        insert into target_order_items (id, order_id, product_id, quantity, price, is_deleted, synced_at)
        values(%s, %s, %s, %s, %s, FALSE, NOW())
        on conflict(id) do update set 
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            is_deleted = FALSE,
            synced_at = NOW();
        """,
        (data["id"], data["order_id"], data["product_id"], data["quantity"], data["price"])
    )
    print(f"📦 [SYNC ITEM UPSERT] Target order item #{data['id']} synced!")

def soft_delete_target_orders(cur, data):
    """Handles DELETE ('d') events by marking is_deleted = true in target_orders."""
    order_id = data["id"]
    cur.execute(
        """
        UPDATE target_orders 
        SET is_deleted = TRUE, deleted_at = NOW(), synced_at = NOW()
        WHERE id = %s;
        """,
        (order_id,)
    )
    print(f"⚠️ [SYNC SOFT-DELETE] Target order #{order_id} marked as IS_DELETED = TRUE")

def soft_delete_target_order_items(cur, data):
    """Marks item as soft-deleted in target_order_items."""
    item_id = data["id"]
    cur.execute(
        """
        UPDATE target_order_items 
        SET is_deleted = TRUE, deleted_at = NOW(), synced_at = NOW()
        WHERE id = %s;
        """,
        (item_id,)
    )
    print(f"⚠️ [SYNC ITEM SOFT-DELETE] Order item #{item_id} marked IS_DELETED = TRUE")

# Handler Dispatch Map (defined after functions)
HANDLERS = { 
    "cdc_demo.public.orders": { 
        "upsert": upsert_target_orders,
        "delete": soft_delete_target_orders
    },
    "cdc_demo.public.order_items": {
        "upsert": upsert_target_order_items,
        "delete": soft_delete_target_order_items
    }
}

def main(): 
    conn = get_connection() 
    cur = conn.cursor()

    consumer = Consumer(KAFKA_CONFIG)
    topic_orders = "cdc_demo.public.orders"
    topic_items = "cdc_demo.public.order_items"

    consumer.subscribe([topic_orders, topic_items])
    print(f"🎧 CDC Consumer listening on topics: '{topic_orders}' and '{topic_items}'...")

    try: 
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                continue 
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue 
                
            raw_data = msg.value().decode('utf-8')

            try:
                event = json.loads(raw_data)
                payload = event.get('payload')

                if not payload: 
                    send_to_dlq(raw_data, "Missing payload field in CDC event")
                    continue

                topic = msg.topic() 
                op = payload.get("op")
                handlers = HANDLERS.get(topic)

                if handlers:
                    if op in ('c', 'r', 'u'):
                        data = payload.get('after')
                        if data:
                            handlers["upsert"](cur, data)
                    elif op == 'd':
                        data = payload.get('before')
                        if data:
                            handlers["delete"](cur, data)

            except Exception as err:
                send_to_dlq(raw_data, err)

    except KeyboardInterrupt: 
        print("\nShutting down CDC Consumer...")
    finally: 
        consumer.close() 
        cur.close() 
        conn.close() 

if __name__ == "__main__":
    main()