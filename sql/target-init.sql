-- Target Replica Table for Analytical Storage & Soft Delete Tracking
CREATE TABLE IF NOT EXISTS target_orders (
    id INT PRIMARY KEY,
    customer_id INT,
    total_amount NUMERIC(10, 2),
    status VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS target_order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price NUMERIC(10, 2),
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);