CREATE TABLE IF NOT EXISTS edu.order_items (
    id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price NUMERIC(10, 2)
);