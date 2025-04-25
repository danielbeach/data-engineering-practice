-- Xóa bảng orders trước
DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address_1 TEXT,
    address_2 TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    join_date DATE
);

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_code VARCHAR(10),
    product_description TEXT
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    transaction_date DATE,
    quantity INTEGER,
    price NUMERIC(10, 2),
    FOREIGN KEY (customer_id) REFERENCES accounts(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX idx_accounts_state ON accounts(state);
CREATE INDEX idx_transactions_product_id ON transactions(product_id);
