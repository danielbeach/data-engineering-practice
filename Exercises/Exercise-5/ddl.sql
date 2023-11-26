ALTER ROLE postgres WITH PASSWORD 'postgres';
CREATE TABLE IF NOT EXISTS accounts (customer_id INTEGER,  first_name TEXT,  last_name TEXT,  address_1 TEXT,  address_2 TEXT,  city TEXT,  state TEXT,  zip_code INTEGER,  join_date TEXT);

-- INSERT DATA --
INSERT INTO accounts VALUES ('4321', ' john', ' doe', ' 1532 East Main St.', ' PO BOX 5', ' Middleton', ' Ohio', '50045', ' 2022/01/16');
INSERT INTO accounts VALUES ('5677', ' jane', ' doe', ' 543 Oak Rd.', 'nan', 'BigTown', ' Iowa', '84432', ' 2021/05/07');
CREATE TABLE IF NOT EXISTS products (product_id INTEGER,  product_code INTEGER,  product_description TEXT);

-- INSERT DATA --
INSERT INTO products VALUES ('345', '1', ' Widget Medium');
INSERT INTO products VALUES ('241', '2', ' Widget Large');
CREATE TABLE IF NOT EXISTS transactions (transaction_id TEXT,  transaction_date TEXT,  product_id INTEGER,  product_code INTEGER,  product_description TEXT,  quantity INTEGER,  account_id INTEGER);

-- INSERT DATA --
INSERT INTO transactions VALUES ('AS345-ASDF-31234-FDAAD-9345', ' 2022/06/01', '345', '1', ' Widget Medium', '5', '4321');
INSERT INTO transactions VALUES ('9234A-JFDA-87654-BFAEA-0932', ' 2022/06/02', '241', '2', ' Widget Large', '1', '5677');
