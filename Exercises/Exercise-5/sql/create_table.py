AccountTable = \
               """ 
               CREATE TABLE IF NOT EXISTS accounts(customerId SMALLINT, firstName VARCHAR(30), lastName VARCHAR(30), address1 TEXT, 
               address2 TEXT, city VARCHAR(20), state VARCHAR(20), zipCode INTEGER, joinDate DATE,
               PRIMARY KEY (customerId))
               """

ProductTable = \
               """ 
               CREATE TABLE IF NOT EXISTS products(productId SMALLINT, productCode SMALLINT, productDescription TEXT,
               PRIMARY KEY (productId))
               """

TransactionTable = \
               """ 
               CREATE TABLE IF NOT EXISTS transactions(transactionId VARCHAR(30), transactionDate DATE, productId SMALLINT,
               productCode SMALLINT, product_description TEXT, quantity SMALLINT, account_id SMALLINT,
               PRIMARY KEY (transactionId),
               FOREIGN KEY (productId) REFERENCES products(productId),
               FOREIGN KEY (account_id) REFERENCES accounts(customerId))
               """
