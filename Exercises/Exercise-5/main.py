import psycopg2


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    
    drop_tables_query="""
        DROP INDEX IF EXISTS idx_transaction_id;
        DROP TABLE IF EXISTS transactions;
        DROP INDEX IF EXISTS idx_customer_id;
        DROP TABLE IF EXISTS accounts;
        DROP INDEX IF EXISTS idx_product_id;
        DROP TABLE IF EXISTS products;
    """
    create_table_products="""
        
        CREATE TABLE IF NOT EXISTS products(
            product_id int, 
            product_code varchar, 
            product_description varchar,
            PRIMARY KEY(product_id)
        );
        CREATE INDEX idx_product_id ON products (product_id);
    """

    create_table_accounts="""
        
        CREATE TABLE IF NOT EXISTS accounts(
            customer_id int, 
            first_name varchar, 
            last_name varchar, 
            address_1 varchar, 
            address_2 varchar, 
            city varchar, 
            state varchar, 
            zip_code int, 
            join_date date,
            PRIMARY KEY(customer_id)
        );
        CREATE INDEX idx_customer_id ON accounts (customer_id);
    """

    create_table_transactions="""
        
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id varchar PRIMARY KEY, 
            transaction_date date, 
            product_id int, 
            product_code int, 
            product_description varchar, 
            quantity int, 
            account_id int,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
        );
        CREATE INDEX idx_transaction_id ON transactions (transaction_id);
    """
    
    cur = conn.cursor()

    try:
        cur.execute(drop_tables_query)
        cur.execute(create_table_products)
        cur.execute(create_table_accounts)
        cur.execute(create_table_transactions)
        conn.commit()  # Commit the changes to the database
        print("Tables and index created successfully.")
    except psycopg2.Error as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error creating tables: {e}")

    tablelist = ['products','accounts','transactions']

    try:
        for table in tablelist:
            with open(f'./data/{table}.csv','r') as f:
                next(f)
                cur.copy_from(f,table,sep=',')
            conn.commit()
            cur.execute(f"SELECT COUNT(*) as count FROM {table}")
            row = cur.fetchall()
            print(f'{table}:{row[0][0]} rows inserted')
        print('Data inserted successfully')
    except psycopg2.Error as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error creating tables: {e}")

    cur.close()
    conn.close()
        

    


if __name__ == "__main__":
    main()
